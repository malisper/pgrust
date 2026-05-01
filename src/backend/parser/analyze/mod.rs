mod agg;
mod agg_output;
mod agg_output_special;
mod agg_scope;
mod coerce;
mod collation;
mod constraints;
mod create_table;
mod create_table_inherits;
mod expr;
mod functions;
mod generated;
mod geometry;
mod index_predicates;
mod infer;
mod modify;
mod multiranges;
mod on_conflict;
mod partition;
mod paths;
mod query;
mod ranges;
mod rules;
mod scope;
mod sqlfunc_inline;
mod system_views;
mod window;

pub(crate) use self::scope::{ScopeColumn, ScopeRelation};

use crate::RelFileLocator;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::{Value, cast_value};
use crate::backend::optimizer::planner_with_config;
use crate::backend::rewrite::{
    format_stored_rule_definition_with_catalog, format_view_definition, pg_rewrite_query,
};
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::visible_catalog::VisibleCatalog;
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, FLOAT8_ARRAY_TYPE_OID, FLOAT8_TYPE_OID, INTERVAL_ARRAY_TYPE_OID,
    INTERVAL_TYPE_OID, MODE_AGG_PROC_OID, PERCENTILE_CONT_FLOAT8_AGG_PROC_OID,
    PERCENTILE_CONT_FLOAT8_MULTI_AGG_PROC_OID, PERCENTILE_CONT_INTERVAL_AGG_PROC_OID,
    PERCENTILE_CONT_INTERVAL_MULTI_AGG_PROC_OID, PERCENTILE_DISC_AGG_PROC_OID,
    PERCENTILE_DISC_MULTI_AGG_PROC_OID, PgAggregateRow, PgAmRow, PgAmopRow, PgAmprocRow,
    PgAttributeRow, PgAuthIdRow, PgAuthMembersRow, PgCastRow, PgClassRow, PgCollationRow,
    PgConstraintRow, PgConversionRow, PgDatabaseRow, PgDependRow, PgEnumRow, PgEventTriggerRow,
    PgForeignDataWrapperRow, PgForeignServerRow, PgForeignTableRow, PgIndexRow, PgInheritsRow,
    PgLanguageRow, PgNamespaceRow, PgOpclassRow, PgOperatorRow, PgOpfamilyRow,
    PgPartitionedTableRow, PgProcRow, PgPublicationNamespaceRow, PgPublicationRelRow,
    PgPublicationRow, PgRangeRow, PgRewriteRow, PgSequenceRow, PgStatisticExtDataRow,
    PgStatisticExtRow, PgStatisticRow, PgTablespaceRow, PgTsConfigMapRow, PgTsConfigRow,
    PgTsDictRow, PgTsParserRow, PgTsTemplateRow, PgTypeRow, PgUserMappingRow, RECORD_TYPE_OID,
    bootstrap_pg_aggregate_rows, bootstrap_pg_am_rows, bootstrap_pg_amop_rows,
    bootstrap_pg_amproc_rows, bootstrap_pg_cast_rows, bootstrap_pg_collation_rows,
    bootstrap_pg_conversion_rows, bootstrap_pg_database_rows, bootstrap_pg_enum_rows,
    bootstrap_pg_language_rows, bootstrap_pg_namespace_rows, bootstrap_pg_opclass_rows,
    bootstrap_pg_operator_rows, bootstrap_pg_opfamily_rows, bootstrap_pg_proc_row_by_oid,
    bootstrap_pg_proc_rows, bootstrap_pg_proc_rows_by_name, bootstrap_pg_tablespace_rows,
    bootstrap_pg_ts_config_map_rows, bootstrap_pg_ts_config_rows, bootstrap_pg_ts_dict_rows,
    bootstrap_pg_ts_parser_rows, bootstrap_pg_ts_template_rows, builtin_range_rows,
    builtin_type_row_by_name, builtin_type_row_by_oid, builtin_type_rows,
    is_synthetic_range_proc_name, multirange_type_ref_for_sql_type,
    proc_oid_for_builtin_aggregate_function, proc_oid_for_builtin_hypothetical_aggregate_function,
    range_type_ref_for_sql_type, relkind_is_analyzable, synthetic_range_proc_row_by_oid,
    synthetic_range_proc_rows_by_name,
};
use crate::include::nodes::pathnodes::{PlannerConfig, PlannerIndexExprCacheEntry};
use crate::include::nodes::plannodes::{Plan, PlannedStmt};
use crate::include::nodes::primnodes::{
    AggAccum, AggFunc, BuiltinScalarFunction, Expr, HypotheticalAggFunc, JsonTableFunction,
    OrderByEntry, OrderedSetAggFunc, QueryColumn, RelationDesc, SetReturningCall, SortGroupClause,
    TargetEntry, ToastRelationRef, Var, expr_contains_set_returning, expr_sql_type_hint,
    user_attrno,
};
use std::sync::atomic::{AtomicUsize, Ordering};

use super::parsenodes::*;
pub use crate::backend::catalog::catalog::{Catalog, CatalogEntry};

static NEXT_WORKTABLE_ID: AtomicUsize = AtomicUsize::new(1);
static NEXT_CTE_ID: AtomicUsize = AtomicUsize::new(1);
use crate::backend::utils::cache::relcache::RelCache;
use crate::backend::utils::cache::system_views::{
    build_pg_indexes_rows, build_pg_locks_rows, build_pg_matviews_rows, build_pg_policies_rows,
    build_pg_rules_rows_with_definition_formatter, build_pg_stats_ext_exprs_rows,
    build_pg_stats_ext_rows, build_pg_stats_rows, build_pg_tables_rows,
    build_pg_user_mappings_rows, build_pg_views_rows_with_definition_formatter,
    current_pg_stat_progress_copy_rows,
};
use agg::*;
use agg_output::*;
use agg_scope::*;
pub use coerce::is_binary_coercible_type;
pub(crate) use coerce::sql_type_name;
use coerce::*;
pub(crate) use collation::{
    CollationConsumer, bind_explicit_collation, consumer_for_subquery_comparison_op,
    derive_consumer_collation, finalize_order_by_expr, is_collatable_type, resolve_collation_oid,
    strip_explicit_collation,
};
pub(crate) use constraints::*;
pub(crate) use constraints::{
    BoundExclusionConstraint, BoundReferencedByForeignKey, BoundRelationConstraints,
    BoundTemporalConstraint,
};
pub use create_table::*;
pub use create_table_inherits::*;
pub(crate) use expr::bind_expr_with_outer_and_ctes;
use expr::*;
use functions::*;
pub(crate) use functions::{ResolvedFunctionCall, resolve_function_call};
pub(crate) use generated::{
    bind_generated_expr, expr_references_column, generated_relation_output_exprs,
    scope_for_base_relation_with_generated, scope_for_relation_with_generated,
    validate_generated_columns,
};
use geometry::*;
pub(crate) use index_predicates::*;
use infer::*;
pub use modify::{
    BoundArraySubscript, BoundAssignment, BoundAssignmentTarget, BoundAssignmentTargetIndirection,
    BoundDeleteStatement, BoundDeleteTarget, BoundInsertSource, BoundInsertStatement,
    BoundMergeAction, BoundMergeStatement, BoundMergeWhenClause, BoundUpdateStatement,
    BoundUpdateTarget, PreparedInsert, bind_delete, bind_insert, bind_insert_prepared, bind_update,
    plan_merge,
};
pub(crate) use modify::{
    bind_delete_with_outer_scopes, bind_delete_with_outer_scopes_and_ctes,
    bind_insert_with_outer_scopes, bind_insert_with_outer_scopes_and_ctes,
    bind_update_with_outer_scopes, bind_update_with_outer_scopes_and_ctes,
    plan_merge_with_outer_ctes, plan_merge_with_outer_scopes_and_ctes,
    rewrite_bound_delete_auto_view_target, rewrite_bound_insert_auto_view_target,
    rewrite_bound_update_auto_view_target,
};
pub use on_conflict::{BoundOnConflictAction, BoundOnConflictClause};
pub(crate) use partition::*;
pub use paths::BoundModifyRowSource;
use paths::bind_order_by_items;
pub(crate) use query::analyze_select_query_with_outer;
use query::{
    AnalyzedFrom, analyze_values_query_with_outer, identity_target_list, normalize_target_list,
    query_from_from_projection,
};
pub(crate) use query::{
    rewrite_local_vars_for_output_exprs, rewrite_planned_local_vars_for_output_exprs,
};
pub(crate) use rules::{
    BoundRuleAction, bind_rule_action_statement, bind_rule_qual, validate_rule_definition,
};
pub use scope::BoundRelation;
use scope::*;
pub(crate) use scope::{
    BoundCte, BoundModifyingCte, BoundScope, BoundWritableCte, scope_for_relation,
    shift_scope_rtindexes,
};
use sqlfunc_inline::*;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use system_views::*;
use window::*;

thread_local! {
    static EXTERNAL_PARAM_TYPES: RefCell<Vec<(usize, SqlType)>> = const { RefCell::new(Vec::new()) };
}

pub(crate) fn with_external_param_types<T>(types: &[(usize, SqlType)], f: impl FnOnce() -> T) -> T {
    EXTERNAL_PARAM_TYPES.with(|cell| {
        let old = cell.replace(types.to_vec());
        let result = f();
        cell.replace(old);
        result
    })
}

pub(crate) fn external_param_type(paramid: usize) -> Option<SqlType> {
    EXTERNAL_PARAM_TYPES.with(|cell| {
        cell.borrow()
            .iter()
            .rev()
            .find(|(candidate, _)| *candidate == paramid)
            .map(|(_, ty)| *ty)
    })
}

pub(crate) fn is_system_column_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "tableoid" | "ctid" | "xmin" | "xmax" | "cmin" | "cmax"
    )
}

pub(crate) fn sql_expr_mentions_system_column(sql: &str) -> bool {
    let mut chars = sql.char_indices().peekable();
    while let Some((_, ch)) = chars.next() {
        match ch {
            '\'' => {
                while let Some((_, inner)) = chars.next() {
                    if inner == '\'' {
                        if chars.peek().is_some_and(|(_, next)| *next == '\'') {
                            let _ = chars.next();
                            continue;
                        }
                        break;
                    }
                }
            }
            '"' => {
                let mut ident = String::new();
                while let Some((_, inner)) = chars.next() {
                    if inner == '"' {
                        if chars.peek().is_some_and(|(_, next)| *next == '"') {
                            ident.push('"');
                            let _ = chars.next();
                            continue;
                        }
                        break;
                    }
                    ident.push(inner);
                }
                if is_system_column_name(&ident) {
                    return true;
                }
            }
            ch if ch == '_' || ch.is_ascii_alphabetic() => {
                let mut ident = String::from(ch);
                while let Some((_, next)) = chars.peek().copied() {
                    if next == '_' || next == '$' || next.is_ascii_alphanumeric() {
                        ident.push(next);
                        let _ = chars.next();
                    } else {
                        break;
                    }
                }
                if is_system_column_name(&ident) {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundIndexRelation {
    pub name: String,
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub relkind: char,
    pub desc: RelationDesc,
    pub index_meta: crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    pub index_exprs: Vec<Expr>,
    pub index_predicate: Option<Expr>,
    pub constraint_oid: Option<u32>,
    pub constraint_name: Option<String>,
    pub constraint_deferrable: bool,
    pub constraint_initially_deferred: bool,
}

fn dedup_proc_rows(rows: &mut Vec<PgProcRow>) {
    let mut seen = BTreeSet::new();
    rows.retain(|row| {
        seen.insert((
            row.proname.clone(),
            row.prorettype,
            row.proargtypes.clone(),
            row.prokind,
            row.proretset,
        ))
    });
}

fn extend_synthetic_range_proc_rows<C: CatalogLookup + ?Sized>(
    catalog: &C,
    name: &str,
    rows: &mut Vec<PgProcRow>,
) {
    let needs_synthetic_lookup = if rows.is_empty() {
        is_synthetic_range_proc_name(name) || resolve_scalar_function(name).is_none()
    } else {
        is_synthetic_range_proc_name(name)
    };
    if needs_synthetic_lookup {
        rows.extend(synthetic_range_proc_rows_by_name(
            name,
            &catalog.type_rows(),
            &catalog.range_rows(),
        ));
    }
}

pub(crate) fn bind_index_exprs(
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    heap_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Expr>, ParseError> {
    if let Some(exprs) = &index_meta.rd_indexprs {
        return Ok(exprs.clone());
    }
    bind_index_exprs_uncached(index_meta, heap_desc, catalog)
}

pub(crate) fn relation_get_index_expressions(
    index_meta: &mut crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    heap_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Expr>, ParseError> {
    if let Some(exprs) = &index_meta.rd_indexprs {
        return Ok(exprs.clone());
    }
    let exprs = bind_index_exprs_uncached(index_meta, heap_desc, catalog)?;
    index_meta.rd_indexprs = Some(exprs.clone());
    Ok(exprs)
}

#[allow(non_snake_case)]
pub(crate) fn RelationGetIndexExpressions(
    index_meta: &mut crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    heap_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Expr>, ParseError> {
    relation_get_index_expressions(index_meta, heap_desc, catalog)
}

fn bind_index_exprs_uncached(
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    heap_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<Expr>, ParseError> {
    let Some(indexprs) = index_meta.indexprs.as_deref() else {
        return Ok(Vec::new());
    };
    let expr_sqls =
        serde_json::from_str::<Vec<String>>(indexprs).map_err(|_| ParseError::UnexpectedToken {
            expected: "serialized index expressions",
            actual: "invalid index expression metadata".into(),
        })?;
    let index_catalog = IndexExpressionCatalogLookup { inner: catalog };
    expr_sqls
        .into_iter()
        .map(|expr_sql| bind_relation_expr(&expr_sql, None, heap_desc, &index_catalog))
        .collect()
}

pub(crate) fn bind_index_predicate(
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    heap_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    if let Some(predicate) = &index_meta.rd_indpred {
        return Ok(predicate.clone());
    }
    bind_index_predicate_uncached(index_meta, heap_desc, catalog)
}

pub(crate) fn relation_get_index_predicate(
    index_meta: &mut crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    heap_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    if let Some(predicate) = &index_meta.rd_indpred {
        return Ok(predicate.clone());
    }
    let predicate = bind_index_predicate_uncached(index_meta, heap_desc, catalog)?;
    index_meta.rd_indpred = Some(predicate.clone());
    Ok(predicate)
}

#[allow(non_snake_case)]
pub(crate) fn RelationGetIndexPredicate(
    index_meta: &mut crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    heap_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    relation_get_index_predicate(index_meta, heap_desc, catalog)
}

fn bind_index_predicate_uncached(
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    heap_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Option<Expr>, ParseError> {
    let Some(predicate_sql) = index_meta.indpred.as_deref().map(str::trim) else {
        return Ok(None);
    };
    if predicate_sql.is_empty() {
        return Ok(None);
    }
    let relation_name = catalog
        .class_row_by_oid(index_meta.indrelid)
        .map(|row| row.relname);
    let relation_name = relation_name
        .as_deref()
        .map(|name| name.rsplit('.').next().unwrap_or(name));
    let index_catalog = IndexExpressionCatalogLookup { inner: catalog };
    bind_index_predicate_sql_expr(predicate_sql, relation_name, heap_desc, &index_catalog).map(Some)
}

fn build_sort_clause(
    sort_inputs: Vec<OrderByEntry>,
    target_list: &[TargetEntry],
) -> Vec<SortGroupClause> {
    let mut next_sort_group_ref = target_list
        .iter()
        .map(|target| target.ressortgroupref.max(target.resno))
        .max()
        .unwrap_or(0)
        + 1;
    sort_inputs
        .into_iter()
        .map(|item| {
            let tle_sort_group_ref = if item.ressortgroupref != 0 {
                item.ressortgroupref
            } else {
                let next = next_sort_group_ref;
                next_sort_group_ref += 1;
                next
            };
            SortGroupClause {
                expr: item.expr,
                tle_sort_group_ref,
                descending: item.descending,
                nulls_first: item.nulls_first,
                collation_oid: item.collation_oid,
            }
        })
        .collect()
}

fn target_or_sort_clause_contains_srf(
    target_list: &[TargetEntry],
    sort_clause: &[SortGroupClause],
) -> bool {
    target_list
        .iter()
        .any(|target| expr_contains_set_returning(&target.expr))
        || sort_clause
            .iter()
            .any(|clause| expr_contains_set_returning(&clause.expr))
}

fn order_entry_matches_sort_clause(entry: &OrderByEntry, clause: &SortGroupClause) -> bool {
    (entry.ressortgroupref != 0 && entry.ressortgroupref == clause.tle_sort_group_ref)
        || entry.expr == clause.expr
}

fn sort_group_clause_for_order_entry(
    entry: OrderByEntry,
    next_sort_group_ref: &mut usize,
) -> SortGroupClause {
    let tle_sort_group_ref = if entry.ressortgroupref != 0 {
        entry.ressortgroupref
    } else {
        let next = *next_sort_group_ref;
        *next_sort_group_ref += 1;
        next
    };
    SortGroupClause {
        expr: entry.expr,
        tle_sort_group_ref,
        descending: entry.descending,
        nulls_first: entry.nulls_first,
        collation_oid: entry.collation_oid,
    }
}

fn build_distinct_on_clause(
    distinct_on: &[SqlExpr],
    sort_clause: &[SortGroupClause],
    target_list: &[TargetEntry],
    catalog: &dyn CatalogLookup,
    bind_expr: impl Fn(&SqlExpr) -> Result<Expr, ParseError>,
) -> Result<Vec<SortGroupClause>, ParseError> {
    if distinct_on.is_empty() {
        return Ok(Vec::new());
    }
    let distinct_items = distinct_on
        .iter()
        .cloned()
        .map(|expr| OrderByItem {
            expr,
            location: None,
            descending: false,
            nulls_first: None,
            using_operator: None,
        })
        .collect::<Vec<_>>();
    let distinct_inputs = bind_order_by_items(&distinct_items, target_list, catalog, bind_expr)?;
    let mut result = Vec::new();
    let mut skipped_sortitem = false;

    for sort_item in sort_clause {
        if distinct_inputs
            .iter()
            .any(|entry| order_entry_matches_sort_clause(entry, sort_item))
        {
            if skipped_sortitem {
                return Err(ParseError::FeatureNotSupportedMessage(
                    "SELECT DISTINCT ON expressions must match initial ORDER BY expressions".into(),
                ));
            }
            result.push(sort_item.clone());
        } else {
            skipped_sortitem = true;
        }
    }

    let mut next_sort_group_ref = target_list
        .iter()
        .map(|target| target.ressortgroupref.max(target.resno))
        .chain(sort_clause.iter().map(|clause| clause.tle_sort_group_ref))
        .max()
        .unwrap_or(0)
        + 1;

    for entry in distinct_inputs {
        if result
            .iter()
            .any(|clause| order_entry_matches_sort_clause(&entry, clause))
        {
            continue;
        }
        if skipped_sortitem {
            return Err(ParseError::FeatureNotSupportedMessage(
                "SELECT DISTINCT ON expressions must match initial ORDER BY expressions".into(),
            ));
        }
        if result.iter().any(|clause| {
            clause.expr == entry.expr || clause.tle_sort_group_ref == entry.ressortgroupref
        }) {
            continue;
        }
        result.push(sort_group_clause_for_order_entry(
            entry,
            &mut next_sort_group_ref,
        ));
    }
    Ok(result)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedAggregateCall {
    proc_oid: u32,
    result_type: SqlType,
    declared_arg_types: Vec<SqlType>,
    func_variadic: bool,
    builtin_impl: Option<AggFunc>,
}

impl ResolvedAggregateCall {
    fn is_custom(&self) -> bool {
        self.builtin_impl.is_none()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResolvedHypotheticalAggregateCall {
    proc_oid: u32,
    result_type: SqlType,
    builtin_impl: HypotheticalAggFunc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResolvedOrderedSetAggregateCall {
    proc_oid: u32,
    result_type: SqlType,
    builtin_impl: OrderedSetAggFunc,
}

fn resolve_builtin_aggregate_call(
    catalog: &dyn CatalogLookup,
    func: AggFunc,
    arg_types: &[SqlType],
    func_variadic: bool,
) -> Option<ResolvedFunctionCall> {
    if matches!(
        func,
        AggFunc::RegrCount
            | AggFunc::RegrSxx
            | AggFunc::RegrSyy
            | AggFunc::RegrSxy
            | AggFunc::RegrAvgX
            | AggFunc::RegrAvgY
            | AggFunc::RegrR2
            | AggFunc::RegrSlope
            | AggFunc::RegrIntercept
            | AggFunc::CovarPop
            | AggFunc::CovarSamp
            | AggFunc::Corr
    ) && arg_types.len() == 2
    {
        let float8_args = [
            SqlType::new(SqlTypeKind::Float8),
            SqlType::new(SqlTypeKind::Float8),
        ];
        if let Ok(resolved) =
            resolve_function_call(catalog, func.name(), &float8_args, func_variadic)
        {
            return Some(resolved);
        }
    }
    if matches!(
        func,
        AggFunc::Sum
            | AggFunc::Avg
            | AggFunc::VarPop
            | AggFunc::VarSamp
            | AggFunc::StddevPop
            | AggFunc::StddevSamp
            | AggFunc::BoolAnd
            | AggFunc::BoolOr
    ) {
        return None;
    }
    resolve_function_call(catalog, func.name(), arg_types, func_variadic)
        .ok()
        .or_else(|| {
            // PostgreSQL treats unknown string literals as coercible to bytea for
            // string_agg(bytea, bytea). pgrust currently infers those literals as
            // text too early, so retry with a bytea delimiter when the first arg
            // already forces the bytea aggregate variant.
            if func == AggFunc::StringAgg
                && arg_types.len() == 2
                && arg_types[0].kind == SqlTypeKind::Bytea
            {
                let mut retried = arg_types.to_vec();
                retried[1] = SqlType::new(SqlTypeKind::Bytea);
                resolve_function_call(catalog, func.name(), &retried, func_variadic).ok()
            } else {
                None
            }
        })
}

fn resolve_aggregate_call(
    catalog: &dyn CatalogLookup,
    name: &str,
    arg_types: &[SqlType],
    func_variadic: bool,
) -> Option<ResolvedAggregateCall> {
    if let Some(func) = resolve_builtin_aggregate(name) {
        let resolved = resolve_builtin_aggregate_call(catalog, func, arg_types, func_variadic);
        return Some(ResolvedAggregateCall {
            proc_oid: resolved
                .as_ref()
                .map(|call| call.proc_oid)
                .or_else(|| proc_oid_for_builtin_aggregate_function(func))
                .unwrap_or(0),
            result_type: aggregate_sql_type(func, arg_types.first().copied()),
            declared_arg_types: resolved
                .as_ref()
                .map(|call| call.declared_arg_types.clone())
                .unwrap_or_else(|| fallback_builtin_aggregate_declared_arg_types(func, arg_types)),
            func_variadic: resolved
                .as_ref()
                .map(|call| call.func_variadic)
                .unwrap_or(func_variadic),
            builtin_impl: Some(func),
        });
    }

    let resolved = resolve_function_call(catalog, name, arg_types, func_variadic).ok()?;
    if resolved.hypothetical_agg_impl.is_some() {
        return None;
    }
    (resolved.prokind == 'a').then_some(ResolvedAggregateCall {
        proc_oid: resolved.proc_oid,
        result_type: concrete_polymorphic_aggregate_result_type(resolved.result_type, arg_types),
        declared_arg_types: resolved.declared_arg_types,
        func_variadic: resolved.func_variadic,
        builtin_impl: resolved.agg_impl,
    })
}

fn concrete_polymorphic_aggregate_result_type(
    result_type: SqlType,
    arg_types: &[SqlType],
) -> SqlType {
    match result_type.kind {
        SqlTypeKind::AnyArray | SqlTypeKind::AnyCompatibleArray => arg_types
            .iter()
            .copied()
            .find(|ty| ty.is_array)
            .unwrap_or(result_type),
        SqlTypeKind::AnyElement | SqlTypeKind::AnyCompatible => arg_types
            .iter()
            .copied()
            .find(|ty| ty.is_array)
            .map(SqlType::element_type)
            .or_else(|| arg_types.first().copied())
            .unwrap_or(result_type),
        _ => result_type,
    }
}

fn fallback_builtin_aggregate_declared_arg_types(
    func: AggFunc,
    arg_types: &[SqlType],
) -> Vec<SqlType> {
    match func {
        AggFunc::RegrCount
        | AggFunc::RegrSxx
        | AggFunc::RegrSyy
        | AggFunc::RegrSxy
        | AggFunc::RegrAvgX
        | AggFunc::RegrAvgY
        | AggFunc::RegrR2
        | AggFunc::RegrSlope
        | AggFunc::RegrIntercept
        | AggFunc::CovarPop
        | AggFunc::CovarSamp
        | AggFunc::Corr => vec![SqlType::new(SqlTypeKind::Float8); arg_types.len()],
        AggFunc::BoolAnd | AggFunc::BoolOr => {
            vec![SqlType::new(SqlTypeKind::Bool); arg_types.len()]
        }
        _ => arg_types.to_vec(),
    }
}

fn preserve_array_agg_array_arg_type(
    func: Option<AggFunc>,
    arg_types: &[SqlType],
    raw_args: &[SqlExpr],
    mut args: Vec<Expr>,
    catalog: &dyn CatalogLookup,
) -> Vec<Expr> {
    let array_arg_type = arg_types
        .first()
        .copied()
        .filter(|ty| ty.is_array)
        .or_else(|| {
            raw_args
                .first()
                .and_then(|arg| explicit_array_cast_type(arg, catalog))
        });
    if func == Some(AggFunc::ArrayAgg)
        && let (Some(arg_type), Some(first_arg)) = (array_arg_type, args.first_mut())
        && !expr_sql_type_hint(first_arg).is_some_and(|ty| ty.is_array)
    {
        *first_arg = Expr::Cast(Box::new(first_arg.clone()), arg_type);
    }
    args
}

fn explicit_array_cast_type(expr: &SqlExpr, catalog: &dyn CatalogLookup) -> Option<SqlType> {
    if let SqlExpr::Cast(_, raw_type) = expr
        && let Ok(ty) = resolve_raw_type_name(raw_type, catalog)
        && ty.is_array
    {
        return Some(ty);
    }
    None
}

fn resolve_hypothetical_aggregate_call(name: &str) -> Option<ResolvedHypotheticalAggregateCall> {
    let builtin_impl = resolve_builtin_hypothetical_aggregate(name)?;
    let proc_oid = proc_oid_for_builtin_hypothetical_aggregate_function(builtin_impl)?;
    Some(ResolvedHypotheticalAggregateCall {
        proc_oid,
        result_type: hypothetical_aggregate_sql_type(builtin_impl),
        builtin_impl,
    })
}

fn resolve_ordered_set_aggregate_call(
    name: &str,
    direct_arg_types: &[SqlType],
    aggregate_arg_types: &[SqlType],
) -> Option<ResolvedOrderedSetAggregateCall> {
    let builtin_impl =
        resolve_builtin_ordered_set_aggregate_impl(name, direct_arg_types, aggregate_arg_types)?;
    let proc_oid = ordered_set_aggregate_proc_oid(builtin_impl, aggregate_arg_types)?;
    Some(ResolvedOrderedSetAggregateCall {
        proc_oid,
        result_type: ordered_set_aggregate_sql_type(builtin_impl, aggregate_arg_types),
        builtin_impl,
    })
}

fn resolve_catalog_within_group_aggregate_call(
    catalog: &dyn CatalogLookup,
    name: &str,
    direct_arg_types: &[SqlType],
    aggregate_arg_types: &[SqlType],
    func_variadic: bool,
    expected_aggkind: char,
) -> Option<ResolvedAggregateCall> {
    let mut all_arg_types = Vec::with_capacity(direct_arg_types.len() + aggregate_arg_types.len());
    all_arg_types.extend_from_slice(direct_arg_types);
    all_arg_types.extend_from_slice(aggregate_arg_types);
    if let Some(resolved) = resolve_aggregate_call(catalog, name, &all_arg_types, func_variadic)
        && let Some(aggregate) = catalog.aggregate_by_fnoid(resolved.proc_oid)
        && aggregate.aggkind == expected_aggkind
        && aggregate.aggnumdirectargs as usize == direct_arg_types.len()
    {
        return Some(resolved);
    }

    catalog.proc_rows_by_name(name).into_iter().find_map(|row| {
        if row.prokind != 'a' {
            return None;
        }
        let aggregate = catalog.aggregate_by_fnoid(row.oid)?;
        if aggregate.aggkind != expected_aggkind
            || aggregate.aggnumdirectargs as usize != direct_arg_types.len()
        {
            return None;
        }
        let total_args = all_arg_types.len();
        let matches_arity = if row.provariadic == 0 {
            row.pronargs as usize == total_args
        } else {
            let fixed = row.pronargs.saturating_sub(1) as usize;
            total_args >= fixed
        };
        if !matches_arity {
            return None;
        }
        let result_type = catalog
            .type_by_oid(row.prorettype)
            .map(|row| row.sql_type)
            .unwrap_or_else(|| {
                aggregate_arg_types
                    .first()
                    .copied()
                    .unwrap_or_else(|| SqlType::new(SqlTypeKind::Text))
            });
        let result_type = concrete_polymorphic_aggregate_result_type(result_type, &all_arg_types);
        Some(ResolvedAggregateCall {
            proc_oid: row.oid,
            result_type,
            declared_arg_types: all_arg_types.clone(),
            func_variadic: row.provariadic != 0,
            builtin_impl: None,
        })
    })
}

fn resolve_builtin_ordered_set_aggregate_impl(
    name: &str,
    direct_arg_types: &[SqlType],
    aggregate_arg_types: &[SqlType],
) -> Option<OrderedSetAggFunc> {
    match resolve_builtin_ordered_set_aggregate(name)? {
        OrderedSetAggFunc::PercentileDisc => {
            if direct_arg_types.first().is_some_and(|ty| ty.is_array) {
                Some(OrderedSetAggFunc::PercentileDiscMulti)
            } else {
                Some(OrderedSetAggFunc::PercentileDisc)
            }
        }
        OrderedSetAggFunc::PercentileCont => {
            if direct_arg_types.first().is_some_and(|ty| ty.is_array) {
                Some(OrderedSetAggFunc::PercentileContMulti)
            } else {
                Some(OrderedSetAggFunc::PercentileCont)
            }
        }
        OrderedSetAggFunc::Mode
            if direct_arg_types.is_empty() && aggregate_arg_types.len() == 1 =>
        {
            Some(OrderedSetAggFunc::Mode)
        }
        _ => None,
    }
}

fn ordered_set_aggregate_proc_oid(
    func: OrderedSetAggFunc,
    aggregate_arg_types: &[SqlType],
) -> Option<u32> {
    match func {
        OrderedSetAggFunc::PercentileDisc => Some(PERCENTILE_DISC_AGG_PROC_OID),
        OrderedSetAggFunc::PercentileDiscMulti => Some(PERCENTILE_DISC_MULTI_AGG_PROC_OID),
        OrderedSetAggFunc::PercentileCont => {
            if aggregate_arg_types
                .first()
                .is_some_and(|ty| matches!(ty.kind, SqlTypeKind::Interval))
            {
                Some(PERCENTILE_CONT_INTERVAL_AGG_PROC_OID)
            } else {
                Some(PERCENTILE_CONT_FLOAT8_AGG_PROC_OID)
            }
        }
        OrderedSetAggFunc::PercentileContMulti => {
            if aggregate_arg_types
                .first()
                .is_some_and(|ty| matches!(ty.kind, SqlTypeKind::Interval))
            {
                Some(PERCENTILE_CONT_INTERVAL_MULTI_AGG_PROC_OID)
            } else {
                Some(PERCENTILE_CONT_FLOAT8_MULTI_AGG_PROC_OID)
            }
        }
        OrderedSetAggFunc::Mode => Some(MODE_AGG_PROC_OID),
    }
}

fn hypothetical_aggregate_sql_type(func: HypotheticalAggFunc) -> SqlType {
    match func {
        HypotheticalAggFunc::Rank | HypotheticalAggFunc::DenseRank => {
            SqlType::new(SqlTypeKind::Int8)
        }
        HypotheticalAggFunc::PercentRank | HypotheticalAggFunc::CumeDist => {
            SqlType::new(SqlTypeKind::Float8)
        }
    }
}

fn ordered_set_aggregate_sql_type(func: OrderedSetAggFunc, arg_types: &[SqlType]) -> SqlType {
    match func {
        OrderedSetAggFunc::PercentileDisc | OrderedSetAggFunc::Mode => arg_types
            .first()
            .copied()
            .unwrap_or_else(|| SqlType::new(SqlTypeKind::Text)),
        OrderedSetAggFunc::PercentileDiscMulti => SqlType::array_of(
            arg_types
                .first()
                .copied()
                .unwrap_or_else(|| SqlType::new(SqlTypeKind::Text)),
        ),
        OrderedSetAggFunc::PercentileCont => arg_types
            .first()
            .filter(|ty| matches!(ty.kind, SqlTypeKind::Interval))
            .copied()
            .unwrap_or_else(|| SqlType::new(SqlTypeKind::Float8)),
        OrderedSetAggFunc::PercentileContMulti => {
            if arg_types
                .first()
                .is_some_and(|ty| matches!(ty.kind, SqlTypeKind::Interval))
            {
                SqlType::array_of(
                    SqlType::new(SqlTypeKind::Interval).with_identity(INTERVAL_TYPE_OID, 0),
                )
                .with_identity(INTERVAL_ARRAY_TYPE_OID, 0)
            } else {
                SqlType::array_of(
                    SqlType::new(SqlTypeKind::Float8).with_identity(FLOAT8_TYPE_OID, 0),
                )
                .with_identity(FLOAT8_ARRAY_TYPE_OID, 0)
            }
        }
    }
}

fn ordered_set_requires_within_group_error(name: &str) -> ParseError {
    ParseError::DetailedError {
        message: format!("WITHIN GROUP is required for ordered-set aggregate {name}"),
        detail: None,
        hint: None,
        sqlstate: "42809",
    }
}

fn not_ordered_set_aggregate_error(name: &str) -> ParseError {
    ParseError::DetailedError {
        message: format!("{name} is not an ordered-set aggregate, so it cannot have WITHIN GROUP"),
        detail: None,
        hint: None,
        sqlstate: "42809",
    }
}

fn ordered_set_direct_arg_count_mismatch_error(
    name: &str,
    direct_arg_types: &[SqlType],
    aggregate_arg_types: &[SqlType],
) -> ParseError {
    let signature = direct_arg_types
        .iter()
        .chain(aggregate_arg_types.iter())
        .map(|ty| sql_type_name(*ty))
        .collect::<Vec<_>>()
        .join(", ");
    ParseError::DetailedError {
        message: format!("function {name}({signature}) does not exist"),
        detail: None,
        hint: Some(format!(
            "To use the ordered-set aggregate {name}, provide one direct percentile argument and one ordering column.",
        )),
        sqlstate: "42883",
    }
}

fn hypothetical_direct_arg_count_mismatch_error(
    name: &str,
    direct_arg_types: &[SqlType],
    aggregate_arg_types: &[SqlType],
) -> ParseError {
    let signature = direct_arg_types
        .iter()
        .chain(aggregate_arg_types.iter())
        .map(|ty| sql_type_name(*ty))
        .collect::<Vec<_>>()
        .join(", ");
    ParseError::DetailedError {
        message: format!("function {name}({signature}) does not exist"),
        detail: None,
        hint: Some(format!(
            "To use the hypothetical-set aggregate {name}, the number of hypothetical direct arguments (here {}) must match the number of ordering columns (here {}).",
            direct_arg_types.len(),
            aggregate_arg_types.len(),
        )),
        sqlstate: "42883",
    }
}

fn hypothetical_within_group_type_mismatch_error(
    direct_type: SqlType,
    aggregate_type: SqlType,
) -> ParseError {
    ParseError::DetailedError {
        message: format!(
            "WITHIN GROUP types {} and {} cannot be matched",
            sql_type_name(aggregate_type),
            sql_type_name(direct_type),
        ),
        detail: None,
        hint: None,
        sqlstate: "42804",
    }
}

fn coerce_hypothetical_aggregate_inputs(
    name: &str,
    direct_args: &[SqlFunctionArg],
    direct_arg_types: &[SqlType],
    bound_direct_args: Vec<Expr>,
    aggregate_args: &[SqlFunctionArg],
    aggregate_arg_types: &[SqlType],
    bound_args: Vec<Expr>,
    order_by: &[OrderByItem],
    bound_order_exprs: Vec<Expr>,
    catalog: &dyn CatalogLookup,
) -> Result<(Vec<Expr>, Vec<Expr>, Vec<OrderByEntry>), ParseError> {
    if direct_args.len() != aggregate_args.len() {
        return Err(hypothetical_direct_arg_count_mismatch_error(
            name,
            direct_arg_types,
            aggregate_arg_types,
        ));
    }

    let mut coerced_direct_args = Vec::with_capacity(direct_args.len());
    let mut coerced_args = Vec::with_capacity(aggregate_args.len());
    let mut coerced_order_by = Vec::with_capacity(order_by.len());

    for (
        ((((direct_arg, direct_type), bound_direct_arg), aggregate_arg), aggregate_type),
        (order_item, (bound_arg, bound_order_expr)),
    ) in direct_args
        .iter()
        .zip(direct_arg_types.iter().copied())
        .zip(bound_direct_args.into_iter())
        .zip(aggregate_args.iter())
        .zip(aggregate_arg_types.iter().copied())
        .zip(
            order_by
                .iter()
                .zip(bound_args.into_iter().zip(bound_order_exprs.into_iter())),
        )
    {
        let direct_type =
            coerce_unknown_string_literal_type(&direct_arg.value, direct_type, aggregate_type);
        let aggregate_type =
            coerce_unknown_string_literal_type(&aggregate_arg.value, aggregate_type, direct_type);
        let common_type =
            resolve_common_scalar_type(direct_type, aggregate_type).ok_or_else(|| {
                hypothetical_within_group_type_mismatch_error(direct_type, aggregate_type)
            })?;
        let (direct_expr, direct_explicit_collation) = strip_explicit_collation(bound_direct_arg);
        let (arg_expr, _) = strip_explicit_collation(bound_arg);
        let (order_expr, order_explicit_collation) = strip_explicit_collation(bound_order_expr);
        let collation_oid = derive_consumer_collation(
            catalog,
            CollationConsumer::OrderBy,
            &[
                (common_type, direct_explicit_collation),
                (common_type, order_explicit_collation),
            ],
        )?;
        coerced_direct_args.push(coerce_bound_expr(direct_expr, direct_type, common_type));
        coerced_args.push(coerce_bound_expr(arg_expr, aggregate_type, common_type));
        let mut order_by_entry = build_bound_order_by_entry(
            order_item,
            coerce_bound_expr(order_expr, aggregate_type, common_type),
            0,
            catalog,
        )?;
        order_by_entry.collation_oid = collation_oid;
        coerced_order_by.push(order_by_entry);
    }

    Ok((coerced_direct_args, coerced_args, coerced_order_by))
}

fn coerce_ordered_set_aggregate_inputs(
    name: &str,
    direct_args: &[SqlFunctionArg],
    direct_arg_types: &[SqlType],
    bound_direct_args: Vec<Expr>,
    aggregate_args: &[SqlFunctionArg],
    aggregate_arg_types: &[SqlType],
    bound_args: Vec<Expr>,
    order_by: &[OrderByItem],
    bound_order_exprs: Vec<Expr>,
    catalog: &dyn CatalogLookup,
) -> Result<(Vec<Expr>, Vec<Expr>, Vec<OrderByEntry>), ParseError> {
    let Some(func) =
        resolve_builtin_ordered_set_aggregate_impl(name, direct_arg_types, aggregate_arg_types)
    else {
        return Err(ParseError::UnexpectedToken {
            expected: "supported ordered-set aggregate",
            actual: name.to_string(),
        });
    };
    let expected_direct_args = if matches!(func, OrderedSetAggFunc::Mode) {
        0
    } else {
        1
    };
    if direct_args.len() != expected_direct_args || aggregate_args.len() != 1 || order_by.len() != 1
    {
        return Err(ordered_set_direct_arg_count_mismatch_error(
            name,
            direct_arg_types,
            aggregate_arg_types,
        ));
    }

    let (coerced_direct_args, bound_direct_args) = if expected_direct_args == 0 {
        (Vec::new(), bound_direct_args)
    } else {
        let target_direct_type = if matches!(
            func,
            OrderedSetAggFunc::PercentileDiscMulti | OrderedSetAggFunc::PercentileContMulti
        ) {
            SqlType::array_of(SqlType::new(SqlTypeKind::Float8).with_identity(FLOAT8_TYPE_OID, 0))
                .with_identity(FLOAT8_ARRAY_TYPE_OID, 0)
        } else {
            SqlType::new(SqlTypeKind::Float8)
        };
        let direct_type = coerce_unknown_string_literal_type(
            &direct_args[0].value,
            direct_arg_types
                .first()
                .copied()
                .unwrap_or(target_direct_type),
            target_direct_type,
        );
        let direct_expr = bound_direct_args
            .into_iter()
            .next()
            .expect("direct arg length checked above");
        (
            vec![coerce_bound_expr(
                direct_expr,
                direct_type,
                target_direct_type,
            )],
            Vec::new(),
        )
    };
    drop(bound_direct_args);

    let mut target_arg_type = aggregate_arg_types
        .first()
        .copied()
        .unwrap_or_else(|| SqlType::new(SqlTypeKind::Text));
    if matches!(
        func,
        OrderedSetAggFunc::PercentileCont | OrderedSetAggFunc::PercentileContMulti
    ) && !matches!(target_arg_type.kind, SqlTypeKind::Interval)
    {
        target_arg_type = SqlType::new(SqlTypeKind::Float8);
    }
    let arg_type = coerce_unknown_string_literal_type(
        &aggregate_args[0].value,
        aggregate_arg_types
            .first()
            .copied()
            .unwrap_or(target_arg_type),
        SqlType::new(SqlTypeKind::Text),
    );
    let arg_expr = bound_args
        .into_iter()
        .next()
        .expect("aggregate arg length checked above");
    let order_expr = bound_order_exprs
        .into_iter()
        .next()
        .expect("order arg length checked above");

    let coerced_arg = coerce_bound_expr(arg_expr, arg_type, target_arg_type);
    let order_by_entry = build_bound_order_by_entry(
        &order_by[0],
        coerce_bound_expr(order_expr, arg_type, target_arg_type),
        0,
        catalog,
    )?;

    Ok((coerced_direct_args, vec![coerced_arg], vec![order_by_entry]))
}

fn coerce_catalog_within_group_aggregate_inputs(
    direct_args: &[SqlFunctionArg],
    direct_arg_types: &[SqlType],
    bound_direct_args: Vec<Expr>,
    aggregate_args: &[SqlFunctionArg],
    aggregate_arg_types: &[SqlType],
    bound_args: Vec<Expr>,
    order_by: &[OrderByItem],
    bound_order_exprs: Vec<Expr>,
    declared_arg_types: &[SqlType],
    catalog: &dyn CatalogLookup,
) -> Result<(Vec<Expr>, Vec<Expr>, Vec<OrderByEntry>), ParseError> {
    if aggregate_args_are_named(direct_args) || aggregate_args_are_named(aggregate_args) {
        return Err(ParseError::UnexpectedToken {
            expected: "aggregate arguments without names",
            actual: "named ordered-set aggregate arguments".into(),
        });
    }
    let direct_count = direct_args.len();
    let mut coerced_direct_args = Vec::with_capacity(bound_direct_args.len());
    for (index, (expr, actual_type)) in bound_direct_args
        .into_iter()
        .zip(direct_arg_types.iter().copied())
        .enumerate()
    {
        let declared_type = declared_arg_types
            .get(index)
            .copied()
            .unwrap_or(actual_type);
        coerced_direct_args.push(coerce_bound_expr(expr, actual_type, declared_type));
    }

    let mut coerced_args = Vec::with_capacity(bound_args.len());
    let mut bound_order_by = Vec::with_capacity(bound_order_exprs.len());
    for (index, ((arg_expr, actual_type), (order_item, order_expr))) in bound_args
        .into_iter()
        .zip(aggregate_arg_types.iter().copied())
        .zip(order_by.iter().zip(bound_order_exprs.into_iter()))
        .enumerate()
    {
        let declared_type = declared_arg_types
            .get(direct_count + index)
            .copied()
            .unwrap_or(actual_type);
        coerced_args.push(coerce_bound_expr(arg_expr, actual_type, declared_type));
        bound_order_by.push(build_bound_order_by_entry(
            order_item,
            coerce_bound_expr(order_expr, actual_type, declared_type),
            0,
            catalog,
        )?);
    }
    Ok((coerced_direct_args, coerced_args, bound_order_by))
}
fn validate_distinct_aggregate_order_by(
    arg_values: &[SqlExpr],
    order_by: &[OrderByItem],
    distinct: bool,
) -> Result<(), ParseError> {
    if !distinct {
        return Ok(());
    }
    for item in order_by {
        if !arg_values.iter().any(|arg| arg == &item.expr) {
            return Err(ParseError::UnexpectedToken {
                expected: "ORDER BY expressions in DISTINCT aggregate argument list",
                actual: "ORDER BY expression must appear in argument list".into(),
            });
        }
    }
    Ok(())
}

pub(crate) fn default_pg_settings_rows() -> Vec<Vec<Value>> {
    vec![
        vec![
            Value::Text("wal_segment_size".into()),
            Value::Text(
                crate::backend::access::transam::xlog::WAL_SEG_SIZE_BYTES
                    .to_string()
                    .into(),
            ),
        ],
        vec![Value::Text("jit".into()), Value::Text("off".into())],
        vec![
            Value::Text("max_prepared_transactions".into()),
            Value::Text("64".into()),
        ],
    ]
}

pub trait CatalogLookup {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation>;

    fn materialize_visible_catalog(&self) -> Option<VisibleCatalog> {
        None
    }

    fn relation_by_oid(&self, _relation_oid: u32) -> Option<BoundRelation> {
        None
    }

    fn current_user_oid(&self) -> u32 {
        BOOTSTRAP_SUPERUSER_OID
    }

    fn search_path(&self) -> Vec<String> {
        Vec::new()
    }

    fn session_user_oid(&self) -> u32 {
        BOOTSTRAP_SUPERUSER_OID
    }

    fn authid_rows(&self) -> Vec<PgAuthIdRow> {
        Vec::new()
    }

    fn auth_members_rows(&self) -> Vec<PgAuthMembersRow> {
        Vec::new()
    }

    fn depend_rows(&self) -> Vec<PgDependRow> {
        Vec::new()
    }

    fn sequence_rows(&self) -> Vec<PgSequenceRow> {
        Vec::new()
    }

    fn depend_rows_referencing(
        &self,
        refclassid: u32,
        refobjid: u32,
        refobjsubid: Option<i32>,
    ) -> Vec<PgDependRow> {
        self.depend_rows()
            .into_iter()
            .filter(|row| {
                row.refclassid == refclassid
                    && row.refobjid == refobjid
                    && refobjsubid.is_none_or(|objsubid| row.refobjsubid == objsubid)
            })
            .collect()
    }

    fn role_name_by_oid(&self, role_oid: u32) -> Option<String> {
        self.authid_rows()
            .into_iter()
            .find(|row| row.oid == role_oid)
            .map(|row| row.rolname)
    }

    fn database_rows(&self) -> Vec<PgDatabaseRow> {
        bootstrap_pg_database_rows().to_vec()
    }

    fn database_row_by_oid(&self, oid: u32) -> Option<PgDatabaseRow> {
        self.database_rows().into_iter().find(|row| row.oid == oid)
    }

    fn event_trigger_rows(&self) -> Vec<PgEventTriggerRow> {
        Vec::new()
    }

    fn event_trigger_row_by_oid(&self, oid: u32) -> Option<PgEventTriggerRow> {
        self.event_trigger_rows()
            .into_iter()
            .find(|row| row.oid == oid)
    }

    fn event_trigger_row_by_name(&self, name: &str) -> Option<PgEventTriggerRow> {
        self.event_trigger_rows()
            .into_iter()
            .find(|row| row.evtname == name)
    }

    fn tablespace_rows(&self) -> Vec<PgTablespaceRow> {
        bootstrap_pg_tablespace_rows().to_vec()
    }

    fn namespace_row_by_oid(&self, oid: u32) -> Option<PgNamespaceRow> {
        bootstrap_pg_namespace_rows()
            .into_iter()
            .find(|row| row.oid == oid)
    }

    fn namespace_rows(&self) -> Vec<PgNamespaceRow> {
        bootstrap_pg_namespace_rows().to_vec()
    }

    fn row_security_enabled(&self) -> bool {
        true
    }

    fn current_relation_pages(&self, _relation_oid: u32) -> Option<u32> {
        None
    }

    fn current_relation_live_tuples(&self, _relation_oid: u32) -> Option<f64> {
        None
    }

    fn brin_pages_per_range(&self, _relation_oid: u32) -> Option<u32> {
        None
    }

    fn index_relations_for_heap(&self, _relation_oid: u32) -> Vec<BoundIndexRelation> {
        Vec::new()
    }

    fn index_relations_for_heap_with_cache(
        &self,
        relation_oid: u32,
        _index_expr_cache: &RefCell<BTreeMap<u32, PlannerIndexExprCacheEntry>>,
    ) -> Vec<BoundIndexRelation> {
        self.index_relations_for_heap(relation_oid)
    }

    fn index_row_by_oid(&self, _index_oid: u32) -> Option<PgIndexRow> {
        None
    }

    fn lookup_relation(&self, name: &str) -> Option<BoundRelation> {
        self.lookup_any_relation(name)
            .filter(|entry| entry.relkind == 'r')
    }

    fn lookup_analyzable_relation(&self, name: &str) -> Option<BoundRelation> {
        self.lookup_any_relation(name)
            .filter(|entry| relkind_is_analyzable(entry.relkind))
    }

    fn lookup_relation_by_oid(&self, _relation_oid: u32) -> Option<BoundRelation> {
        None
    }

    fn proc_rows_by_name(&self, name: &str) -> Vec<PgProcRow> {
        let mut rows = bootstrap_pg_proc_rows_by_name(name);
        extend_synthetic_range_proc_rows(self, name, &mut rows);
        dedup_proc_rows(&mut rows);
        rows
    }

    fn proc_row_by_oid(&self, oid: u32) -> Option<PgProcRow> {
        bootstrap_pg_proc_row_by_oid(oid)
            .or_else(|| synthetic_range_proc_row_by_oid(oid, &self.type_rows(), &self.range_rows()))
    }

    fn proc_rows(&self) -> Vec<PgProcRow> {
        bootstrap_pg_proc_rows()
    }

    fn opclass_rows(&self) -> Vec<PgOpclassRow> {
        bootstrap_pg_opclass_rows()
    }

    fn opfamily_rows(&self) -> Vec<PgOpfamilyRow> {
        bootstrap_pg_opfamily_rows()
    }

    fn am_rows(&self) -> Vec<PgAmRow> {
        bootstrap_pg_am_rows().to_vec()
    }

    fn amproc_rows(&self) -> Vec<PgAmprocRow> {
        bootstrap_pg_amproc_rows()
    }

    fn amop_rows(&self) -> Vec<PgAmopRow> {
        bootstrap_pg_amop_rows()
    }

    fn collation_rows(&self) -> Vec<PgCollationRow> {
        bootstrap_pg_collation_rows().to_vec()
    }

    fn aggregate_by_fnoid(&self, aggfnoid: u32) -> Option<PgAggregateRow> {
        bootstrap_pg_aggregate_rows()
            .into_iter()
            .find(|row| row.aggfnoid == aggfnoid)
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

    fn operator_by_oid(&self, oid: u32) -> Option<PgOperatorRow> {
        bootstrap_pg_operator_rows()
            .into_iter()
            .find(|row| row.oid == oid)
    }

    fn operator_rows(&self) -> Vec<PgOperatorRow> {
        bootstrap_pg_operator_rows()
    }

    fn ts_config_rows(&self) -> Vec<PgTsConfigRow> {
        bootstrap_pg_ts_config_rows().to_vec()
    }

    fn ts_parser_rows(&self) -> Vec<PgTsParserRow> {
        bootstrap_pg_ts_parser_rows().to_vec()
    }

    fn ts_dict_rows(&self) -> Vec<PgTsDictRow> {
        bootstrap_pg_ts_dict_rows().to_vec()
    }

    fn ts_template_rows(&self) -> Vec<PgTsTemplateRow> {
        bootstrap_pg_ts_template_rows().to_vec()
    }

    fn ts_config_map_rows(&self) -> Vec<PgTsConfigMapRow> {
        bootstrap_pg_ts_config_map_rows()
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

    fn cast_rows(&self) -> Vec<PgCastRow> {
        bootstrap_pg_cast_rows()
    }

    fn conversion_rows(&self) -> Vec<PgConversionRow> {
        bootstrap_pg_conversion_rows().to_vec()
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        builtin_type_rows()
    }

    fn type_by_oid(&self, oid: u32) -> Option<PgTypeRow> {
        self.type_rows().into_iter().find(|row| row.oid == oid)
    }

    fn type_by_name(&self, name: &str) -> Option<PgTypeRow> {
        let normalized = normalize_catalog_lookup_name(name);
        self.type_rows()
            .into_iter()
            .find(|row| row.typname.eq_ignore_ascii_case(normalized))
    }

    fn domain_by_name(&self, _name: &str) -> Option<DomainLookup> {
        None
    }

    fn domain_by_type_oid(&self, _domain_oid: u32) -> Option<DomainLookup> {
        None
    }

    fn type_default_sql(&self, _type_oid: u32) -> Option<String> {
        None
    }

    fn range_rows(&self) -> Vec<PgRangeRow> {
        builtin_range_rows()
    }

    fn range_row_by_type_oid(&self, oid: u32) -> Option<PgRangeRow> {
        self.range_rows()
            .into_iter()
            .find(|row| row.rngtypid == oid)
    }

    fn enum_label_oid(&self, _type_oid: u32, _label: &str) -> Option<u32> {
        None
    }

    fn enum_label(&self, _type_oid: u32, _label_oid: u32) -> Option<String> {
        None
    }

    fn enum_label_by_oid(&self, label_oid: u32) -> Option<String> {
        self.enum_rows()
            .into_iter()
            .find(|row| row.oid == label_oid)
            .map(|row| row.enumlabel)
    }

    fn enum_rows(&self) -> Vec<PgEnumRow> {
        bootstrap_pg_enum_rows().to_vec()
    }

    fn enum_label_is_committed(&self, _type_oid: u32, _label_oid: u32) -> bool {
        true
    }

    fn domain_allowed_enum_label_oids(&self, _domain_oid: u32) -> Option<Vec<u32>> {
        None
    }

    fn domain_check_name(&self, _domain_oid: u32) -> Option<String> {
        None
    }

    fn domain_check_by_type_oid(&self, _domain_oid: u32) -> Option<String> {
        None
    }

    fn type_oid_for_sql_type(&self, sql_type: SqlType) -> Option<u32> {
        if let Some(range_type) = range_type_ref_for_sql_type(sql_type) {
            if sql_type.is_array {
                return self
                    .type_rows()
                    .into_iter()
                    .find(|row| row.typelem == range_type.type_oid())
                    .map(|row| row.oid);
            }
            return Some(range_type.type_oid());
        }
        if let Some(multirange_type) = multirange_type_ref_for_sql_type(sql_type) {
            if sql_type.is_array {
                return self
                    .type_rows()
                    .into_iter()
                    .find(|row| row.typelem == multirange_type.type_oid())
                    .map(|row| row.oid);
            }
            return Some(multirange_type.type_oid());
        }
        if !sql_type.is_array && sql_type.type_oid != 0 {
            return Some(sql_type.type_oid);
        }
        if let Some(row) = self
            .type_rows()
            .into_iter()
            .find(|row| row.sql_type == sql_type)
        {
            return Some(row.oid);
        }
        let mut fallback = None;
        for row in self.type_rows() {
            if row.oid == crate::include::catalog::UNKNOWN_TYPE_OID {
                continue;
            }
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

    fn language_rows(&self) -> Vec<PgLanguageRow> {
        bootstrap_pg_language_rows().to_vec()
    }

    fn language_row_by_oid(&self, oid: u32) -> Option<PgLanguageRow> {
        self.language_rows().into_iter().find(|row| row.oid == oid)
    }

    fn language_row_by_name(&self, name: &str) -> Option<PgLanguageRow> {
        let normalized = normalize_catalog_lookup_name(name);
        self.language_rows()
            .into_iter()
            .find(|row| row.lanname.eq_ignore_ascii_case(normalized))
    }

    fn rewrite_rows_for_relation(&self, _relation_oid: u32) -> Vec<PgRewriteRow> {
        Vec::new()
    }

    fn rewrite_rows(&self) -> Vec<PgRewriteRow> {
        Vec::new()
    }

    fn rewrite_row_by_oid(&self, rewrite_oid: u32) -> Option<PgRewriteRow> {
        self.rewrite_rows()
            .into_iter()
            .find(|row| row.oid == rewrite_oid)
    }

    fn statistic_ext_rows(&self) -> Vec<PgStatisticExtRow> {
        Vec::new()
    }

    fn statistic_ext_row_by_oid(&self, oid: u32) -> Option<PgStatisticExtRow> {
        self.statistic_ext_rows()
            .into_iter()
            .find(|row| row.oid == oid)
    }

    fn statistic_ext_row_by_name_namespace(
        &self,
        name: &str,
        namespace_oid: u32,
    ) -> Option<PgStatisticExtRow> {
        let normalized = normalize_catalog_lookup_name(name);
        self.statistic_ext_rows().into_iter().find(|row| {
            row.stxnamespace == namespace_oid && row.stxname.eq_ignore_ascii_case(normalized)
        })
    }

    fn statistic_ext_rows_for_relation(&self, relation_oid: u32) -> Vec<PgStatisticExtRow> {
        self.statistic_ext_rows()
            .into_iter()
            .filter(|row| row.stxrelid == relation_oid)
            .collect()
    }

    fn statistic_ext_data_rows(&self) -> Vec<PgStatisticExtDataRow> {
        Vec::new()
    }

    fn statistic_ext_data_row(
        &self,
        stxoid: u32,
        stxdinherit: bool,
    ) -> Option<PgStatisticExtDataRow> {
        self.statistic_ext_data_rows()
            .into_iter()
            .find(|row| row.stxoid == stxoid && row.stxdinherit == stxdinherit)
    }

    fn trigger_rows_for_relation(
        &self,
        _relation_oid: u32,
    ) -> Vec<crate::include::catalog::PgTriggerRow> {
        Vec::new()
    }

    fn trigger_rows(&self) -> Vec<crate::include::catalog::PgTriggerRow> {
        Vec::new()
    }

    fn policy_rows_for_relation(
        &self,
        _relation_oid: u32,
    ) -> Vec<crate::include::catalog::PgPolicyRow> {
        Vec::new()
    }

    fn constraint_rows_for_relation(&self, _relation_oid: u32) -> Vec<PgConstraintRow> {
        Vec::new()
    }

    fn constraint_row_by_oid(&self, oid: u32) -> Option<PgConstraintRow> {
        self.constraint_rows()
            .into_iter()
            .find(|row| row.oid == oid)
    }

    fn constraint_rows_for_index(&self, index_oid: u32) -> Vec<PgConstraintRow> {
        self.constraint_rows()
            .into_iter()
            .filter(|row| row.conindid == index_oid)
            .collect()
    }

    fn foreign_key_constraint_rows_referencing_relation(
        &self,
        relation_oid: u32,
    ) -> Vec<PgConstraintRow> {
        self.constraint_rows()
            .into_iter()
            .filter(|row| {
                row.contype == crate::include::catalog::CONSTRAINT_FOREIGN
                    && row.confrelid == relation_oid
            })
            .collect()
    }

    fn foreign_key_constraint_rows_referencing_index(
        &self,
        index_oid: u32,
    ) -> Vec<PgConstraintRow> {
        self.constraint_rows()
            .into_iter()
            .filter(|row| {
                row.contype == crate::include::catalog::CONSTRAINT_FOREIGN
                    && row.conindid == index_oid
            })
            .collect()
    }

    fn constraint_rows(&self) -> Vec<PgConstraintRow> {
        Vec::new()
    }

    fn class_row_by_oid(&self, _relation_oid: u32) -> Option<PgClassRow> {
        None
    }

    fn attribute_rows_for_relation(
        &self,
        _relation_oid: u32,
    ) -> Vec<crate::include::catalog::PgAttributeRow> {
        Vec::new()
    }

    fn attribute_rows(&self) -> Vec<PgAttributeRow> {
        self.class_rows()
            .into_iter()
            .flat_map(|class| self.attribute_rows_for_relation(class.oid))
            .collect()
    }

    fn class_rows(&self) -> Vec<PgClassRow> {
        Vec::new()
    }

    fn partitioned_table_row(&self, _relation_oid: u32) -> Option<PgPartitionedTableRow> {
        None
    }

    fn partitioned_table_rows(&self) -> Vec<PgPartitionedTableRow> {
        Vec::new()
    }

    fn inheritance_parents(&self, _relation_oid: u32) -> Vec<PgInheritsRow> {
        Vec::new()
    }

    fn inheritance_children(&self, _relation_oid: u32) -> Vec<PgInheritsRow> {
        Vec::new()
    }

    fn inheritance_rows(&self) -> Vec<PgInheritsRow> {
        Vec::new()
    }

    fn publication_rows(&self) -> Vec<PgPublicationRow> {
        Vec::new()
    }

    fn publication_rel_rows(&self) -> Vec<PgPublicationRelRow> {
        Vec::new()
    }

    fn publication_namespace_rows(&self) -> Vec<PgPublicationNamespaceRow> {
        Vec::new()
    }

    fn find_all_inheritors(&self, relation_oid: u32) -> Vec<u32> {
        let mut out = vec![relation_oid];
        let mut pending = vec![relation_oid];
        while let Some(parent_oid) = pending.pop() {
            let mut child_oids = self
                .inheritance_children(parent_oid)
                .into_iter()
                .filter(|row| !row.inhdetachpending)
                .map(|row| row.inhrelid)
                .collect::<Vec<_>>();
            child_oids.sort_unstable();
            child_oids.dedup();
            for child_oid in child_oids {
                if out.contains(&child_oid) {
                    continue;
                }
                out.push(child_oid);
                pending.push(child_oid);
            }
        }
        out.sort_unstable();
        out
    }

    fn has_subclass(&self, relation_oid: u32) -> bool {
        self.class_row_by_oid(relation_oid)
            .map(|row| row.relhassubclass)
            .unwrap_or_else(|| !self.inheritance_children(relation_oid).is_empty())
    }

    fn statistic_rows_for_relation(&self, _relation_oid: u32) -> Vec<PgStatisticRow> {
        Vec::new()
    }

    fn foreign_data_wrapper_rows(&self) -> Vec<PgForeignDataWrapperRow> {
        Vec::new()
    }

    fn foreign_data_wrapper_row_by_oid(&self, oid: u32) -> Option<PgForeignDataWrapperRow> {
        self.foreign_data_wrapper_rows()
            .into_iter()
            .find(|row| row.oid == oid)
    }

    fn foreign_server_rows(&self) -> Vec<PgForeignServerRow> {
        Vec::new()
    }

    fn foreign_table_rows(&self) -> Vec<PgForeignTableRow> {
        Vec::new()
    }

    fn user_mapping_rows(&self) -> Vec<PgUserMappingRow> {
        Vec::new()
    }

    fn pg_tables_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_views_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_matviews_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_indexes_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_policies_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_rules_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stats_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stats_ext_rows(&self) -> Vec<Vec<Value>> {
        build_pg_stats_ext_rows(
            self.namespace_rows(),
            self.authid_rows(),
            self.auth_members_rows(),
            self.class_rows(),
            self.attribute_rows(),
            self.statistic_ext_rows(),
            self.statistic_ext_data_rows(),
            self.current_user_oid(),
        )
    }

    fn pg_stats_ext_exprs_rows(&self) -> Vec<Vec<Value>> {
        build_pg_stats_ext_exprs_rows(
            self.namespace_rows(),
            self.authid_rows(),
            self.auth_members_rows(),
            self.class_rows(),
            self.statistic_ext_rows(),
            self.statistic_ext_data_rows(),
            self.current_user_oid(),
        )
    }

    fn pg_settings_rows(&self) -> Vec<Vec<Value>> {
        default_pg_settings_rows()
    }

    fn pg_user_mappings_rows(&self) -> Vec<Vec<Value>> {
        build_pg_user_mappings_rows(
            self.authid_rows(),
            self.auth_members_rows(),
            self.foreign_server_rows(),
            self.user_mapping_rows(),
            self.current_user_oid(),
        )
    }

    fn pg_stat_activity_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stat_database_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stat_checkpointer_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stat_wal_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stat_slru_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stat_archiver_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stat_bgwriter_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stat_recovery_prefetch_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stat_subscription_stats_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stat_all_tables_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stat_user_tables_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_statio_user_tables_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stat_user_functions_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stat_io_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stat_progress_copy_rows(&self) -> Vec<Vec<Value>> {
        current_pg_stat_progress_copy_rows()
    }

    fn pg_locks_rows(&self) -> Vec<Vec<Value>> {
        build_pg_locks_rows(Vec::new())
    }
}

struct IndexExpressionCatalogLookup<'a> {
    inner: &'a dyn CatalogLookup,
}

impl CatalogLookup for IndexExpressionCatalogLookup<'_> {
    fn lookup_any_relation(&self, _name: &str) -> Option<BoundRelation> {
        None
    }

    fn lookup_relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.inner.lookup_relation_by_oid(relation_oid)
    }

    fn relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.inner.relation_by_oid(relation_oid)
    }

    fn current_user_oid(&self) -> u32 {
        self.inner.current_user_oid()
    }

    fn search_path(&self) -> Vec<String> {
        self.inner.search_path()
    }

    fn session_user_oid(&self) -> u32 {
        self.inner.session_user_oid()
    }

    fn authid_rows(&self) -> Vec<PgAuthIdRow> {
        self.inner.authid_rows()
    }

    fn auth_members_rows(&self) -> Vec<PgAuthMembersRow> {
        self.inner.auth_members_rows()
    }

    fn namespace_row_by_oid(&self, oid: u32) -> Option<PgNamespaceRow> {
        self.inner.namespace_row_by_oid(oid)
    }

    fn namespace_rows(&self) -> Vec<PgNamespaceRow> {
        self.inner.namespace_rows()
    }

    fn row_security_enabled(&self) -> bool {
        self.inner.row_security_enabled()
    }

    fn current_relation_pages(&self, relation_oid: u32) -> Option<u32> {
        self.inner.current_relation_pages(relation_oid)
    }

    fn current_relation_live_tuples(&self, relation_oid: u32) -> Option<f64> {
        self.inner.current_relation_live_tuples(relation_oid)
    }

    fn brin_pages_per_range(&self, relation_oid: u32) -> Option<u32> {
        self.inner.brin_pages_per_range(relation_oid)
    }

    fn index_relations_for_heap(&self, relation_oid: u32) -> Vec<BoundIndexRelation> {
        panic!("index expression binding must not discover indexes for relation {relation_oid}");
    }

    fn proc_rows_by_name(&self, name: &str) -> Vec<PgProcRow> {
        self.inner.proc_rows_by_name(name)
    }

    fn proc_row_by_oid(&self, oid: u32) -> Option<PgProcRow> {
        self.inner.proc_row_by_oid(oid)
    }

    fn opclass_rows(&self) -> Vec<PgOpclassRow> {
        self.inner.opclass_rows()
    }

    fn opfamily_rows(&self) -> Vec<PgOpfamilyRow> {
        self.inner.opfamily_rows()
    }

    fn collation_rows(&self) -> Vec<PgCollationRow> {
        self.inner.collation_rows()
    }

    fn aggregate_by_fnoid(&self, aggfnoid: u32) -> Option<PgAggregateRow> {
        self.inner.aggregate_by_fnoid(aggfnoid)
    }

    fn operator_by_name_left_right(
        &self,
        name: &str,
        left_type_oid: u32,
        right_type_oid: u32,
    ) -> Option<PgOperatorRow> {
        self.inner
            .operator_by_name_left_right(name, left_type_oid, right_type_oid)
    }

    fn operator_by_oid(&self, oid: u32) -> Option<PgOperatorRow> {
        self.inner.operator_by_oid(oid)
    }

    fn cast_by_source_target(
        &self,
        source_type_oid: u32,
        target_type_oid: u32,
    ) -> Option<PgCastRow> {
        self.inner
            .cast_by_source_target(source_type_oid, target_type_oid)
    }

    fn cast_rows(&self) -> Vec<PgCastRow> {
        self.inner.cast_rows()
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        self.inner.type_rows()
    }

    fn type_by_oid(&self, oid: u32) -> Option<PgTypeRow> {
        self.inner.type_by_oid(oid)
    }

    fn type_by_name(&self, name: &str) -> Option<PgTypeRow> {
        self.inner.type_by_name(name)
    }

    fn domain_by_name(&self, name: &str) -> Option<DomainLookup> {
        self.inner.domain_by_name(name)
    }

    fn domain_by_type_oid(&self, domain_oid: u32) -> Option<DomainLookup> {
        self.inner.domain_by_type_oid(domain_oid)
    }

    fn range_rows(&self) -> Vec<PgRangeRow> {
        self.inner.range_rows()
    }

    fn range_row_by_type_oid(&self, oid: u32) -> Option<PgRangeRow> {
        self.inner.range_row_by_type_oid(oid)
    }

    fn enum_label_oid(&self, type_oid: u32, label: &str) -> Option<u32> {
        self.inner.enum_label_oid(type_oid, label)
    }

    fn enum_label(&self, type_oid: u32, label_oid: u32) -> Option<String> {
        self.inner.enum_label(type_oid, label_oid)
    }

    fn enum_label_by_oid(&self, label_oid: u32) -> Option<String> {
        self.inner.enum_label_by_oid(label_oid)
    }

    fn enum_rows(&self) -> Vec<PgEnumRow> {
        self.inner.enum_rows()
    }

    fn enum_label_is_committed(&self, type_oid: u32, label_oid: u32) -> bool {
        self.inner.enum_label_is_committed(type_oid, label_oid)
    }

    fn domain_allowed_enum_label_oids(&self, domain_oid: u32) -> Option<Vec<u32>> {
        self.inner.domain_allowed_enum_label_oids(domain_oid)
    }

    fn domain_check_name(&self, domain_oid: u32) -> Option<String> {
        self.inner.domain_check_name(domain_oid)
    }

    fn domain_check_by_type_oid(&self, domain_oid: u32) -> Option<String> {
        self.inner.domain_check_by_type_oid(domain_oid)
    }

    fn type_oid_for_sql_type(&self, sql_type: SqlType) -> Option<u32> {
        self.inner.type_oid_for_sql_type(sql_type)
    }

    fn language_rows(&self) -> Vec<PgLanguageRow> {
        self.inner.language_rows()
    }

    fn language_row_by_oid(&self, oid: u32) -> Option<PgLanguageRow> {
        self.inner.language_row_by_oid(oid)
    }

    fn language_row_by_name(&self, name: &str) -> Option<PgLanguageRow> {
        self.inner.language_row_by_name(name)
    }

    fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow> {
        self.inner.class_row_by_oid(relation_oid)
    }
}

pub(crate) fn bound_index_relation_from_relcache_entry(
    name: String,
    entry: &crate::backend::utils::cache::relcache::RelCacheEntry,
    catalog: &dyn CatalogLookup,
) -> Option<BoundIndexRelation> {
    bound_index_relation_from_relcache_entry_with_heap(name, entry, catalog, None)
}

pub(crate) fn bound_index_relation_from_relcache_entry_with_heap(
    name: String,
    entry: &crate::backend::utils::cache::relcache::RelCacheEntry,
    catalog: &dyn CatalogLookup,
    heap_relation: Option<&BoundRelation>,
) -> Option<BoundIndexRelation> {
    bound_index_relation_from_relcache_entry_with_heap_and_cache(
        name,
        entry,
        catalog,
        heap_relation,
        None,
    )
}

pub(crate) fn bound_index_relation_from_relcache_entry_with_heap_and_cache(
    name: String,
    entry: &crate::backend::utils::cache::relcache::RelCacheEntry,
    catalog: &dyn CatalogLookup,
    heap_relation: Option<&BoundRelation>,
    index_expr_cache: Option<&RefCell<BTreeMap<u32, PlannerIndexExprCacheEntry>>>,
) -> Option<BoundIndexRelation> {
    let mut index_meta = entry.index.as_ref()?.clone();
    let owned_heap;
    let heap_relation = if let Some(heap) = heap_relation {
        Some(heap)
    } else {
        owned_heap = catalog.relation_by_oid(index_meta.indrelid);
        owned_heap.as_ref()
    };
    let (index_exprs, index_predicate) = if let Some(heap) = heap_relation {
        planner_cached_index_expressions(
            entry.relation_oid,
            &mut index_meta,
            &heap.desc,
            catalog,
            index_expr_cache,
        )
    } else {
        (Vec::new(), None)
    };

    let backing_constraint = catalog
        .constraint_rows_for_index(entry.relation_oid)
        .into_iter()
        .find(|row| {
            matches!(
                row.contype,
                crate::include::catalog::CONSTRAINT_PRIMARY
                    | crate::include::catalog::CONSTRAINT_UNIQUE
            )
        });

    Some(BoundIndexRelation {
        name,
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        relkind: entry.relkind,
        desc: entry.desc.clone(),
        index_meta,
        index_exprs,
        index_predicate,
        constraint_oid: backing_constraint.as_ref().map(|row| row.oid),
        constraint_name: backing_constraint.as_ref().map(|row| row.conname.clone()),
        constraint_deferrable: backing_constraint
            .as_ref()
            .is_some_and(|row| row.condeferrable),
        constraint_initially_deferred: backing_constraint
            .as_ref()
            .is_some_and(|row| row.condeferred),
    })
}

fn planner_cached_index_expressions(
    index_oid: u32,
    index_meta: &mut crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    heap_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
    index_expr_cache: Option<&RefCell<BTreeMap<u32, PlannerIndexExprCacheEntry>>>,
) -> (Vec<Expr>, Option<Expr>) {
    if let Some(cache) = index_expr_cache
        && let Some(cached) = cache.borrow().get(&index_oid).cloned()
    {
        return (cached.exprs, cached.predicate);
    }

    let index_exprs =
        RelationGetIndexExpressions(index_meta, heap_desc, catalog).unwrap_or_default();
    let index_predicate = RelationGetIndexPredicate(index_meta, heap_desc, catalog)
        .ok()
        .flatten();
    if let Some(cache) = index_expr_cache {
        cache.borrow_mut().insert(
            index_oid,
            PlannerIndexExprCacheEntry {
                exprs: index_exprs.clone(),
                predicate: index_predicate.clone(),
            },
        );
    }
    (index_exprs, index_predicate)
}

impl CatalogLookup for Catalog {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation> {
        let relcache = RelCache::from_catalog(self);
        relcache
            .get_by_name(name)
            .map(|entry| bound_relation_from_relcache_entry(&relcache, entry))
    }

    fn lookup_relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        let relcache = RelCache::from_catalog(self);
        relcache
            .get_by_oid(relation_oid)
            .map(|entry| bound_relation_from_relcache_entry(&relcache, entry))
    }

    fn relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        let relcache = RelCache::from_catalog(self);
        relcache
            .get_by_oid(relation_oid)
            .map(|entry| bound_relation_from_relcache_entry(&relcache, entry))
    }

    fn authid_rows(&self) -> Vec<PgAuthIdRow> {
        CatCache::from_catalog(self).authid_rows()
    }

    fn auth_members_rows(&self) -> Vec<PgAuthMembersRow> {
        CatCache::from_catalog(self).auth_members_rows()
    }

    fn event_trigger_rows(&self) -> Vec<PgEventTriggerRow> {
        CatCache::from_catalog(self).event_trigger_rows()
    }

    fn tablespace_rows(&self) -> Vec<PgTablespaceRow> {
        CatCache::from_catalog(self).tablespace_rows()
    }

    fn namespace_row_by_oid(&self, oid: u32) -> Option<PgNamespaceRow> {
        CatCache::from_catalog(self).namespace_by_oid(oid).cloned()
    }

    fn index_relations_for_heap(&self, relation_oid: u32) -> Vec<BoundIndexRelation> {
        self.index_relations_for_heap_with_cache(relation_oid, &RefCell::new(BTreeMap::new()))
    }

    fn index_relations_for_heap_with_cache(
        &self,
        relation_oid: u32,
        index_expr_cache: &RefCell<BTreeMap<u32, PlannerIndexExprCacheEntry>>,
    ) -> Vec<BoundIndexRelation> {
        let relcache = RelCache::from_catalog(self);
        let heap_relation = relcache
            .get_by_oid(relation_oid)
            .map(|entry| bound_relation_from_relcache_entry(&relcache, entry));
        relcache
            .relation_get_index_list(relation_oid)
            .into_iter()
            .filter_map(|index_oid| {
                let entry = relcache.get_by_oid(index_oid)?;
                let name = relcache
                    .relation_name_by_oid(index_oid)
                    .unwrap_or_else(|| index_oid.to_string());
                bound_index_relation_from_relcache_entry_with_heap_and_cache(
                    name,
                    entry,
                    self,
                    heap_relation.as_ref(),
                    Some(index_expr_cache),
                )
            })
            .collect()
    }

    fn index_row_by_oid(&self, index_oid: u32) -> Option<PgIndexRow> {
        CatCache::from_catalog(self)
            .index_rows()
            .into_iter()
            .find(|row| row.indexrelid == index_oid)
    }

    fn proc_rows_by_name(&self, name: &str) -> Vec<PgProcRow> {
        let mut rows = CatCache::from_catalog(self)
            .proc_rows_by_name(name)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        extend_synthetic_range_proc_rows(self, name, &mut rows);
        dedup_proc_rows(&mut rows);
        rows
    }

    fn proc_row_by_oid(&self, oid: u32) -> Option<PgProcRow> {
        CatCache::from_catalog(self)
            .proc_by_oid(oid)
            .cloned()
            .or_else(|| synthetic_range_proc_row_by_oid(oid, &self.type_rows(), &self.range_rows()))
    }

    fn opclass_rows(&self) -> Vec<PgOpclassRow> {
        CatCache::from_catalog(self).opclass_rows()
    }

    fn opfamily_rows(&self) -> Vec<PgOpfamilyRow> {
        CatCache::from_catalog(self).opfamily_rows()
    }

    fn am_rows(&self) -> Vec<PgAmRow> {
        CatCache::from_catalog(self).am_rows()
    }

    fn collation_rows(&self) -> Vec<PgCollationRow> {
        CatCache::from_catalog(self).collation_rows()
    }

    fn operator_by_name_left_right(
        &self,
        name: &str,
        left_type_oid: u32,
        right_type_oid: u32,
    ) -> Option<PgOperatorRow> {
        let normalized = normalize_catalog_lookup_name(name);
        CatCache::from_catalog(self)
            .operator_rows()
            .into_iter()
            .find(|row| {
                row.oprname.eq_ignore_ascii_case(normalized)
                    && row.oprleft == left_type_oid
                    && row.oprright == right_type_oid
            })
    }

    fn operator_by_oid(&self, oid: u32) -> Option<PgOperatorRow> {
        CatCache::from_catalog(self)
            .operator_rows()
            .into_iter()
            .find(|row| row.oid == oid)
    }

    fn operator_rows(&self) -> Vec<PgOperatorRow> {
        CatCache::from_catalog(self).operator_rows()
    }

    fn ts_config_rows(&self) -> Vec<PgTsConfigRow> {
        CatCache::from_catalog(self).ts_config_rows()
    }

    fn ts_parser_rows(&self) -> Vec<PgTsParserRow> {
        CatCache::from_catalog(self).ts_parser_rows()
    }

    fn ts_dict_rows(&self) -> Vec<PgTsDictRow> {
        CatCache::from_catalog(self).ts_dict_rows()
    }

    fn ts_template_rows(&self) -> Vec<PgTsTemplateRow> {
        CatCache::from_catalog(self).ts_template_rows()
    }

    fn ts_config_map_rows(&self) -> Vec<PgTsConfigMapRow> {
        CatCache::from_catalog(self).ts_config_map_rows()
    }

    fn conversion_rows(&self) -> Vec<PgConversionRow> {
        CatCache::from_catalog(self).conversion_rows()
    }

    fn aggregate_by_fnoid(&self, aggfnoid: u32) -> Option<PgAggregateRow> {
        CatCache::from_catalog(self)
            .aggregate_by_fnoid(aggfnoid)
            .cloned()
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        let relcache = RelCache::from_catalog(self);
        let mut rows = builtin_type_rows();
        rows.extend(composite_type_rows_from_relcache(&relcache));
        rows
    }

    fn type_by_oid(&self, oid: u32) -> Option<PgTypeRow> {
        if let Some(row) = builtin_type_row_by_oid(oid) {
            return Some(row);
        }
        if let Some(row) = crate::include::catalog::bootstrap_composite_type_rows()
            .into_iter()
            .find(|row| row.oid == oid)
        {
            return Some(row);
        }
        self.entries().find_map(|(name, entry)| {
            let relname = name.rsplit('.').next().unwrap_or(name);
            if entry.row_type_oid == oid {
                Some(crate::include::catalog::composite_type_row_with_owner(
                    relname,
                    entry.row_type_oid,
                    entry.namespace_oid,
                    entry.owner_oid,
                    entry.relation_oid,
                    entry.array_type_oid,
                ))
            } else if entry.array_type_oid == oid {
                Some(
                    crate::include::catalog::composite_array_type_row_with_owner(
                        relname,
                        entry.array_type_oid,
                        entry.namespace_oid,
                        entry.owner_oid,
                        entry.row_type_oid,
                        entry.relation_oid,
                    ),
                )
            } else {
                None
            }
        })
    }

    fn type_by_name(&self, name: &str) -> Option<PgTypeRow> {
        let normalized = normalize_catalog_lookup_name(name);
        if let Some(row) = builtin_type_row_by_name(normalized) {
            return Some(row);
        }
        if let Some(row) = crate::include::catalog::bootstrap_composite_type_rows()
            .into_iter()
            .find(|row| row.typname.eq_ignore_ascii_case(normalized))
        {
            return Some(row);
        }
        self.entries().find_map(|(entry_name, entry)| {
            let relname = entry_name.rsplit('.').next().unwrap_or(entry_name);
            if entry.row_type_oid != 0 && relname.eq_ignore_ascii_case(normalized) {
                return Some(crate::include::catalog::composite_type_row_with_owner(
                    relname,
                    entry.row_type_oid,
                    entry.namespace_oid,
                    entry.owner_oid,
                    entry.relation_oid,
                    entry.array_type_oid,
                ));
            }
            let array_typname = format!("_{relname}");
            if entry.array_type_oid != 0 && array_typname.eq_ignore_ascii_case(normalized) {
                return Some(
                    crate::include::catalog::composite_array_type_row_with_owner(
                        relname,
                        entry.array_type_oid,
                        entry.namespace_oid,
                        entry.owner_oid,
                        entry.row_type_oid,
                        entry.relation_oid,
                    ),
                );
            }
            None
        })
    }

    fn type_oid_for_sql_type(&self, sql_type: SqlType) -> Option<u32> {
        if let Some(range_type) = range_type_ref_for_sql_type(sql_type) {
            if sql_type.is_array {
                return builtin_type_rows()
                    .into_iter()
                    .find(|row| row.typelem == range_type.type_oid())
                    .map(|row| row.oid);
            }
            return Some(range_type.type_oid());
        }
        if let Some(multirange_type) = multirange_type_ref_for_sql_type(sql_type) {
            if sql_type.is_array {
                return builtin_type_rows()
                    .into_iter()
                    .find(|row| row.typelem == multirange_type.type_oid())
                    .map(|row| row.oid);
            }
            return Some(multirange_type.type_oid());
        }
        if !sql_type.is_array && sql_type.type_oid != 0 {
            return Some(sql_type.type_oid);
        }
        for row in builtin_type_rows() {
            if row.sql_type == sql_type {
                return Some(row.oid);
            }
        }
        let mut fallback = None;
        for row in builtin_type_rows() {
            if row.oid == crate::include::catalog::UNKNOWN_TYPE_OID {
                continue;
            }
            if row.sql_type.kind != sql_type.kind || row.sql_type.is_array != sql_type.is_array {
                continue;
            }
            if row.typrelid == 0 {
                return Some(row.oid);
            }
            fallback.get_or_insert(row.oid);
        }
        for (name, entry) in self.entries() {
            let relname = name.rsplit('.').next().unwrap_or(name);
            let rows = [
                (entry.row_type_oid != 0).then(|| {
                    crate::include::catalog::composite_type_row_with_owner(
                        relname,
                        entry.row_type_oid,
                        entry.namespace_oid,
                        entry.owner_oid,
                        entry.relation_oid,
                        entry.array_type_oid,
                    )
                }),
                (entry.array_type_oid != 0).then(|| {
                    crate::include::catalog::composite_array_type_row_with_owner(
                        relname,
                        entry.array_type_oid,
                        entry.namespace_oid,
                        entry.owner_oid,
                        entry.row_type_oid,
                        entry.relation_oid,
                    )
                }),
            ];
            for row in rows.into_iter().flatten() {
                if row.sql_type == sql_type {
                    return Some(row.oid);
                }
                if row.sql_type.kind != sql_type.kind || row.sql_type.is_array != sql_type.is_array
                {
                    continue;
                }
                if row.typrelid == 0 {
                    return Some(row.oid);
                }
                fallback.get_or_insert(row.oid);
            }
        }
        fallback
    }

    fn language_rows(&self) -> Vec<PgLanguageRow> {
        CatCache::from_catalog(self).language_rows()
    }

    fn language_row_by_oid(&self, oid: u32) -> Option<PgLanguageRow> {
        CatCache::from_catalog(self)
            .language_rows()
            .into_iter()
            .find(|row| row.oid == oid)
    }

    fn language_row_by_name(&self, name: &str) -> Option<PgLanguageRow> {
        let normalized = normalize_catalog_lookup_name(name);
        CatCache::from_catalog(self)
            .language_rows()
            .into_iter()
            .find(|row| row.lanname.eq_ignore_ascii_case(normalized))
    }

    fn rewrite_rows_for_relation(&self, relation_oid: u32) -> Vec<PgRewriteRow> {
        self.rewrite_rows_for_relation(relation_oid).to_vec()
    }

    fn rewrite_rows(&self) -> Vec<PgRewriteRow> {
        self.rewrite_rows().to_vec()
    }

    fn trigger_rows_for_relation(
        &self,
        relation_oid: u32,
    ) -> Vec<crate::include::catalog::PgTriggerRow> {
        crate::backend::utils::cache::catcache::CatCache::from_catalog(self)
            .trigger_rows_for_relation(relation_oid)
    }

    fn trigger_rows(&self) -> Vec<crate::include::catalog::PgTriggerRow> {
        crate::backend::utils::cache::catcache::CatCache::from_catalog(self).trigger_rows()
    }

    fn policy_rows_for_relation(
        &self,
        relation_oid: u32,
    ) -> Vec<crate::include::catalog::PgPolicyRow> {
        crate::backend::utils::cache::catcache::CatCache::from_catalog(self)
            .policy_rows_for_relation(relation_oid)
    }

    fn constraint_rows_for_relation(&self, relation_oid: u32) -> Vec<PgConstraintRow> {
        let catcache = crate::backend::utils::cache::catcache::CatCache::from_catalog(self);
        catcache.constraint_rows_for_relation(relation_oid)
    }

    fn constraint_rows(&self) -> Vec<PgConstraintRow> {
        let catcache = crate::backend::utils::cache::catcache::CatCache::from_catalog(self);
        catcache.constraint_rows()
    }

    fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow> {
        let catcache = crate::backend::utils::cache::catcache::CatCache::from_catalog(self);
        catcache.class_by_oid(relation_oid).cloned()
    }

    fn class_rows(&self) -> Vec<PgClassRow> {
        CatCache::from_catalog(self).class_rows()
    }

    fn attribute_rows(&self) -> Vec<PgAttributeRow> {
        CatCache::from_catalog(self).attribute_rows()
    }

    fn partitioned_table_row(&self, relation_oid: u32) -> Option<PgPartitionedTableRow> {
        let catcache = crate::backend::utils::cache::catcache::CatCache::from_catalog(self);
        catcache.partitioned_table_row(relation_oid).cloned()
    }

    fn partitioned_table_rows(&self) -> Vec<PgPartitionedTableRow> {
        let catcache = crate::backend::utils::cache::catcache::CatCache::from_catalog(self);
        catcache.partitioned_table_rows()
    }

    fn inheritance_parents(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
        self.inherit_rows()
            .iter()
            .filter(|row| row.inhrelid == relation_oid)
            .cloned()
            .collect()
    }

    fn inheritance_children(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
        self.inherit_rows()
            .iter()
            .filter(|row| row.inhparent == relation_oid)
            .cloned()
            .collect()
    }

    fn inheritance_rows(&self) -> Vec<PgInheritsRow> {
        CatCache::from_catalog(self).inherit_rows()
    }

    fn publication_rows(&self) -> Vec<PgPublicationRow> {
        CatCache::from_catalog(self).publication_rows()
    }

    fn publication_rel_rows(&self) -> Vec<PgPublicationRelRow> {
        CatCache::from_catalog(self).publication_rel_rows()
    }

    fn publication_namespace_rows(&self) -> Vec<PgPublicationNamespaceRow> {
        CatCache::from_catalog(self).publication_namespace_rows()
    }

    fn statistic_rows_for_relation(&self, relation_oid: u32) -> Vec<PgStatisticRow> {
        let catcache = crate::backend::utils::cache::catcache::CatCache::from_catalog(self);
        catcache
            .statistic_rows()
            .into_iter()
            .filter(|row| row.starelid == relation_oid)
            .collect()
    }

    fn foreign_data_wrapper_rows(&self) -> Vec<PgForeignDataWrapperRow> {
        CatCache::from_catalog(self).foreign_data_wrapper_rows()
    }

    fn foreign_server_rows(&self) -> Vec<PgForeignServerRow> {
        CatCache::from_catalog(self).foreign_server_rows()
    }

    fn foreign_table_rows(&self) -> Vec<PgForeignTableRow> {
        CatCache::from_catalog(self).foreign_table_rows()
    }

    fn user_mapping_rows(&self) -> Vec<PgUserMappingRow> {
        CatCache::from_catalog(self).user_mapping_rows()
    }

    fn pg_tables_rows(&self) -> Vec<Vec<Value>> {
        let catcache = crate::backend::utils::cache::catcache::CatCache::from_catalog(self);
        build_pg_tables_rows(
            catcache.namespace_rows(),
            catcache.authid_rows(),
            catcache.class_rows(),
        )
    }

    fn pg_views_rows(&self) -> Vec<Vec<Value>> {
        let catcache = crate::backend::utils::cache::catcache::CatCache::from_catalog(self);
        build_pg_views_rows_with_definition_formatter(
            catcache.namespace_rows(),
            catcache.authid_rows(),
            catcache.class_rows(),
            catcache.rewrite_rows(),
            |class, definition| {
                self.relation_by_oid(class.oid)
                    .and_then(|relation| {
                        format_view_definition(class.oid, &relation.desc, self).ok()
                    })
                    .unwrap_or_else(|| definition.to_string())
            },
        )
    }

    fn pg_matviews_rows(&self) -> Vec<Vec<Value>> {
        let catcache = crate::backend::utils::cache::catcache::CatCache::from_catalog(self);
        build_pg_matviews_rows(
            catcache.namespace_rows(),
            catcache.authid_rows(),
            catcache.class_rows(),
            catcache.index_rows(),
            catcache.rewrite_rows(),
        )
    }

    fn pg_indexes_rows(&self) -> Vec<Vec<Value>> {
        let catcache = crate::backend::utils::cache::catcache::CatCache::from_catalog(self);
        build_pg_indexes_rows(
            catcache.namespace_rows(),
            catcache.class_rows(),
            catcache.attribute_rows(),
            catcache.index_rows(),
            catcache.am_rows(),
        )
    }

    fn pg_policies_rows(&self) -> Vec<Vec<Value>> {
        let catcache = crate::backend::utils::cache::catcache::CatCache::from_catalog(self);
        build_pg_policies_rows(
            catcache.namespace_rows(),
            catcache.authid_rows(),
            catcache.class_rows(),
            catcache.policy_rows(),
        )
    }

    fn pg_rules_rows(&self) -> Vec<Vec<Value>> {
        let catcache = crate::backend::utils::cache::catcache::CatCache::from_catalog(self);
        build_pg_rules_rows_with_definition_formatter(
            catcache.namespace_rows(),
            catcache.class_rows(),
            catcache.rewrite_rows(),
            |row, relation_name| {
                format_stored_rule_definition_with_catalog(row, relation_name, self)
            },
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

    fn pg_stat_activity_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn materialize_visible_catalog(&self) -> Option<VisibleCatalog> {
        Some(VisibleCatalog::new(
            RelCache::from_catalog(self),
            Some(CatCache::from_catalog(self)),
        ))
    }
}

impl CatalogLookup for RelCache {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation> {
        self.get_by_name(name)
            .map(|entry| bound_relation_from_relcache_entry(self, entry))
    }

    fn lookup_relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.get_by_oid(relation_oid)
            .map(|entry| bound_relation_from_relcache_entry(self, entry))
    }

    fn constraint_rows_for_relation(&self, relation_oid: u32) -> Vec<PgConstraintRow> {
        let Some((name, entry)) = self
            .entries()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
        else {
            return Vec::new();
        };
        crate::backend::catalog::pg_constraint::derived_pg_constraint_rows(
            relation_oid,
            name.rsplit('.').next().unwrap_or(name),
            entry.namespace_oid,
            &entry.desc,
        )
    }

    fn constraint_rows(&self) -> Vec<PgConstraintRow> {
        self.entries()
            .flat_map(|(name, entry)| {
                crate::backend::catalog::pg_constraint::derived_pg_constraint_rows(
                    entry.relation_oid,
                    name.rsplit('.').next().unwrap_or(name),
                    entry.namespace_oid,
                    &entry.desc,
                )
            })
            .collect()
    }

    fn trigger_rows_for_relation(
        &self,
        _relation_oid: u32,
    ) -> Vec<crate::include::catalog::PgTriggerRow> {
        Vec::new()
    }

    fn relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.get_by_oid(relation_oid).map(|entry| BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            toast: toast_relation_from_cache(self, entry),
            namespace_oid: entry.namespace_oid,
            owner_oid: entry.owner_oid,
            of_type_oid: entry.of_type_oid,
            relpersistence: entry.relpersistence,
            relkind: entry.relkind,
            relispopulated: entry.relispopulated,
            relispartition: entry.relispartition,
            relpartbound: entry.relpartbound.clone(),
            desc: entry.desc.clone(),
            partitioned_table: entry.partitioned_table.clone(),
            partition_spec: entry.partition_spec.clone(),
        })
    }

    fn index_relations_for_heap(&self, relation_oid: u32) -> Vec<BoundIndexRelation> {
        self.index_relations_for_heap_with_cache(relation_oid, &RefCell::new(BTreeMap::new()))
    }

    fn index_relations_for_heap_with_cache(
        &self,
        relation_oid: u32,
        index_expr_cache: &RefCell<BTreeMap<u32, PlannerIndexExprCacheEntry>>,
    ) -> Vec<BoundIndexRelation> {
        let heap_relation = self
            .get_by_oid(relation_oid)
            .map(|entry| bound_relation_from_relcache_entry(self, entry));
        self.relation_get_index_list(relation_oid)
            .into_iter()
            .filter_map(|index_oid| {
                let entry = self.get_by_oid(index_oid)?;
                let name = self
                    .relation_name_by_oid(index_oid)
                    .unwrap_or_else(|| index_oid.to_string());
                bound_index_relation_from_relcache_entry_with_heap_and_cache(
                    name,
                    entry,
                    self,
                    heap_relation.as_ref(),
                    Some(index_expr_cache),
                )
            })
            .collect()
    }

    fn index_row_by_oid(&self, index_oid: u32) -> Option<PgIndexRow> {
        let entry = self.get_by_oid(index_oid)?;
        let index = entry.index.as_ref()?;
        Some(PgIndexRow {
            indexrelid: entry.relation_oid,
            indrelid: index.indrelid,
            indnatts: index.indnatts,
            indnkeyatts: index.indnkeyatts,
            indisunique: index.indisunique,
            indnullsnotdistinct: index.indnullsnotdistinct,
            indisprimary: index.indisprimary,
            indisexclusion: index.indisexclusion,
            indimmediate: index.indimmediate,
            indisclustered: index.indisclustered,
            indisvalid: index.indisvalid,
            indcheckxmin: index.indcheckxmin,
            indisready: index.indisready,
            indislive: index.indislive,
            indisreplident: index.indisreplident,
            indkey: index.indkey.clone(),
            indcollation: index.indcollation.clone(),
            indclass: index.indclass.clone(),
            indoption: index.indoption.clone(),
            indexprs: index.indexprs.clone(),
            indpred: index.indpred.clone(),
        })
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        let mut rows = builtin_type_rows();
        rows.extend(composite_type_rows_from_relcache(self));
        rows
    }

    fn namespace_row_by_oid(&self, oid: u32) -> Option<PgNamespaceRow> {
        bootstrap_pg_namespace_rows()
            .into_iter()
            .find(|row| row.oid == oid)
    }

    fn operator_by_oid(&self, oid: u32) -> Option<PgOperatorRow> {
        bootstrap_pg_operator_rows()
            .into_iter()
            .find(|row| row.oid == oid)
    }

    fn language_rows(&self) -> Vec<PgLanguageRow> {
        bootstrap_pg_language_rows().to_vec()
    }

    fn materialize_visible_catalog(&self) -> Option<VisibleCatalog> {
        Some(VisibleCatalog::new(self.clone(), None))
    }

    fn partitioned_table_row(&self, relation_oid: u32) -> Option<PgPartitionedTableRow> {
        self.get_by_oid(relation_oid)
            .and_then(|entry| entry.partitioned_table.clone())
    }

    fn partitioned_table_rows(&self) -> Vec<PgPartitionedTableRow> {
        self.entries()
            .filter_map(|(_, entry)| entry.partitioned_table.clone())
            .collect()
    }
}

pub(crate) fn normalize_catalog_lookup_name(name: &str) -> &str {
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

fn bound_relation_from_relcache_entry(
    relcache: &RelCache,
    entry: &crate::backend::utils::cache::relcache::RelCacheEntry,
) -> BoundRelation {
    BoundRelation {
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        toast: toast_relation_from_cache(relcache, entry),
        namespace_oid: entry.namespace_oid,
        owner_oid: entry.owner_oid,
        of_type_oid: entry.of_type_oid,
        relpersistence: entry.relpersistence,
        relkind: entry.relkind,
        relispopulated: entry.relispopulated,
        relispartition: entry.relispartition,
        relpartbound: entry.relpartbound.clone(),
        desc: entry.desc.clone(),
        partitioned_table: entry.partitioned_table.clone(),
        partition_spec: entry.partition_spec.clone(),
    }
}

fn composite_type_rows_from_relcache(relcache: &RelCache) -> Vec<PgTypeRow> {
    relcache
        .entries()
        .flat_map(|(name, entry)| {
            let relname = name.rsplit('.').next().unwrap_or(name);
            let mut rows = Vec::new();
            if entry.row_type_oid != 0 {
                rows.push(crate::include::catalog::composite_type_row_with_owner(
                    relname,
                    entry.row_type_oid,
                    entry.namespace_oid,
                    entry.owner_oid,
                    entry.relation_oid,
                    entry.array_type_oid,
                ));
            }
            if entry.array_type_oid != 0 {
                rows.push(
                    crate::include::catalog::composite_array_type_row_with_owner(
                        relname,
                        entry.array_type_oid,
                        entry.namespace_oid,
                        entry.owner_oid,
                        entry.row_type_oid,
                        entry.relation_oid,
                    ),
                );
            }
            rows
        })
        .collect()
}

#[derive(Default)]
pub(crate) struct LiteralDefaultCatalog;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainLookup {
    pub oid: u32,
    pub array_oid: u32,
    pub name: String,
    pub sql_type: SqlType,
    pub default: Option<String>,
    pub check: Option<String>,
    pub not_null: bool,
    pub constraints: Vec<DomainConstraintLookup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainConstraintLookup {
    pub name: String,
    pub kind: DomainConstraintLookupKind,
    pub expr: Option<String>,
    pub validated: bool,
    pub enforced: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomainConstraintLookupKind {
    Check,
    NotNull,
}

impl CatalogLookup for LiteralDefaultCatalog {
    fn lookup_any_relation(&self, _name: &str) -> Option<BoundRelation> {
        None
    }

    fn language_rows(&self) -> Vec<PgLanguageRow> {
        bootstrap_pg_language_rows().to_vec()
    }

    fn materialize_visible_catalog(&self) -> Option<VisibleCatalog> {
        Some(VisibleCatalog::new(RelCache::default(), None))
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
            let target = raw_type_name_hint(ty);
            if matches!(
                target.kind,
                SqlTypeKind::Date
                    | SqlTypeKind::Time
                    | SqlTypeKind::TimeTz
                    | SqlTypeKind::Timestamp
                    | SqlTypeKind::TimestampTz
            ) {
                return None;
            }
            cast_value(inner, target).ok()
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

fn group_by_target_ordinal_expr(
    expr: &SqlExpr,
    targets: &[SelectItem],
    input_scope: &BoundScope,
) -> Result<Option<SqlExpr>, ParseError> {
    let SqlExpr::IntegerLiteral(value) = expr else {
        return Ok(None);
    };
    let Ok(ordinal) = value.parse::<usize>() else {
        return Ok(None);
    };
    if ordinal == 0 || ordinal > targets.len() {
        return Err(ParseError::UnexpectedToken {
            expected: "GROUP BY position in select list",
            actual: format!("GROUP BY position {value} is not in select list"),
        });
    }
    let target_expr = &targets[ordinal - 1].expr;
    if let SqlExpr::Column(name) = target_expr {
        if name == "*" {
            return Ok(first_visible_scope_column_expr(input_scope, None));
        }
        if let Some(relation_name) = name.strip_suffix(".*") {
            return Ok(first_visible_scope_column_expr(
                input_scope,
                Some(relation_name),
            ));
        }
    }
    Ok(Some(target_expr.clone()))
}

fn first_visible_scope_column_expr(
    input_scope: &BoundScope,
    relation_name: Option<&str>,
) -> Option<SqlExpr> {
    input_scope
        .columns
        .iter()
        .find(|column| {
            !column.hidden
                && relation_name.is_none_or(|relation_name| {
                    column
                        .relation_names
                        .iter()
                        .any(|visible| visible.eq_ignore_ascii_case(relation_name))
                })
        })
        .map(|column| SqlExpr::Column(column.output_name.clone()))
}

fn group_by_target_alias_expr(
    expr: &SqlExpr,
    targets: &[SelectItem],
    input_scope: &BoundScope,
) -> Option<SqlExpr> {
    let SqlExpr::Column(name) = expr else {
        return None;
    };
    if name.contains('.') {
        return None;
    }
    match resolve_column(input_scope, name) {
        Ok(_) => return None,
        Err(ParseError::UnknownColumn(_)) => {}
        Err(_) => return None,
    }
    let mut matches = targets
        .iter()
        .filter(|target| target.output_name.eq_ignore_ascii_case(name));
    let first = matches.next()?;
    matches.next().is_none().then(|| first.expr.clone())
}

fn grouped_select_item_output_name(item: &SelectItem) -> String {
    grouped_expr_output_name(&item.output_name, &item.expr)
}

fn grouped_select_item_exprs(
    item: &SelectItem,
    input_scope: &BoundScope,
) -> Result<Vec<(String, SqlExpr)>, ParseError> {
    if let SqlExpr::Column(name) = &item.expr {
        if name == "*" {
            return expand_grouped_star_exprs(input_scope, None);
        }
        if let Some(relation_name) = name.strip_suffix(".*") {
            return expand_grouped_star_exprs(input_scope, Some(relation_name));
        }
    }
    Ok(vec![(
        grouped_select_item_output_name(item),
        item.expr.clone(),
    )])
}

fn expand_grouped_star_exprs(
    input_scope: &BoundScope,
    relation_name: Option<&str>,
) -> Result<Vec<(String, SqlExpr)>, ParseError> {
    let expanded = input_scope
        .columns
        .iter()
        .filter(|column| {
            !column.hidden
                && relation_name.is_none_or(|relation_name| {
                    column
                        .relation_names
                        .iter()
                        .any(|visible| visible.eq_ignore_ascii_case(relation_name))
                })
        })
        .map(|column| {
            (
                column.output_name.clone(),
                SqlExpr::Column(column.output_name.clone()),
            )
        })
        .collect::<Vec<_>>();
    if expanded.is_empty() {
        return Err(ParseError::UnknownColumn(
            relation_name
                .map(|name| format!("{name}.*"))
                .unwrap_or_else(|| "*".into()),
        ));
    }
    Ok(expanded)
}

fn grouped_expr_output_name(output_name: &str, expr: &SqlExpr) -> String {
    if output_name == "?column?"
        && let SqlExpr::ScalarSubquery(select) = expr
        && let Some(target) = select.targets.first()
    {
        return grouped_expr_output_name(&target.output_name, &target.expr);
    }
    output_name.to_string()
}

#[derive(Debug, Clone, Default)]
struct ExpandedGroupBy {
    group_by: Vec<SqlExpr>,
    grouping_sets: Vec<Vec<SqlExpr>>,
    has_explicit_grouping_sets: bool,
}

fn normalize_group_by_expr(
    expr: &SqlExpr,
    stmt: &SelectStatement,
    input_scope: &BoundScope,
) -> Result<SqlExpr, ParseError> {
    group_by_target_ordinal_expr(expr, &stmt.targets, input_scope).map(|resolved| {
        resolved
            .or_else(|| group_by_target_alias_expr(expr, &stmt.targets, input_scope))
            .unwrap_or_else(|| expr.clone())
    })
}

fn normalize_group_by_item(
    item: &GroupByItem,
    stmt: &SelectStatement,
    input_scope: &BoundScope,
) -> Result<GroupByItem, ParseError> {
    Ok(match item {
        GroupByItem::Expr(SqlExpr::Row(exprs)) => GroupByItem::List(
            exprs
                .iter()
                .map(|expr| normalize_group_by_expr(expr, stmt, input_scope))
                .collect::<Result<_, _>>()?,
        ),
        GroupByItem::Expr(expr) => {
            GroupByItem::Expr(normalize_group_by_expr(expr, stmt, input_scope)?)
        }
        GroupByItem::Empty => GroupByItem::Empty,
        GroupByItem::List(exprs) => GroupByItem::List(
            exprs
                .iter()
                .map(|expr| normalize_group_by_expr(expr, stmt, input_scope))
                .collect::<Result<_, _>>()?,
        ),
        GroupByItem::Rollup(items) => GroupByItem::Rollup(
            items
                .iter()
                .map(|item| normalize_group_by_item(item, stmt, input_scope))
                .collect::<Result<_, _>>()?,
        ),
        GroupByItem::Cube(items) => GroupByItem::Cube(
            items
                .iter()
                .map(|item| normalize_group_by_item(item, stmt, input_scope))
                .collect::<Result<_, _>>()?,
        ),
        GroupByItem::Sets(items) => GroupByItem::Sets(
            items
                .iter()
                .map(|item| normalize_group_by_item(item, stmt, input_scope))
                .collect::<Result<_, _>>()?,
        ),
    })
}

fn expand_group_by_items(
    stmt: &SelectStatement,
    input_scope: &BoundScope,
) -> Result<ExpandedGroupBy, ParseError> {
    if stmt.group_by.is_empty() {
        return Ok(ExpandedGroupBy::default());
    }

    let normalized = stmt
        .group_by
        .iter()
        .map(|item| normalize_group_by_item(item, stmt, input_scope))
        .collect::<Result<Vec<_>, _>>()?;
    let has_explicit_grouping_sets = normalized
        .iter()
        .any(group_by_item_has_explicit_grouping_sets);
    let mut grouping_sets = vec![Vec::new()];
    for item in &normalized {
        grouping_sets = concat_grouping_sets(grouping_sets, expand_group_by_item(item));
    }
    if stmt.group_by_distinct {
        dedupe_grouping_sets(&mut grouping_sets);
    }

    let mut group_by = Vec::new();
    for set in &grouping_sets {
        let mut set_prefix = Vec::new();
        for expr in set {
            set_prefix.push(expr.clone());
            let needed_occurrences = expr_occurrence_count(&set_prefix, expr);
            let existing_occurrences = expr_occurrence_count(&group_by, expr);
            if existing_occurrences < needed_occurrences {
                group_by.push(expr.clone());
            }
        }
    }

    Ok(ExpandedGroupBy {
        group_by,
        grouping_sets: has_explicit_grouping_sets
            .then_some(grouping_sets)
            .unwrap_or_default(),
        has_explicit_grouping_sets,
    })
}

fn group_by_item_has_explicit_grouping_sets(item: &GroupByItem) -> bool {
    match item {
        GroupByItem::Expr(_) => false,
        GroupByItem::Empty | GroupByItem::List(_) => true,
        GroupByItem::Rollup(_) | GroupByItem::Cube(_) | GroupByItem::Sets(_) => true,
    }
}

fn expand_group_by_item(item: &GroupByItem) -> Vec<Vec<SqlExpr>> {
    match item {
        GroupByItem::Expr(expr) => vec![vec![expr.clone()]],
        GroupByItem::Empty => vec![Vec::new()],
        GroupByItem::List(exprs) => vec![exprs.clone()],
        GroupByItem::Rollup(items) => expand_rollup_grouping_sets(items),
        GroupByItem::Cube(items) => expand_cube_grouping_sets(items),
        GroupByItem::Sets(items) => items.iter().flat_map(expand_group_by_item).collect(),
    }
}

fn grouping_item_exprs(item: &GroupByItem) -> Vec<SqlExpr> {
    let mut exprs = Vec::new();
    for set in expand_group_by_item(item) {
        for expr in set {
            if !exprs.contains(&expr) {
                exprs.push(expr);
            }
        }
    }
    exprs
}

fn expand_rollup_grouping_sets(items: &[GroupByItem]) -> Vec<Vec<SqlExpr>> {
    let item_exprs = items.iter().map(grouping_item_exprs).collect::<Vec<_>>();
    let mut sets = Vec::with_capacity(item_exprs.len() + 1);
    for len in (0..=item_exprs.len()).rev() {
        let mut set = Vec::new();
        for exprs in item_exprs.iter().take(len) {
            append_grouping_set_exprs(&mut set, exprs);
        }
        sets.push(set);
    }
    sets
}

fn expand_cube_grouping_sets(items: &[GroupByItem]) -> Vec<Vec<SqlExpr>> {
    let item_exprs = items.iter().map(grouping_item_exprs).collect::<Vec<_>>();
    let Some(count) = 1usize.checked_shl(item_exprs.len() as u32) else {
        return vec![item_exprs.into_iter().flatten().collect()];
    };
    let mut sets = Vec::with_capacity(count);
    for mask in (0..count).rev() {
        let mut set = Vec::new();
        for (index, exprs) in item_exprs.iter().enumerate() {
            let bit = 1usize << (item_exprs.len() - index - 1);
            if mask & bit != 0 {
                append_grouping_set_exprs(&mut set, exprs);
            }
        }
        sets.push(set);
    }
    sets
}

fn concat_grouping_sets(left: Vec<Vec<SqlExpr>>, right: Vec<Vec<SqlExpr>>) -> Vec<Vec<SqlExpr>> {
    let mut result = Vec::new();
    for left_set in &left {
        for right_set in &right {
            let mut set = left_set.clone();
            set.extend(right_set.iter().cloned());
            result.push(set);
        }
    }
    result
}

fn expr_occurrence_count(exprs: &[SqlExpr], needle: &SqlExpr) -> usize {
    exprs.iter().filter(|expr| *expr == needle).count()
}

fn dedupe_grouping_sets(grouping_sets: &mut Vec<Vec<SqlExpr>>) {
    let mut deduped = Vec::new();
    for set in grouping_sets.drain(..) {
        let set = set.into_iter().fold(Vec::new(), |mut normalized, expr| {
            if !normalized.contains(&expr) {
                normalized.push(expr);
            }
            normalized
        });
        if !deduped.contains(&set) {
            deduped.push(set);
        }
    }
    *grouping_sets = deduped;
}

fn take_group_by_expr_match(
    expr: &SqlExpr,
    group_by_exprs: &[SqlExpr],
    used_group_by_indexes: &mut Vec<usize>,
) -> Option<usize> {
    let index = group_by_exprs
        .iter()
        .enumerate()
        .find(|(index, group_expr)| group_expr == &expr && !used_group_by_indexes.contains(index))
        .map(|(index, _)| index)?;
    used_group_by_indexes.push(index);
    Some(index)
}

fn append_grouping_set_exprs(set: &mut Vec<SqlExpr>, exprs: &[SqlExpr]) {
    for expr in exprs {
        if !set.contains(expr) {
            set.push(expr.clone());
        }
    }
}

fn bind_grouping_sets(
    grouping_sets: &[Vec<SqlExpr>],
    group_keys: &[Expr],
    group_key_refs: &[usize],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    visible_ctes: &[BoundCte],
) -> Result<Vec<Vec<usize>>, ParseError> {
    grouping_sets
        .iter()
        .map(|set| {
            let mut bound_set = Vec::new();
            let mut used_group_key_indexes = Vec::new();
            for expr in set {
                let bound = bind_expr_with_outer_and_ctes(
                    expr,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    visible_ctes,
                )?;
                let Some(group_index) = group_keys
                    .iter()
                    .enumerate()
                    .find(|(group_index, group_key)| {
                        grouping_key_inner_expr(group_key) == &bound
                            && !used_group_key_indexes.contains(group_index)
                    })
                    .map(|(group_index, _)| group_index)
                else {
                    return Err(ParseError::UnexpectedToken {
                        expected: "grouping set expression in GROUP BY",
                        actual: sql_expr_name(expr),
                    });
                };
                used_group_key_indexes.push(group_index);
                bound_set.push(
                    group_key_refs
                        .get(group_index)
                        .copied()
                        .unwrap_or(group_index + 1),
                );
            }
            Ok(bound_set)
        })
        .collect()
}

fn grouping_key_inner_expr(expr: &Expr) -> &Expr {
    match expr {
        Expr::GroupingKey(grouping_key) => &grouping_key.expr,
        _ => expr,
    }
}

fn grouping_type_hashable(sql_type: SqlType) -> bool {
    if sql_type.is_array {
        return grouping_type_hashable(sql_type.element_type());
    }
    !matches!(
        sql_type.kind,
        SqlTypeKind::VarBit
            | SqlTypeKind::Bit
            | SqlTypeKind::Record
            | SqlTypeKind::Composite
            | SqlTypeKind::Json
            | SqlTypeKind::JsonPath
            | SqlTypeKind::Xml
    )
}

fn grouping_type_sortable(sql_type: SqlType) -> bool {
    if sql_type.is_array {
        return grouping_type_sortable(sql_type.element_type());
    }
    !matches!(
        sql_type.kind,
        SqlTypeKind::Xid
            | SqlTypeKind::Json
            | SqlTypeKind::Jsonb
            | SqlTypeKind::JsonPath
            | SqlTypeKind::Xml
            | SqlTypeKind::TsVector
            | SqlTypeKind::TsQuery
    )
}

fn could_not_implement_group_by_error() -> ParseError {
    ParseError::DetailedError {
        message: "could not implement GROUP BY".into(),
        detail: Some(
            "Some of the datatypes only support hashing, while others only support sorting.".into(),
        ),
        hint: None,
        sqlstate: "42803",
    }
}

fn grouping_ref_type(group_keys: &[Expr], group_key_refs: &[usize], ref_id: usize) -> SqlType {
    group_key_refs
        .iter()
        .position(|candidate| *candidate == ref_id)
        .and_then(|index| group_keys.get(index))
        .and_then(|expr| expr_sql_type_hint(grouping_key_inner_expr(expr)))
        .unwrap_or_else(|| SqlType::new(SqlTypeKind::Text))
}

fn validate_grouping_set_capabilities(
    grouping_sets: &[Vec<usize>],
    group_keys: &[Expr],
    group_key_refs: &[usize],
    aggs: &[CollectedAggregate],
) -> Result<(), ParseError> {
    if grouping_sets.is_empty() {
        return Ok(());
    }

    let sorted_grouping_required = aggs
        .iter()
        .any(|agg| agg.distinct || !agg.order_by.is_empty());
    for grouping_set in grouping_sets {
        let key_types = grouping_set
            .iter()
            .map(|ref_id| grouping_ref_type(group_keys, group_key_refs, *ref_id))
            .collect::<Vec<_>>();
        let all_sortable = key_types.iter().copied().all(grouping_type_sortable);
        let all_hashable = key_types.iter().copied().all(grouping_type_hashable);
        if (!all_sortable && !all_hashable) || (sorted_grouping_required && !all_sortable) {
            return Err(could_not_implement_group_by_error());
        }
    }
    Ok(())
}

fn expand_group_by_with_primary_key_dependencies(
    group_by_exprs: &mut Vec<SqlExpr>,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
) -> Vec<u32> {
    let mut constraint_deps = Vec::new();
    for relation in &scope.relations {
        let Some(relation_name) = relation.relation_names.first() else {
            continue;
        };
        let Some(bound_relation) = catalog.lookup_any_relation(relation_name) else {
            continue;
        };
        let Some(primary) = catalog
            .constraint_rows_for_relation(bound_relation.relation_oid)
            .into_iter()
            .find(|row| row.contype == crate::include::catalog::CONSTRAINT_PRIMARY)
        else {
            continue;
        };
        let Some(conkey) = primary.conkey.as_deref() else {
            continue;
        };
        let pk_scope_indexes = conkey
            .iter()
            .filter_map(|attnum| {
                bound_relation
                    .desc
                    .columns
                    .get((*attnum).saturating_sub(1) as usize)
                    .and_then(|column| {
                        scope_index_for_relation_column(scope, relation_name, &column.name)
                    })
            })
            .collect::<Vec<_>>();
        if pk_scope_indexes.len() != conkey.len()
            || !pk_scope_indexes
                .iter()
                .all(|index| group_by_contains_scope_index(group_by_exprs, scope, *index))
        {
            continue;
        }
        for column in &bound_relation.desc.columns {
            let Some(scope_index) =
                scope_index_for_relation_column(scope, relation_name, &column.name)
            else {
                continue;
            };
            if !group_by_contains_scope_index(group_by_exprs, scope, scope_index) {
                group_by_exprs.push(SqlExpr::Column(format!("{relation_name}.{}", column.name)));
            }
        }
        constraint_deps.push(primary.oid);
    }
    constraint_deps.sort_unstable();
    constraint_deps.dedup();
    constraint_deps
}

fn scope_index_for_relation_column(
    scope: &BoundScope,
    relation_name: &str,
    column_name: &str,
) -> Option<usize> {
    scope.columns.iter().position(|column| {
        !column.hidden
            && column.output_name.eq_ignore_ascii_case(column_name)
            && column
                .relation_names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(relation_name))
    })
}

fn group_by_contains_scope_index(
    group_by_exprs: &[SqlExpr],
    scope: &BoundScope,
    scope_index: usize,
) -> bool {
    group_by_exprs.iter().any(|expr| {
        matches!(expr, SqlExpr::Column(name) if resolve_column(scope, name).ok() == Some(scope_index))
    })
}

pub(crate) fn raw_type_name_hint(raw: &RawTypeName) -> SqlType {
    match raw {
        RawTypeName::Builtin(ty) => *ty,
        RawTypeName::Serial(SerialKind::Small) => SqlType::new(SqlTypeKind::Int2),
        RawTypeName::Serial(SerialKind::Regular) => SqlType::new(SqlTypeKind::Int4),
        RawTypeName::Serial(SerialKind::Big) => SqlType::new(SqlTypeKind::Int8),
        RawTypeName::Named { array_bounds, .. } => {
            let (base_name, _) = split_named_type_typmod(raw_type_name_name(raw));
            if *array_bounds == 0 && base_name.eq_ignore_ascii_case("unknown") {
                return SqlType::new(SqlTypeKind::Text);
            }
            let mut ty = builtin_named_type_alias(base_name)
                .unwrap_or_else(|| SqlType::new(SqlTypeKind::Composite));
            for _ in 0..*array_bounds {
                ty = SqlType::array_of(ty);
            }
            ty
        }
        RawTypeName::Record => SqlType::record(RECORD_TYPE_OID),
    }
}

pub(crate) fn raw_type_name_is_unknown(raw: &RawTypeName) -> bool {
    match raw {
        RawTypeName::Named { name, array_bounds } if *array_bounds == 0 => {
            let (base_name, typmod_args) = split_named_type_typmod(name);
            typmod_args.is_none() && base_name.eq_ignore_ascii_case("unknown")
        }
        _ => false,
    }
}

pub(crate) fn resolve_raw_type_name(
    raw: &RawTypeName,
    catalog: &dyn CatalogLookup,
) -> Result<SqlType, ParseError> {
    match raw {
        RawTypeName::Builtin(ty) => Ok(*ty),
        RawTypeName::Serial(kind) => Err(ParseError::FeatureNotSupported(format!(
            "{} is only allowed in CREATE TABLE / ALTER TABLE ADD COLUMN",
            match kind {
                SerialKind::Small => "smallserial",
                SerialKind::Regular => "serial",
                SerialKind::Big => "bigserial",
            }
        ))),
        RawTypeName::Record => Ok(SqlType::record(RECORD_TYPE_OID)),
        RawTypeName::Named { name, array_bounds }
            if name.eq_ignore_ascii_case("unknown")
                || name.eq_ignore_ascii_case("pg_catalog.unknown") =>
        {
            if *array_bounds == 0 {
                Ok(SqlType::new(SqlTypeKind::Text))
            } else {
                Ok(SqlType::array_of(SqlType::new(SqlTypeKind::Text)))
            }
        }
        RawTypeName::Named { name, array_bounds } => {
            if let Some(mut ty) = resolve_percent_type_name(name, catalog)? {
                for _ in 0..*array_bounds {
                    ty = SqlType::array_of(ty);
                }
                return Ok(ty);
            }
            let (base_name, typmod_args) = split_named_type_typmod(name);
            let mut ty = if let Some(alias) = builtin_named_type_alias(base_name) {
                alias
            } else {
                catalog
                    .type_by_name(base_name)
                    .map(|row| row.sql_type)
                    .ok_or_else(|| ParseError::UnsupportedType(base_name.to_string()))?
            };
            if let Some(args) = typmod_args {
                ty = ty.with_typmod(resolve_named_type_typmod(base_name, args)?);
            }
            for _ in 0..*array_bounds {
                ty = array_of_resolved_type(ty, catalog);
            }
            Ok(ty)
        }
    }
}

fn resolve_percent_type_name(
    name: &str,
    catalog: &dyn CatalogLookup,
) -> Result<Option<SqlType>, ParseError> {
    let trimmed = name.trim();
    let lowered = trimmed.to_ascii_lowercase();
    if let Some(prefix) = lowered.strip_suffix("%type") {
        let original_prefix = &trimmed[..prefix.len()];
        let Some((relation_name, column_name)) = original_prefix.trim().rsplit_once('.') else {
            return Err(ParseError::UnexpectedToken {
                expected: "%TYPE reference in relation.column form",
                actual: name.into(),
            });
        };
        let relation = catalog
            .lookup_any_relation(relation_name.trim())
            .ok_or_else(|| ParseError::UnsupportedType(relation_name.trim().into()))?;
        let column = relation
            .desc
            .columns
            .iter()
            .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name.trim()))
            .ok_or_else(|| ParseError::UnknownColumn(original_prefix.trim().into()))?;
        return Ok(Some(column.sql_type));
    }
    if let Some(prefix) = lowered.strip_suffix("%rowtype") {
        let relation_name = &trimmed[..prefix.len()];
        let relation = catalog
            .lookup_any_relation(relation_name.trim())
            .ok_or_else(|| ParseError::UnsupportedType(relation_name.trim().into()))?;
        return Ok(Some(relation_row_type(&relation, catalog)));
    }
    Ok(None)
}

fn relation_row_type(relation: &BoundRelation, catalog: &dyn CatalogLookup) -> SqlType {
    catalog
        .type_rows()
        .into_iter()
        .find(|row| row.typrelid == relation.relation_oid)
        .map(|row| SqlType::named_composite(row.oid, relation.relation_oid))
        .unwrap_or_else(|| SqlType::record(RECORD_TYPE_OID))
}

pub(super) fn domain_lookup_sql_type(domain: &DomainLookup) -> SqlType {
    let identity_arg =
        if domain.sql_type.type_oid != 0 && matches!(domain.sql_type.kind, SqlTypeKind::Enum) {
            domain.sql_type.type_oid
        } else {
            domain.sql_type.typrelid
        };
    domain.sql_type.with_identity(domain.oid, identity_arg)
}

pub(super) fn is_array_of_domain_over_array_type(
    sql_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> bool {
    if !sql_type.is_array || sql_type.typrelid == 0 {
        return false;
    }
    catalog
        .domain_by_type_oid(sql_type.type_oid)
        .is_some_and(|domain| {
            domain.sql_type.is_array
                && sql_type.typrelid == domain.array_oid
                && catalog
                    .domain_by_type_oid(domain.sql_type.type_oid)
                    .is_some()
        })
}

pub(super) fn domain_over_array_element_type(
    sql_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Option<SqlType> {
    if !sql_type.is_array || sql_type.typrelid == 0 {
        return None;
    }
    let domain = catalog.domain_by_type_oid(sql_type.type_oid)?;
    (domain.sql_type.is_array
        && sql_type.typrelid == domain.array_oid
        && catalog
            .domain_by_type_oid(domain.sql_type.type_oid)
            .is_some())
    .then(|| domain_lookup_sql_type(&domain))
}

pub(super) fn array_of_resolved_type(sql_type: SqlType, catalog: &dyn CatalogLookup) -> SqlType {
    if sql_type.is_array
        && let Some(domain) = catalog.domain_by_type_oid(sql_type.type_oid)
        && domain.sql_type.is_array
        && catalog
            .domain_by_type_oid(domain.sql_type.type_oid)
            .is_some()
    {
        // :HACK: SqlType only has one is_array bit, so an array of a domain
        // whose base type is itself an array is otherwise indistinguishable
        // from the scalar domain. Use typrelid as a local marker for the
        // domain's array type OID until array dimensions are represented
        // explicitly in type metadata.
        return SqlType::array_of(sql_type).with_identity(sql_type.type_oid, domain.array_oid);
    }
    SqlType::array_of(sql_type)
}

pub(crate) fn split_named_type_typmod(name: &str) -> (&str, Option<Vec<i32>>) {
    let trimmed = name.trim();
    let Some(open) = trimmed.find('(') else {
        return (trimmed, None);
    };
    if !trimmed.ends_with(')') {
        return (trimmed, None);
    }
    let base = trimmed[..open].trim();
    let args = trimmed[open + 1..trimmed.len() - 1]
        .split(',')
        .filter_map(|item| item.trim().parse::<i32>().ok())
        .collect::<Vec<_>>();
    (base, Some(args))
}

fn resolve_named_type_typmod(type_name: &str, args: Vec<i32>) -> Result<i32, ParseError> {
    match args.as_slice() {
        [precision] => Ok(SqlType::VARHDRSZ + (*precision << 16)),
        [precision, scale] => Ok(SqlType::VARHDRSZ + ((*precision << 16) | (*scale & 0xffff))),
        _ => Err(ParseError::DetailedError {
            message: "invalid NUMERIC type modifier".into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        }),
    }
    .map_err(|err| match err {
        ParseError::DetailedError { .. } => err,
        _ => ParseError::UnsupportedType(type_name.to_string()),
    })
}

fn builtin_named_type_alias(name: &str) -> Option<SqlType> {
    if let Some((schema, base)) = name.rsplit_once('.')
        && schema.eq_ignore_ascii_case("pg_catalog")
    {
        return builtin_named_type_alias(base);
    }
    if name.eq_ignore_ascii_case("float") {
        Some(SqlType::new(SqlTypeKind::Float8))
    } else if name.eq_ignore_ascii_case("xid8") {
        Some(
            SqlType::new(SqlTypeKind::Int8)
                .with_identity(crate::include::catalog::XID8_TYPE_OID, 0),
        )
    } else if name.eq_ignore_ascii_case("any") {
        Some(
            SqlType::new(SqlTypeKind::AnyElement).with_identity(crate::include::catalog::ANYOID, 0),
        )
    } else if name.eq_ignore_ascii_case("anyenum") {
        Some(
            SqlType::new(SqlTypeKind::AnyElement)
                .with_identity(crate::include::catalog::ANYENUMOID, 0),
        )
    } else if name.eq_ignore_ascii_case("anynonarray") {
        Some(
            SqlType::new(SqlTypeKind::AnyElement)
                .with_identity(crate::include::catalog::ANYNONARRAYOID, 0),
        )
    } else if name.eq_ignore_ascii_case("anycompatiblenonarray") {
        Some(
            SqlType::new(SqlTypeKind::AnyCompatible)
                .with_identity(crate::include::catalog::ANYCOMPATIBLENONARRAYOID, 0),
        )
    } else if name.eq_ignore_ascii_case("bpchar") {
        Some(SqlType::new(SqlTypeKind::Char))
    } else if name.eq_ignore_ascii_case("character") {
        Some(SqlType::new(SqlTypeKind::Char))
    } else if name.eq_ignore_ascii_case("index_am_handler") {
        Some(
            SqlType::new(SqlTypeKind::FdwHandler)
                .with_identity(crate::include::catalog::INDEX_AM_HANDLER_TYPE_OID, 0),
        )
    } else if name.eq_ignore_ascii_case("table_am_handler") {
        Some(
            SqlType::new(SqlTypeKind::FdwHandler)
                .with_identity(crate::include::catalog::TABLE_AM_HANDLER_TYPE_OID, 0),
        )
    } else if name.eq_ignore_ascii_case("regtype") {
        Some(SqlType::new(SqlTypeKind::RegType))
    } else if name.eq_ignore_ascii_case("regproc") {
        Some(SqlType::new(SqlTypeKind::RegProc))
    } else if name.eq_ignore_ascii_case("regoper") {
        Some(SqlType::new(SqlTypeKind::RegOper))
    } else if name.eq_ignore_ascii_case("regoperator") {
        Some(SqlType::new(SqlTypeKind::RegOperator))
    } else if name.eq_ignore_ascii_case("regnamespace") {
        Some(SqlType::new(SqlTypeKind::RegNamespace))
    } else if name.eq_ignore_ascii_case("regcollation") {
        Some(SqlType::new(SqlTypeKind::RegCollation))
    } else if name.eq_ignore_ascii_case("cstring") {
        Some(SqlType::new(SqlTypeKind::Cstring))
    } else if name.eq_ignore_ascii_case("unknown") {
        Some(
            SqlType::new(SqlTypeKind::Text)
                .with_identity(crate::include::catalog::UNKNOWN_TYPE_OID, 0),
        )
    } else {
        None
    }
}

fn raw_type_name_name(raw: &RawTypeName) -> &str {
    match raw {
        RawTypeName::Named { name, .. } => name,
        _ => unreachable!("raw_type_name_name only valid for named types"),
    }
}

pub fn derive_literal_default_value(sql: &str, target: SqlType) -> Result<Value, ParseError> {
    let parsed = crate::backend::parser::parse_expr(sql)?;
    let value = if let Some(value) = literal_sql_expr_value(&parsed) {
        value
    } else {
        let catalog = LiteralDefaultCatalog;
        let (bound, from_type) = bind_scalar_expr_in_scope(&parsed, &[], &catalog)?;
        if matches!(&bound, Expr::Var(var) if var.varlevelsup > 0) {
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
    let desc = RelationDesc {
        columns: columns
            .iter()
            .map(|(name, sql_type)| column_desc(name.clone(), *sql_type, true))
            .collect(),
    };
    let scope = scope_for_relation(None, &desc);
    let empty_outer = Vec::new();
    let bound = bind_expr_with_outer(expr, &scope, catalog, &empty_outer, None)?;
    let sql_type = infer_sql_expr_type(expr, &scope, catalog, &empty_outer, None);
    Ok((bound, sql_type))
}

pub(crate) fn bind_scalar_expr_in_named_relation_scope(
    expr: &SqlExpr,
    relation_scopes: &[(&str, &RelationDesc)],
    columns: &[(String, SqlType)],
    catalog: &dyn CatalogLookup,
) -> Result<(Expr, SqlType), ParseError> {
    let scope = named_relation_scope(relation_scopes, columns);
    let empty_outer = Vec::new();
    let bound = bind_expr_with_outer(expr, &scope, catalog, &empty_outer, None)?;
    let sql_type = infer_sql_expr_type(expr, &scope, catalog, &empty_outer, None);
    Ok((bound, sql_type))
}

pub(crate) fn bind_policy_expr_in_named_relation_scope(
    expr: &SqlExpr,
    relation_scopes: &[(&str, &RelationDesc)],
    columns: &[(String, SqlType)],
    catalog: &dyn CatalogLookup,
) -> Result<(Expr, SqlType), ParseError> {
    let scope = named_relation_scope(relation_scopes, columns);
    let empty_outer = Vec::new();
    analyze_expr_aggregates_in_clause(
        expr,
        AggregateClauseKind::Policy,
        &scope,
        catalog,
        &empty_outer,
        None,
        &[],
        &[],
    )?;
    let bound = bind_expr_with_outer(expr, &scope, catalog, &empty_outer, None)?;
    let sql_type = infer_sql_expr_type(expr, &scope, catalog, &empty_outer, None);
    Ok((bound, sql_type))
}

fn named_relation_scope(
    relation_scopes: &[(&str, &RelationDesc)],
    columns: &[(String, SqlType)],
) -> scope::BoundScope {
    let single_relation_system_varno = (relation_scopes.len() == 1).then_some(1);
    let mut desc_columns = columns
        .iter()
        .map(|(name, sql_type)| column_desc(name.clone(), *sql_type, true))
        .collect::<Vec<_>>();
    let mut scope_columns = columns
        .iter()
        .map(|(name, _)| scope::ScopeColumn {
            output_name: name.clone(),
            hidden: false,
            qualified_only: false,
            relation_names: Vec::new(),
            relation_output_exprs: Vec::new(),
            hidden_invalid_relation_names: Vec::new(),
            hidden_missing_relation_names: Vec::new(),
            source_relation_oid: None,
            source_attno: None,
            source_columns: Vec::new(),
        })
        .collect::<Vec<_>>();
    let mut relations = Vec::new();
    for (relation_name, desc) in relation_scopes {
        relations.push(scope::ScopeRelation {
            relation_names: vec![(*relation_name).to_string()],
            hidden_invalid_relation_names: Vec::new(),
            hidden_missing_relation_names: Vec::new(),
            system_varno: single_relation_system_varno,
            relation_oid: None,
        });
        for column in &desc.columns {
            desc_columns.push(column.clone());
            scope_columns.push(scope::ScopeColumn {
                output_name: column.name.clone(),
                hidden: column.dropped,
                qualified_only: false,
                relation_names: vec![(*relation_name).to_string()],
                relation_output_exprs: Vec::new(),
                hidden_invalid_relation_names: Vec::new(),
                hidden_missing_relation_names: Vec::new(),
                source_relation_oid: None,
                source_attno: None,
                source_columns: Vec::new(),
            });
        }
    }
    let desc = RelationDesc {
        columns: desc_columns,
    };
    scope::BoundScope {
        output_exprs: desc
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Expr::Var(Var {
                    varno: 1,
                    varattno: user_attrno(index),
                    varlevelsup: 0,
                    vartype: column.sql_type,
                    collation_oid: None,
                })
            })
            .collect(),
        desc,
        columns: scope_columns,
        relations,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SlotScopeColumn {
    pub slot: usize,
    pub name: String,
    pub sql_type: SqlType,
    pub hidden: bool,
}

const NAMED_SLOT_VARNO: usize = 0;

pub(crate) fn bind_scalar_expr_in_named_slot_scope(
    expr: &SqlExpr,
    relation_scopes: &[(String, Vec<SlotScopeColumn>)],
    columns: &[SlotScopeColumn],
    catalog: &dyn CatalogLookup,
    ctes: &[BoundCte],
) -> Result<(Expr, SqlType), ParseError> {
    if columns.is_empty() && relation_scopes.is_empty() {
        let empty_scope = scope::empty_scope();
        let empty_outer = Vec::new();
        let bound =
            bind_expr_with_outer_and_ctes(expr, &empty_scope, catalog, &empty_outer, None, ctes)?;
        let sql_type = expr_sql_type_hint(&bound).unwrap_or_else(|| {
            infer_sql_expr_type_with_ctes(expr, &empty_scope, catalog, &empty_outer, None, ctes)
        });
        return Ok((bound, sql_type));
    }

    let mut desc_columns = Vec::new();
    let mut scope_columns = Vec::new();
    let mut output_exprs = Vec::new();
    let mut relations = Vec::new();

    for column in columns {
        desc_columns.push(column_desc(column.name.clone(), column.sql_type, true));
        scope_columns.push(scope::ScopeColumn {
            output_name: column.name.clone(),
            hidden: column.hidden,
            qualified_only: false,
            relation_names: Vec::new(),
            relation_output_exprs: Vec::new(),
            hidden_invalid_relation_names: Vec::new(),
            hidden_missing_relation_names: Vec::new(),
            source_relation_oid: None,
            source_attno: None,
            source_columns: Vec::new(),
        });
        output_exprs.push(Expr::Var(Var {
            varno: NAMED_SLOT_VARNO,
            varattno: user_attrno(column.slot),
            varlevelsup: 0,
            vartype: column.sql_type,
            collation_oid: None,
        }));
    }

    for (relation_name, relation_columns) in relation_scopes {
        relations.push(scope::ScopeRelation {
            relation_names: vec![relation_name.clone()],
            hidden_invalid_relation_names: Vec::new(),
            hidden_missing_relation_names: Vec::new(),
            system_varno: None,
            relation_oid: None,
        });
        for column in relation_columns {
            desc_columns.push(column_desc(column.name.clone(), column.sql_type, true));
            scope_columns.push(scope::ScopeColumn {
                output_name: column.name.clone(),
                hidden: column.hidden,
                qualified_only: true,
                relation_names: vec![relation_name.clone()],
                relation_output_exprs: Vec::new(),
                hidden_invalid_relation_names: Vec::new(),
                hidden_missing_relation_names: Vec::new(),
                source_relation_oid: None,
                source_attno: None,
                source_columns: Vec::new(),
            });
            output_exprs.push(Expr::Var(Var {
                varno: NAMED_SLOT_VARNO,
                varattno: user_attrno(column.slot),
                varlevelsup: 0,
                vartype: column.sql_type,
                collation_oid: None,
            }));
        }
    }

    let desc = RelationDesc {
        columns: desc_columns,
    };
    let scope = scope::BoundScope {
        output_exprs,
        desc,
        columns: scope_columns,
        relations,
    };
    // PL/pgSQL scalar expressions can contain correlated subqueries that need
    // to see the same named-slot scope as the enclosing expression.
    let outer_scopes = vec![scope.clone()];
    let bound = bind_expr_with_outer_and_ctes(expr, &scope, catalog, &outer_scopes, None, ctes)?;
    let sql_type = expr_sql_type_hint(&bound).unwrap_or_else(|| {
        infer_sql_expr_type_with_ctes(expr, &scope, catalog, &outer_scopes, None, ctes)
    });
    Ok((bound, sql_type))
}

fn normalize_create_table_name_parts(
    schema_name: Option<&str>,
    table_name: &str,
    persistence: TablePersistence,
    on_commit: OnCommitAction,
) -> Result<(String, TablePersistence), ParseError> {
    let effective_persistence = match schema_name.map(|s| s.to_ascii_lowercase()) {
        Some(schema) if schema == "pg_temp" => {
            if persistence == TablePersistence::Unlogged {
                return Err(ParseError::DetailedError {
                    message: "only temporary relations may be created in temporary schemas".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42P16",
                });
            }
            TablePersistence::Temporary
        }
        Some(schema) => {
            if persistence == TablePersistence::Temporary {
                return Err(ParseError::TempTableInNonTempSchema(schema));
            }
            persistence
        }
        None => persistence,
    };

    if on_commit != OnCommitAction::PreserveRows
        && effective_persistence != TablePersistence::Temporary
    {
        return Err(ParseError::OnCommitOnlyForTempTables);
    }

    Ok((table_name.to_string(), effective_persistence))
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
        stmt.persistence,
        OnCommitAction::PreserveRows,
    )
    .map(|(name, _)| name)
}

fn apply_cte_column_names(
    cte_name: &str,
    cte_location: Option<usize>,
    mut query: Query,
    desc: RelationDesc,
    column_names: &[String],
) -> Result<(Query, RelationDesc), ParseError> {
    if column_names.is_empty() {
        return Ok((query, desc));
    }
    if column_names.len() != desc.columns.len() {
        let err = ParseError::UnexpectedToken {
            expected: "CTE column alias count matching query width",
            actual: format!(
                "WITH query \"{}\" has {} columns available but {} columns specified",
                cte_name,
                desc.columns.len(),
                column_names.len()
            ),
        };
        return Err(positioned_if_available(err, cte_location));
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
    for (index, column) in renamed_desc.columns.iter().enumerate() {
        if let Some(target) = query.target_list.get_mut(index) {
            target.name = column.name.clone();
            target.sql_type = column.sql_type;
            target.resno = index + 1;
        }
    }
    Ok((query, renamed_desc))
}

fn positioned_if_available(err: ParseError, position: Option<usize>) -> ParseError {
    match position {
        Some(position) => err.with_position(position),
        None => err,
    }
}

fn cte_query_desc(query: &Query) -> RelationDesc {
    RelationDesc {
        columns: query
            .columns()
            .into_iter()
            .map(|col| column_desc(col.name, col.sql_type, true))
            .collect(),
    }
}

fn analyze_non_recursive_cte_body(
    body: &CteBody,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    visible_ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<(Query, RelationDesc), ParseError> {
    let cte_outer_scopes = cte_body_outer_scopes(outer_scopes);
    let visible_agg_scope = current_visible_aggregate_scope();
    match body {
        CteBody::Select(select) => {
            let (query, _) = analyze_select_query_with_outer(
                select,
                catalog,
                &cte_outer_scopes,
                grouped_outer,
                visible_agg_scope.as_ref(),
                visible_ctes,
                expanded_views,
            )?;
            let desc = cte_query_desc(&query);
            Ok((query, desc))
        }
        CteBody::Values(values) => {
            let (query, _) = analyze_values_query_with_outer(
                values,
                catalog,
                &cte_outer_scopes,
                grouped_outer,
                visible_ctes,
                expanded_views,
            )?;
            let desc = cte_query_desc(&query);
            Ok((query, desc))
        }
        CteBody::Insert(_) | CteBody::Update(_) | CteBody::Delete(_) | CteBody::Merge(_) => {
            Err(ParseError::FeatureNotSupported(
                "writable CTE must be materialized before binding".into(),
            ))
        }
        CteBody::RecursiveUnion { .. } => {
            let stmt = cte_body_as_select(body)?;
            let (query, _) = analyze_select_query_with_outer(
                &stmt,
                catalog,
                &cte_outer_scopes,
                grouped_outer,
                visible_agg_scope.as_ref(),
                visible_ctes,
                expanded_views,
            )?;
            let desc = cte_query_desc(&query);
            Ok((query, desc))
        }
    }
}

fn cte_body_as_select(body: &CteBody) -> Result<SelectStatement, ParseError> {
    match body {
        CteBody::Select(select) => Ok((**select).clone()),
        CteBody::Values(values) => Ok(SelectStatement {
            with_recursive: values.with_recursive,
            with: values.with.clone(),
            with_from_recursive_union_outer: false,
            distinct: false,
            distinct_on: Vec::new(),
            from: Some(FromItem::Values {
                rows: values.rows.clone(),
            }),
            targets: vec![SelectItem {
                output_name: "*".into(),
                expr: SqlExpr::Column("*".into()),
                location: None,
            }],
            where_clause: None,
            group_by: Vec::new(),
            group_by_distinct: false,
            having: None,
            window_clauses: Vec::new(),
            order_by: values.order_by.clone(),
            order_by_location: None,
            limit: values.limit,
            limit_location: None,
            offset: values.offset,
            offset_location: None,
            locking_clause: None,
            locking_location: None,
            locking_targets: Vec::new(),
            locking_nowait: false,
            set_operation: None,
        }),
        CteBody::Insert(_) | CteBody::Update(_) | CteBody::Delete(_) | CteBody::Merge(_) => {
            Err(ParseError::FeatureNotSupported(
                "writable CTE must be materialized before binding".into(),
            ))
        }
        CteBody::RecursiveUnion {
            all,
            left_nested: _,
            anchor_with_is_subquery: _,
            anchor,
            recursive,
        } => Ok(SelectStatement {
            with_recursive: false,
            with: Vec::new(),
            with_from_recursive_union_outer: false,
            distinct: false,
            distinct_on: Vec::new(),
            from: None,
            targets: Vec::new(),
            where_clause: None,
            group_by: Vec::new(),
            group_by_distinct: false,
            having: None,
            window_clauses: Vec::new(),
            order_by: Vec::new(),
            order_by_location: None,
            limit: None,
            limit_location: None,
            offset: None,
            offset_location: None,
            locking_clause: None,
            locking_location: None,
            locking_targets: Vec::new(),
            locking_nowait: false,
            set_operation: Some(Box::new(SetOperationStatement {
                op: SetOperator::Union { all: *all },
                inputs: vec![cte_body_as_select(anchor)?, (**recursive).clone()],
                location: None,
            })),
        }),
    }
}

fn cte_body_outer_scopes(outer_scopes: &[BoundScope]) -> Vec<BoundScope> {
    // CTE bodies are nested Query levels, but they cannot see the containing
    // statement's local scope. Insert an empty boundary scope so correlated
    // Vars keep the same sublevels_up shape that the planner/setrefs pipeline
    // expects from other child queries.
    let mut cte_outer_scopes = Vec::with_capacity(outer_scopes.len() + 1);
    cte_outer_scopes.push(empty_scope());
    cte_outer_scopes.extend_from_slice(outer_scopes);
    cte_outer_scopes
}

fn prevalidate_recursive_select_targets(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    visible_ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<(), ParseError> {
    if stmt.set_operation.is_some() {
        return Ok(());
    }
    let local_ctes = bind_ctes(
        stmt.with_recursive,
        &stmt.with,
        catalog,
        outer_scopes,
        grouped_outer.cloned(),
        visible_ctes,
        expanded_views,
    )?;
    let mut precheck_visible_ctes = local_ctes;
    precheck_visible_ctes.extend_from_slice(visible_ctes);
    let scope = if let Some(from) = &stmt.from {
        bind_from_item_with_ctes(
            from,
            catalog,
            outer_scopes,
            grouped_outer,
            &precheck_visible_ctes,
            expanded_views,
        )?
        .1
    } else {
        empty_scope()
    };
    let _ = bind_select_targets(
        &stmt.targets,
        &scope,
        catalog,
        outer_scopes,
        grouped_outer,
        &precheck_visible_ctes,
    )?;
    Ok(())
}

fn bind_ctes(
    with_recursive: bool,
    ctes: &[CommonTableExpr],
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    outer_ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<Vec<BoundCte>, ParseError> {
    let binding_order = if with_recursive {
        recursive_cte_binding_order(ctes)?
    } else {
        (0..ctes.len()).collect()
    };
    let mut bound_by_index = vec![None; ctes.len()];
    for cte_index in binding_order {
        let cte = &ctes[cte_index];
        let cte_id = NEXT_CTE_ID.fetch_add(1, Ordering::Relaxed);
        let mut visible = bound_by_index
            .iter()
            .filter_map(|bound| bound.clone())
            .collect::<Vec<_>>();
        visible.extend_from_slice(outer_ctes);
        let self_references_cte = cte_body_references_table(&cte.body, &cte.name);
        if with_recursive && self_references_cte && cte_body_is_modifying(&cte.body) {
            return Err(positioned_if_available(
                ParseError::InvalidRecursion(format!(
                    "recursive query \"{}\" must not contain data-modifying statements",
                    cte.name
                )),
                cte.location,
            ));
        }
        if with_recursive
            && self_references_cte
            && !matches!(cte.body, CteBody::RecursiveUnion { .. })
        {
            return Err(invalid_recursive_cte_shape(cte));
        }
        if !self_references_cte
            && (cte.search.is_some() || cte.cycle.is_some())
            && let CteBody::RecursiveUnion { recursive, .. } = &cte.body
            && recursive
                .with
                .iter()
                .any(|inner_cte| inner_cte.name.eq_ignore_ascii_case(&cte.name))
        {
            return Err(ParseError::FeatureNotSupportedMessage(format!(
                "with a SEARCH or CYCLE clause, the recursive reference to WITH query \"{}\" must be at the top level of its right-hand SELECT",
                cte.name
            )));
        }
        let (plan, desc) = match &cte.body {
            CteBody::RecursiveUnion {
                all,
                left_nested,
                anchor_with_is_subquery,
                anchor,
                recursive,
            } if with_recursive && self_references_cte => {
                let top_level_with_names = recursive_union_top_level_with_names(
                    anchor,
                    recursive,
                    *anchor_with_is_subquery,
                );
                validate_recursive_cte_non_recursive_term(
                    anchor,
                    &cte.name,
                    &top_level_with_names,
                )?;
                let (base_anchor_query, base_anchor_desc) = analyze_non_recursive_cte_body(
                    anchor,
                    catalog,
                    outer_scopes,
                    grouped_outer.clone(),
                    &visible,
                    expanded_views,
                )?;
                validate_recursive_cte_recursive_term(recursive, &cte.name)?;
                let (base_anchor_query, base_desc) = apply_cte_column_names(
                    &cte.name,
                    cte.location,
                    base_anchor_query,
                    base_anchor_desc,
                    &cte.column_names,
                )?;
                validate_cte_search_cycle_clauses(cte, &base_desc, catalog)?;
                let (anchor_query, recursive_stmt, desc) =
                    if cte.search.is_some() || cte.cycle.is_some() {
                        validate_search_cycle_recursive_shape(
                            &cte.name,
                            *left_nested,
                            anchor,
                            recursive,
                        )?;
                        let (rewritten_anchor, rewritten_recursive) =
                            rewrite_search_cycle_recursive_cte(cte, anchor, recursive, &base_desc)?;
                        let (anchor_query, desc) = analyze_non_recursive_cte_body(
                            &rewritten_anchor,
                            catalog,
                            outer_scopes,
                            grouped_outer.clone(),
                            &visible,
                            expanded_views,
                        )?;
                        (anchor_query, rewritten_recursive, desc)
                    } else {
                        (base_anchor_query, (**recursive).clone(), base_desc)
                    };
                let recursive_references_worktable =
                    select_statement_references_table(&recursive_stmt, &cte.name);
                let worktable_id = NEXT_WORKTABLE_ID.fetch_add(1, Ordering::Relaxed);
                let output_columns = desc
                    .columns
                    .iter()
                    .map(|column| QueryColumn {
                        name: column.name.clone(),
                        sql_type: column.sql_type,
                        wire_type_oid: None,
                    })
                    .collect::<Vec<_>>();
                let worktable_plan = AnalyzedFrom::worktable(worktable_id, output_columns.clone());
                let mut recursive_visible = visible.clone();
                recursive_visible.push(BoundCte {
                    name: cte.name.clone(),
                    cte_id,
                    plan: Query {
                        command_type: crate::include::executor::execdesc::CommandType::Select,
                        depends_on_row_security: false,
                        rtable: worktable_plan.rtable.clone(),
                        jointree: worktable_plan.jointree.clone(),
                        target_list: identity_target_list(
                            &output_columns,
                            &worktable_plan.output_exprs,
                        ),
                        distinct: false,
                        distinct_on: Vec::new(),
                        where_qual: None,
                        group_by: Vec::new(),
                        group_by_refs: Vec::new(),
                        grouping_sets: Vec::new(),
                        accumulators: Vec::new(),
                        window_clauses: Vec::new(),
                        having_qual: None,
                        sort_clause: Vec::new(),
                        constraint_deps: Vec::new(),
                        limit_count: None,
                        limit_offset: None,
                        locking_clause: None,
                        locking_targets: Vec::new(),
                        locking_nowait: false,
                        row_marks: Vec::new(),
                        has_target_srfs: false,
                        recursive_union: None,
                        set_operation: None,
                    },
                    desc: desc.clone(),
                    self_reference: true,
                    worktable_id,
                });
                let recursive_outer_scopes = cte_body_outer_scopes(outer_scopes);
                validate_recursive_term_target_operator_errors(
                    &recursive_stmt,
                    catalog,
                    &recursive_outer_scopes,
                    grouped_outer.clone(),
                    current_visible_aggregate_scope().as_ref(),
                    &recursive_visible,
                    expanded_views,
                )?;
                let (mut recursive_query, _) = analyze_select_query_with_outer(
                    &recursive_stmt,
                    catalog,
                    &recursive_outer_scopes,
                    grouped_outer.clone(),
                    current_visible_aggregate_scope().as_ref(),
                    &recursive_visible,
                    expanded_views,
                )?;
                let recursive_desc = cte_query_desc(&recursive_query);
                if recursive_desc.columns.len() != desc.columns.len() {
                    return Err(ParseError::UnexpectedToken {
                        expected: "recursive term width matching non-recursive term",
                        actual: format!(
                            "recursive term has {} columns but non-recursive term has {}",
                            recursive_desc.columns.len(),
                            desc.columns.len()
                        ),
                    });
                }
                for (index, (left, right)) in desc
                    .columns
                    .iter()
                    .zip(recursive_desc.columns.iter())
                    .enumerate()
                {
                    if left.sql_type != right.sql_type {
                        let target = recursive_query
                            .target_list
                            .get_mut(index)
                            .expect("recursive term target width checked earlier");
                        if is_binary_coercible_type(right.sql_type, left.sql_type)
                            || resolve_common_scalar_type(left.sql_type, right.sql_type)
                                == Some(left.sql_type)
                        {
                            target.expr = coerce_bound_expr(
                                target.expr.clone(),
                                right.sql_type,
                                left.sql_type,
                            );
                            target.sql_type = left.sql_type;
                        } else {
                            let overall_type =
                                resolve_common_scalar_type(left.sql_type, right.sql_type)
                                    .unwrap_or(right.sql_type);
                            return Err(positioned_if_available(
                                ParseError::DetailedError {
                                    message: format!(
                                        "recursive query \"{}\" column {} has type {} in non-recursive term but type {} overall",
                                        cte.name,
                                        index + 1,
                                        recursive_cte_error_type_name(left.sql_type),
                                        sql_type_name(overall_type)
                                    ),
                                    detail: None,
                                    hint: Some(
                                        "Cast the output of the non-recursive term to the correct type."
                                            .into(),
                                    ),
                                    sqlstate: "42804",
                                },
                                cte_body_target_location(anchor, index),
                            ));
                        }
                    }
                }
                let recursive_plan = AnalyzedFrom::worktable(worktable_id, output_columns.clone());
                let target_list = normalize_target_list(identity_target_list(
                    &output_columns,
                    &recursive_plan.output_exprs,
                ));
                (
                    Query {
                        command_type: crate::include::executor::execdesc::CommandType::Select,
                        depends_on_row_security: false,
                        rtable: recursive_plan.rtable,
                        jointree: recursive_plan.jointree,
                        target_list,
                        distinct: false,
                        distinct_on: Vec::new(),
                        where_qual: None,
                        group_by: Vec::new(),
                        group_by_refs: Vec::new(),
                        grouping_sets: Vec::new(),
                        accumulators: Vec::new(),
                        window_clauses: Vec::new(),
                        having_qual: None,
                        sort_clause: Vec::new(),
                        constraint_deps: Vec::new(),
                        limit_count: None,
                        limit_offset: None,
                        locking_clause: None,
                        locking_targets: Vec::new(),
                        locking_nowait: false,
                        row_marks: Vec::new(),
                        has_target_srfs: false,
                        recursive_union: Some(Box::new(RecursiveUnionQuery {
                            output_desc: desc.clone(),
                            anchor: anchor_query,
                            recursive: recursive_query,
                            distinct: !*all,
                            recursive_references_worktable,
                            worktable_id,
                        })),
                        set_operation: None,
                    },
                    desc,
                )
            }
            _ => {
                let (query, desc) = analyze_non_recursive_cte_body(
                    &cte.body,
                    catalog,
                    outer_scopes,
                    grouped_outer.clone(),
                    &visible,
                    expanded_views,
                )
                .map_err(|err| {
                    if with_recursive {
                        err
                    } else {
                        non_recursive_cte_forward_reference_error(err, ctes, cte_index, &cte.body)
                    }
                })?;
                apply_cte_column_names(&cte.name, cte.location, query, desc, &cte.column_names)?
            }
        };
        bound_by_index[cte_index] = Some(BoundCte {
            name: cte.name.clone(),
            cte_id,
            plan,
            desc,
            self_reference: false,
            worktable_id: 0,
        });
    }
    Ok(bound_by_index
        .into_iter()
        .map(|bound| bound.expect("all CTEs are bound"))
        .collect())
}

fn non_recursive_cte_forward_reference_error(
    err: ParseError,
    ctes: &[CommonTableExpr],
    current_index: usize,
    body: &CteBody,
) -> ParseError {
    let ParseError::UnknownTable(name) = err else {
        return err;
    };
    if !ctes
        .iter()
        .enumerate()
        .any(|(index, cte)| index >= current_index && cte.name.eq_ignore_ascii_case(&name))
    {
        return ParseError::UnknownTable(name);
    }
    let position = cte_body_table_reference_locations(body, &name)
        .into_iter()
        .next();
    positioned_if_available(
        ParseError::DetailedError {
            message: format!("relation \"{name}\" does not exist"),
            detail: Some(format!(
                "There is a WITH item named \"{name}\", but it cannot be referenced from this part of the query."
            )),
            hint: Some(
                "Use WITH RECURSIVE, or re-order the WITH items to remove forward references."
                    .into(),
            ),
            sqlstate: "42P01",
        },
        position,
    )
}

fn recursive_cte_binding_order(ctes: &[CommonTableExpr]) -> Result<Vec<usize>, ParseError> {
    fn visit(
        index: usize,
        ctes: &[CommonTableExpr],
        states: &mut [u8],
        order: &mut Vec<usize>,
    ) -> Result<(), ParseError> {
        match states[index] {
            1 => {
                return Err(positioned_if_available(
                    ParseError::InvalidRecursion(
                        "mutual recursion between WITH items is not implemented".into(),
                    ),
                    ctes[index].location,
                ));
            }
            2 => return Ok(()),
            _ => {}
        }
        states[index] = 1;
        for dependency in recursive_cte_dependencies(ctes, index) {
            visit(dependency, ctes, states, order)?;
        }
        states[index] = 2;
        order.push(index);
        Ok(())
    }

    let mut states = vec![0; ctes.len()];
    let mut order = Vec::with_capacity(ctes.len());
    for index in 0..ctes.len() {
        visit(index, ctes, &mut states, &mut order)?;
    }
    Ok(order)
}

fn recursive_cte_dependencies(ctes: &[CommonTableExpr], index: usize) -> Vec<usize> {
    ctes.iter()
        .enumerate()
        .filter_map(|(other_index, other)| {
            if other_index == index {
                return None;
            }
            cte_body_references_table(&ctes[index].body, &other.name).then_some(other_index)
        })
        .collect()
}

pub(super) fn cte_body_is_modifying(body: &CteBody) -> bool {
    match body {
        CteBody::Insert(_) | CteBody::Update(_) | CteBody::Delete(_) | CteBody::Merge(_) => true,
        CteBody::Select(_) | CteBody::Values(_) => false,
        CteBody::RecursiveUnion {
            anchor, recursive, ..
        } => {
            cte_body_is_modifying(anchor)
                || recursive
                    .with
                    .iter()
                    .any(|cte| cte_body_is_modifying(&cte.body))
        }
    }
}

fn invalid_recursive_cte_shape(cte: &CommonTableExpr) -> ParseError {
    positioned_if_available(
        ParseError::InvalidRecursion(format!(
            "recursive query \"{}\" does not have the form non-recursive-term UNION [ALL] recursive-term",
            cte.name
        )),
        cte.location,
    )
}

fn cte_body_target_location(body: &CteBody, index: usize) -> Option<usize> {
    match body {
        CteBody::Select(select) => select.targets.get(index).and_then(|target| target.location),
        CteBody::Values(_) => None,
        CteBody::RecursiveUnion { anchor, .. } => cte_body_target_location(anchor, index),
        CteBody::Insert(_) | CteBody::Update(_) | CteBody::Delete(_) | CteBody::Merge(_) => None,
    }
}

fn recursive_cte_error_type_name(sql_type: SqlType) -> String {
    if let Some((precision, scale)) = sql_type.numeric_precision_scale() {
        return format!("numeric({precision},{scale})");
    }
    sql_type_name(sql_type)
}

fn validate_cte_search_cycle_clauses(
    cte: &CommonTableExpr,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    let output_names = desc
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    if let Some(search) = &cte.search {
        validate_cte_search_clause(search, &output_names)?;
    }
    if let Some(cycle) = &cte.cycle {
        validate_cte_cycle_clause(cycle, &output_names)?;
        validate_cte_cycle_clause_types(cycle, catalog)?;
    }
    if let (Some(search), Some(cycle)) = (&cte.search, &cte.cycle) {
        if search
            .sequence_column
            .eq_ignore_ascii_case(&cycle.mark_column)
        {
            return Err(positioned_if_available(
                ParseError::FeatureNotSupportedMessage(
                    "search sequence column name and cycle mark column name are the same".into(),
                ),
                search.location,
            ));
        }
        if search
            .sequence_column
            .eq_ignore_ascii_case(&cycle.path_column)
        {
            return Err(positioned_if_available(
                ParseError::FeatureNotSupportedMessage(
                    "search sequence column name and cycle path column name are the same".into(),
                ),
                search.location,
            ));
        }
    }
    Ok(())
}

fn validate_search_cycle_recursive_shape(
    cte_name: &str,
    left_nested: bool,
    anchor: &CteBody,
    recursive: &SelectStatement,
) -> Result<(), ParseError> {
    if left_nested || matches!(anchor, CteBody::RecursiveUnion { .. }) {
        return Err(ParseError::FeatureNotSupportedMessage(
            "with a SEARCH or CYCLE clause, the left side of the UNION must be a SELECT".into(),
        ));
    }
    if recursive.set_operation.is_some() {
        return Err(ParseError::FeatureNotSupportedMessage(
            "with a SEARCH or CYCLE clause, the right side of the UNION must be a SELECT".into(),
        ));
    }
    if recursive
        .with
        .iter()
        .any(|cte| cte.name.eq_ignore_ascii_case(cte_name))
    {
        return Err(ParseError::FeatureNotSupportedMessage(format!(
            "with a SEARCH or CYCLE clause, the recursive reference to WITH query \"{cte_name}\" must be at the top level of its right-hand SELECT"
        )));
    }
    if recursive
        .from
        .as_ref()
        .and_then(|from| direct_recursive_cte_ref_qualifier(from, cte_name))
        .is_none()
    {
        return Err(ParseError::FeatureNotSupportedMessage(format!(
            "with a SEARCH or CYCLE clause, the recursive reference to WITH query \"{cte_name}\" must be at the top level of its right-hand SELECT"
        )));
    }
    Ok(())
}

fn rewrite_search_cycle_recursive_cte(
    cte: &CommonTableExpr,
    anchor: &CteBody,
    recursive: &SelectStatement,
    base_desc: &RelationDesc,
) -> Result<(CteBody, SelectStatement), ParseError> {
    let base_names = base_desc
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let mut expanded_names = base_names.clone();
    if let Some(search) = &cte.search {
        expanded_names.push(search.sequence_column.clone());
    }
    if let Some(cycle) = &cte.cycle {
        expanded_names.push(cycle.mark_column.clone());
        expanded_names.push(cycle.path_column.clone());
    }

    let anchor_alias = "__pgrust_sc_anchor";
    let recursive_alias = "__pgrust_sc_recursive";
    let self_qualifier = recursive
        .from
        .as_ref()
        .and_then(|from| direct_recursive_cte_ref_qualifier(from, &cte.name))
        .ok_or_else(|| {
            ParseError::FeatureNotSupportedMessage(format!(
                "with a SEARCH or CYCLE clause, the recursive reference to WITH query \"{}\" must be at the top level of its right-hand SELECT",
                cte.name
            ))
        })?;

    let anchor_select = cte_body_as_select(anchor)?;
    let mut anchor_targets = base_names
        .iter()
        .map(|name| select_item(name, qualified_column(anchor_alias, name)))
        .collect::<Vec<_>>();
    append_search_cycle_anchor_targets(&mut anchor_targets, cte, anchor_alias);
    let rewritten_anchor = CteBody::Select(Box::new(select_from_derived(
        anchor_select,
        anchor_alias,
        base_names.clone(),
        anchor_targets,
    )));

    let mut inner_recursive = recursive.clone();
    rewrite_self_star_targets(
        &mut inner_recursive,
        &self_qualifier,
        &cte.name,
        &base_names,
    );
    append_carried_recursive_targets(&mut inner_recursive.targets, cte, &self_qualifier);
    if let Some(cycle) = &cte.cycle {
        let filter = SqlExpr::NotEq(
            Box::new(qualified_column(&self_qualifier, &cycle.mark_column)),
            Box::new(cycle_mark_value(cycle)),
        );
        inner_recursive.where_clause = Some(match inner_recursive.where_clause.take() {
            Some(existing) => SqlExpr::And(Box::new(existing), Box::new(filter)),
            None => filter,
        });
    }

    let mut recursive_targets = base_names
        .iter()
        .map(|name| select_item(name, qualified_column(recursive_alias, name)))
        .collect::<Vec<_>>();
    append_search_cycle_recursive_targets(&mut recursive_targets, cte, recursive_alias);
    let rewritten_recursive = select_from_derived(
        inner_recursive,
        recursive_alias,
        expanded_names,
        recursive_targets,
    );

    Ok((rewritten_anchor, rewritten_recursive))
}

fn select_item(name: &str, expr: SqlExpr) -> SelectItem {
    SelectItem {
        output_name: name.to_string(),
        expr,
        location: None,
    }
}

fn select_from_derived(
    source: SelectStatement,
    alias: &str,
    column_names: Vec<String>,
    targets: Vec<SelectItem>,
) -> SelectStatement {
    SelectStatement {
        with_recursive: false,
        with: Vec::new(),
        with_from_recursive_union_outer: false,
        distinct: false,
        distinct_on: Vec::new(),
        from: Some(FromItem::Alias {
            source: Box::new(FromItem::DerivedTable(Box::new(source))),
            alias: alias.to_string(),
            column_aliases: AliasColumnSpec::Names(column_names),
            preserve_source_names: false,
        }),
        targets,
        where_clause: None,
        group_by: Vec::new(),
        group_by_distinct: false,
        having: None,
        window_clauses: Vec::new(),
        order_by: Vec::new(),
        order_by_location: None,
        limit: None,
        limit_location: None,
        offset: None,
        offset_location: None,
        locking_clause: None,
        locking_location: None,
        locking_targets: Vec::new(),
        locking_nowait: false,
        set_operation: None,
    }
}

fn append_search_cycle_anchor_targets(
    targets: &mut Vec<SelectItem>,
    cte: &CommonTableExpr,
    alias: &str,
) {
    if let Some(search) = &cte.search {
        targets.push(select_item(
            &search.sequence_column,
            search_anchor_expr(search, alias),
        ));
    }
    if let Some(cycle) = &cte.cycle {
        targets.push(select_item(&cycle.mark_column, cycle_default_value(cycle)));
        targets.push(select_item(
            &cycle.path_column,
            initial_path_expr(&cycle.columns, alias),
        ));
    }
}

fn append_search_cycle_recursive_targets(
    targets: &mut Vec<SelectItem>,
    cte: &CommonTableExpr,
    alias: &str,
) {
    if let Some(search) = &cte.search {
        targets.push(select_item(
            &search.sequence_column,
            search_recursive_expr(search, alias),
        ));
    }
    if let Some(cycle) = &cte.cycle {
        targets.push(select_item(
            &cycle.mark_column,
            cycle_mark_case(cycle, alias),
        ));
        targets.push(select_item(
            &cycle.path_column,
            path_cat_expr(&cycle.path_column, &cycle.columns, alias),
        ));
    }
}

fn append_carried_recursive_targets(
    targets: &mut Vec<SelectItem>,
    cte: &CommonTableExpr,
    qualifier: &str,
) {
    if let Some(search) = &cte.search {
        targets.push(select_item(
            &search.sequence_column,
            qualified_column(qualifier, &search.sequence_column),
        ));
    }
    if let Some(cycle) = &cte.cycle {
        targets.push(select_item(
            &cycle.mark_column,
            qualified_column(qualifier, &cycle.mark_column),
        ));
        targets.push(select_item(
            &cycle.path_column,
            qualified_column(qualifier, &cycle.path_column),
        ));
    }
}

fn search_anchor_expr(search: &CteSearchClause, alias: &str) -> SqlExpr {
    if search.breadth_first {
        let mut items = vec![SqlExpr::Const(Value::Int64(0))];
        items.extend(
            search
                .columns
                .iter()
                .map(|name| qualified_column(alias, name)),
        );
        SqlExpr::Row(items)
    } else {
        initial_path_expr(&search.columns, alias)
    }
}

fn search_recursive_expr(search: &CteSearchClause, alias: &str) -> SqlExpr {
    if search.breadth_first {
        let depth = SqlExpr::FieldSelect {
            expr: Box::new(qualified_column(alias, &search.sequence_column)),
            field: "f1".into(),
        };
        let mut items = vec![SqlExpr::Add(
            Box::new(depth),
            Box::new(SqlExpr::Const(Value::Int64(1))),
        )];
        items.extend(
            search
                .columns
                .iter()
                .map(|name| qualified_column(alias, name)),
        );
        SqlExpr::Row(items)
    } else {
        path_cat_expr(&search.sequence_column, &search.columns, alias)
    }
}

fn cycle_mark_case(cycle: &CteCycleClause, alias: &str) -> SqlExpr {
    SqlExpr::Case {
        arg: None,
        args: vec![SqlCaseWhen {
            expr: SqlExpr::QuantifiedArray {
                left: Box::new(path_row_expr(&cycle.columns, alias)),
                op: SubqueryComparisonOp::Eq,
                is_all: false,
                array: Box::new(qualified_column(alias, &cycle.path_column)),
            },
            result: cycle_mark_value(cycle),
        }],
        defresult: Some(Box::new(cycle_default_value(cycle))),
    }
}

fn initial_path_expr(columns: &[String], alias: &str) -> SqlExpr {
    SqlExpr::ArrayLiteral(vec![path_row_expr(columns, alias)])
}

fn path_cat_expr(path_column: &str, columns: &[String], alias: &str) -> SqlExpr {
    SqlExpr::FuncCall {
        name: "array_cat".into(),
        args: SqlCallArgs::Args(vec![
            SqlFunctionArg::positional(qualified_column(alias, path_column)),
            SqlFunctionArg::positional(initial_path_expr(columns, alias)),
        ]),
        order_by: Vec::new(),
        within_group: None,
        distinct: false,
        func_variadic: false,
        filter: None,
        null_treatment: None,
        over: None,
    }
}

fn path_row_expr(columns: &[String], alias: &str) -> SqlExpr {
    SqlExpr::Row(
        columns
            .iter()
            .map(|name| qualified_column(alias, name))
            .collect(),
    )
}

fn cycle_mark_value(cycle: &CteCycleClause) -> SqlExpr {
    cycle
        .mark_value
        .clone()
        .unwrap_or(SqlExpr::Const(Value::Bool(true)))
}

fn cycle_default_value(cycle: &CteCycleClause) -> SqlExpr {
    cycle
        .default_value
        .clone()
        .unwrap_or(SqlExpr::Const(Value::Bool(false)))
}

fn qualified_column(qualifier: &str, column: &str) -> SqlExpr {
    SqlExpr::Column(format!("{qualifier}.{column}"))
}

fn rewrite_self_star_targets(
    stmt: &mut SelectStatement,
    self_qualifier: &str,
    cte_name: &str,
    base_names: &[String],
) {
    let only_self_reference = stmt
        .from
        .as_ref()
        .is_some_and(|from| from_item_is_only_recursive_reference(from, cte_name));
    let mut rewritten = Vec::new();
    for target in std::mem::take(&mut stmt.targets) {
        if target_is_self_star(&target.expr, self_qualifier, cte_name, only_self_reference) {
            rewritten.extend(
                base_names
                    .iter()
                    .map(|name| select_item(name, qualified_column(self_qualifier, name))),
            );
        } else {
            rewritten.push(target);
        }
    }
    stmt.targets = rewritten;
}

fn target_is_self_star(
    expr: &SqlExpr,
    self_qualifier: &str,
    cte_name: &str,
    only_self_reference: bool,
) -> bool {
    match expr {
        SqlExpr::Column(name) if name == "*" => only_self_reference,
        SqlExpr::Column(name) => name.strip_suffix(".*").is_some_and(|qualifier| {
            qualifier.eq_ignore_ascii_case(self_qualifier)
                || qualifier.eq_ignore_ascii_case(cte_name)
        }),
        SqlExpr::FieldSelect { expr, field } if field == "*" => {
            matches!(expr.as_ref(), SqlExpr::Column(name) if name.eq_ignore_ascii_case(self_qualifier) || name.eq_ignore_ascii_case(cte_name))
        }
        _ => false,
    }
}

fn direct_recursive_cte_ref_qualifier(item: &FromItem, cte_name: &str) -> Option<String> {
    match item {
        FromItem::Table { name, .. } if name.eq_ignore_ascii_case(cte_name) => Some(name.clone()),
        FromItem::Alias { source, alias, .. } => match source.as_ref() {
            FromItem::Table { name, .. } if name.eq_ignore_ascii_case(cte_name) => {
                Some(alias.clone())
            }
            _ => direct_recursive_cte_ref_qualifier(source, cte_name),
        },
        FromItem::TableSample { source, .. } | FromItem::Lateral(source) => {
            direct_recursive_cte_ref_qualifier(source, cte_name)
        }
        FromItem::Join { left, right, .. } => direct_recursive_cte_ref_qualifier(left, cte_name)
            .or_else(|| direct_recursive_cte_ref_qualifier(right, cte_name)),
        FromItem::Values { .. }
        | FromItem::Expression { .. }
        | FromItem::Table { .. }
        | FromItem::FunctionCall { .. }
        | FromItem::RowsFrom { .. }
        | FromItem::JsonTable(_)
        | FromItem::XmlTable(_)
        | FromItem::DerivedTable(_) => None,
    }
}

fn from_item_is_only_recursive_reference(item: &FromItem, cte_name: &str) -> bool {
    match item {
        FromItem::Table { name, .. } => name.eq_ignore_ascii_case(cte_name),
        FromItem::Alias { source, .. }
        | FromItem::TableSample { source, .. }
        | FromItem::Lateral(source) => from_item_is_only_recursive_reference(source, cte_name),
        _ => false,
    }
}

fn validate_cte_search_clause(
    search: &CteSearchClause,
    output_names: &[&str],
) -> Result<(), ParseError> {
    for column in &search.columns {
        if !name_in_list(output_names, column) {
            return Err(positioned_if_available(
                ParseError::FeatureNotSupportedMessage(format!(
                    "search column \"{column}\" not in WITH query column list"
                )),
                search.location,
            ));
        }
        if search
            .columns
            .iter()
            .filter(|other| other.eq_ignore_ascii_case(column))
            .count()
            > 1
        {
            return Err(positioned_if_available(
                ParseError::FeatureNotSupportedMessage(format!(
                    "search column \"{column}\" specified more than once"
                )),
                search.location,
            ));
        }
    }
    if name_in_list(output_names, &search.sequence_column) {
        return Err(positioned_if_available(
            ParseError::FeatureNotSupportedMessage(format!(
                "search sequence column name \"{}\" already used in WITH query column list",
                search.sequence_column
            )),
            search.location,
        ));
    }
    Ok(())
}

fn validate_cte_cycle_clause(
    cycle: &CteCycleClause,
    output_names: &[&str],
) -> Result<(), ParseError> {
    for column in &cycle.columns {
        if !name_in_list(output_names, column) {
            return Err(positioned_if_available(
                ParseError::FeatureNotSupportedMessage(format!(
                    "cycle column \"{column}\" not in WITH query column list"
                )),
                cycle.location,
            ));
        }
        if cycle
            .columns
            .iter()
            .filter(|other| other.eq_ignore_ascii_case(column))
            .count()
            > 1
        {
            return Err(positioned_if_available(
                ParseError::FeatureNotSupportedMessage(format!(
                    "cycle column \"{column}\" specified more than once"
                )),
                cycle.location,
            ));
        }
    }
    if name_in_list(output_names, &cycle.mark_column) {
        return Err(positioned_if_available(
            ParseError::FeatureNotSupportedMessage(format!(
                "cycle mark column name \"{}\" already used in WITH query column list",
                cycle.mark_column
            )),
            cycle.location,
        ));
    }
    if name_in_list(output_names, &cycle.path_column) {
        return Err(positioned_if_available(
            ParseError::FeatureNotSupportedMessage(format!(
                "cycle path column name \"{}\" already used in WITH query column list",
                cycle.path_column
            )),
            cycle.location,
        ));
    }
    if cycle.mark_column.eq_ignore_ascii_case(&cycle.path_column) {
        return Err(positioned_if_available(
            ParseError::FeatureNotSupportedMessage(
                "cycle mark column name and cycle path column name are the same".into(),
            ),
            cycle.location,
        ));
    }
    Ok(())
}

fn validate_cte_cycle_clause_types(
    cycle: &CteCycleClause,
    catalog: &dyn CatalogLookup,
) -> Result<(), ParseError> {
    let scope = empty_scope();
    let outer_scopes = Vec::new();
    let mark_value = cycle_mark_value(cycle);
    let default_value = cycle_default_value(cycle);
    let mark_type = infer_sql_expr_type(&mark_value, &scope, catalog, &outer_scopes, None);
    let default_type = infer_sql_expr_type(&default_value, &scope, catalog, &outer_scopes, None);
    let common_type = resolve_common_scalar_type(mark_type, default_type).ok_or_else(|| {
        positioned_if_available(
            ParseError::FeatureNotSupportedMessage(format!(
                "CYCLE types {} and {} cannot be matched",
                sql_type_name(mark_type),
                sql_type_name(default_type)
            )),
            cycle.location,
        )
    })?;
    if !cycle_mark_type_has_equality(catalog, common_type) {
        return Err(ParseError::FeatureNotSupportedMessage(format!(
            "could not identify an equality operator for type {}",
            sql_type_name(common_type)
        )));
    }
    Ok(())
}

fn cycle_mark_type_has_equality(catalog: &dyn CatalogLookup, sql_type: SqlType) -> bool {
    if matches!(sql_type.kind, SqlTypeKind::Point) && !sql_type.is_array {
        return false;
    }
    supports_comparison_operator(catalog, "=", sql_type, sql_type)
}

fn name_in_list(names: &[&str], needle: &str) -> bool {
    names.iter().any(|name| name.eq_ignore_ascii_case(needle))
}

pub(crate) fn cte_body_references_table(body: &CteBody, table_name: &str) -> bool {
    match body {
        CteBody::Select(select) => select_statement_references_table(select, table_name),
        CteBody::Values(values) => values
            .rows
            .iter()
            .flatten()
            .any(|expr| sql_expr_references_table(expr, table_name)),
        CteBody::Insert(insert) => insert_statement_references_table(insert, table_name),
        CteBody::Update(update) => update_statement_references_table(update, table_name),
        CteBody::Delete(delete) => delete_statement_references_table(delete, table_name),
        CteBody::Merge(merge) => merge_statement_references_table(merge, table_name),
        CteBody::RecursiveUnion {
            anchor, recursive, ..
        } => {
            cte_body_references_table(anchor, table_name)
                || select_statement_references_table(recursive, table_name)
        }
    }
}

fn recursive_union_top_level_with_names(
    anchor: &CteBody,
    recursive: &SelectStatement,
    anchor_with_is_subquery: bool,
) -> Vec<String> {
    let anchor_names = cte_body_top_level_with_names(anchor);
    let mut names = if anchor_with_is_subquery {
        anchor_names.clone()
    } else {
        Vec::new()
    };
    for anchor_name in anchor_names.into_iter().filter(|anchor_name| {
        recursive
            .with
            .iter()
            .any(|cte| cte.name.eq_ignore_ascii_case(anchor_name))
    }) {
        if !names
            .iter()
            .any(|candidate: &String| candidate.eq_ignore_ascii_case(&anchor_name))
        {
            names.push(anchor_name);
        }
    }
    names
}

fn cte_body_top_level_with_names(body: &CteBody) -> Vec<String> {
    match body {
        CteBody::Select(select) => select.with.iter().map(|cte| cte.name.clone()).collect(),
        CteBody::Values(values) => values.with.iter().map(|cte| cte.name.clone()).collect(),
        CteBody::Insert(insert) => insert.with.iter().map(|cte| cte.name.clone()).collect(),
        CteBody::Update(update) => update.with.iter().map(|cte| cte.name.clone()).collect(),
        CteBody::Delete(delete) => delete.with.iter().map(|cte| cte.name.clone()).collect(),
        CteBody::Merge(merge) => merge.with.iter().map(|cte| cte.name.clone()).collect(),
        CteBody::RecursiveUnion { anchor, .. } => cte_body_top_level_with_names(anchor),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecursiveReferenceContext {
    Ok,
    NonRecursiveTerm,
    Subquery,
    OuterJoin,
    Intersect,
    Except,
}

fn recursive_reference_error(
    context: RecursiveReferenceContext,
    cte_name: &str,
    position: Option<usize>,
) -> Result<(), ParseError> {
    let message = match context {
        RecursiveReferenceContext::Ok => return Ok(()),
        RecursiveReferenceContext::NonRecursiveTerm => format!(
            "recursive reference to query \"{cte_name}\" must not appear within its non-recursive term"
        ),
        RecursiveReferenceContext::Subquery => {
            format!("recursive reference to query \"{cte_name}\" must not appear within a subquery")
        }
        RecursiveReferenceContext::OuterJoin => {
            format!(
                "recursive reference to query \"{cte_name}\" must not appear within an outer join"
            )
        }
        RecursiveReferenceContext::Intersect => {
            format!("recursive reference to query \"{cte_name}\" must not appear within INTERSECT")
        }
        RecursiveReferenceContext::Except => {
            format!("recursive reference to query \"{cte_name}\" must not appear within EXCEPT")
        }
    };
    Err(positioned_if_available(
        ParseError::InvalidRecursion(message),
        position,
    ))
}

#[derive(Debug)]
struct RecursiveReferenceChecker<'a> {
    cte_name: &'a str,
    self_refcount: usize,
    top_level_with_names: Vec<String>,
}

impl<'a> RecursiveReferenceChecker<'a> {
    fn new(cte_name: &'a str) -> Self {
        Self {
            cte_name,
            self_refcount: 0,
            top_level_with_names: Vec::new(),
        }
    }

    fn with_top_level_with_names(mut self, names: &[String]) -> Self {
        self.top_level_with_names = names.to_vec();
        self
    }

    fn is_top_level_recursive_union_with(&self, name: &str) -> bool {
        self.top_level_with_names
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(name))
    }

    fn validate_recursive_term(&mut self, stmt: &SelectStatement) -> Result<(), ParseError> {
        self.visit_select(stmt, RecursiveReferenceContext::Ok)
    }

    fn validate_non_recursive_term(&mut self, body: &CteBody) -> Result<(), ParseError> {
        self.visit_cte_body(body, RecursiveReferenceContext::NonRecursiveTerm)
    }

    fn visit_cte_body(
        &mut self,
        body: &CteBody,
        context: RecursiveReferenceContext,
    ) -> Result<(), ParseError> {
        match body {
            CteBody::Select(select) => self.visit_select(select, context),
            CteBody::Values(values) => {
                for row in &values.rows {
                    for expr in row {
                        self.visit_expr(expr, context)?;
                    }
                }
                Ok(())
            }
            CteBody::Insert(insert) => {
                if let InsertSource::Select(select) = &insert.source {
                    self.visit_select(select, context)?;
                }
                if let InsertSource::Values(rows) = &insert.source {
                    for row in rows {
                        for expr in row {
                            self.visit_expr(expr, context)?;
                        }
                    }
                }
                for item in &insert.returning {
                    self.visit_expr(&item.expr, context)?;
                }
                Ok(())
            }
            CteBody::Update(update) => {
                for cte in &update.with {
                    self.visit_cte_body(&cte.body, RecursiveReferenceContext::Subquery)?;
                }
                self.note_reference(&update.table_name, context, None)?;
                if let Some(from) = &update.from {
                    self.visit_from(from, context)?;
                }
                for assignment in &update.assignments {
                    self.visit_expr(&assignment.expr, context)?;
                }
                if let Some(where_clause) = &update.where_clause {
                    self.visit_expr(where_clause, context)?;
                }
                for item in &update.returning {
                    self.visit_expr(&item.expr, context)?;
                }
                Ok(())
            }
            CteBody::Delete(delete) => {
                for cte in &delete.with {
                    self.visit_cte_body(&cte.body, RecursiveReferenceContext::Subquery)?;
                }
                self.note_reference(&delete.table_name, context, None)?;
                if let Some(using) = &delete.using {
                    self.visit_from(using, context)?;
                }
                if let Some(where_clause) = &delete.where_clause {
                    self.visit_expr(where_clause, context)?;
                }
                for item in &delete.returning {
                    self.visit_expr(&item.expr, context)?;
                }
                Ok(())
            }
            CteBody::Merge(merge) => {
                for cte in &merge.with {
                    self.visit_cte_body(&cte.body, RecursiveReferenceContext::Subquery)?;
                }
                self.note_reference(&merge.target_table, context, None)?;
                self.visit_from(&merge.source, context)?;
                self.visit_expr(&merge.join_condition, context)?;
                for clause in &merge.when_clauses {
                    if let Some(condition) = &clause.condition {
                        self.visit_expr(condition, context)?;
                    }
                    match &clause.action {
                        MergeAction::Update { assignments } => {
                            for assignment in assignments {
                                self.visit_expr(&assignment.expr, context)?;
                            }
                        }
                        MergeAction::Insert { source, .. } => {
                            if let MergeInsertSource::Values(values) = source {
                                for expr in values {
                                    self.visit_expr(expr, context)?;
                                }
                            }
                        }
                        MergeAction::Delete | MergeAction::DoNothing => {}
                    }
                }
                for item in &merge.returning {
                    self.visit_expr(&item.expr, context)?;
                }
                Ok(())
            }
            CteBody::RecursiveUnion {
                anchor, recursive, ..
            } => {
                self.visit_cte_body(anchor, context)?;
                self.visit_select(recursive, context)
            }
        }
    }

    fn visit_select(
        &mut self,
        stmt: &SelectStatement,
        context: RecursiveReferenceContext,
    ) -> Result<(), ParseError> {
        if stmt.with.iter().any(|cte| cte.name == self.cte_name) {
            return Ok(());
        }

        for cte in &stmt.with {
            let cte_context = if context == RecursiveReferenceContext::NonRecursiveTerm
                && self.is_top_level_recursive_union_with(&cte.name)
            {
                RecursiveReferenceContext::Subquery
            } else {
                context
            };
            self.visit_cte_body(&cte.body, cte_context)?;
        }
        if let Some(from) = &stmt.from {
            self.visit_from(from, context)?;
        }
        for target in &stmt.targets {
            self.visit_expr(&target.expr, context)?;
        }
        if let Some(where_clause) = &stmt.where_clause {
            self.visit_expr(where_clause, context)?;
        }
        for item in &stmt.group_by {
            self.visit_group_by_item(item, context)?;
        }
        if let Some(having) = &stmt.having {
            self.visit_expr(having, context)?;
        }
        for item in &stmt.order_by {
            self.visit_expr(&item.expr, context)?;
        }
        if let Some(set_operation) = &stmt.set_operation {
            self.visit_set_operation(set_operation, context)?;
        }
        Ok(())
    }

    fn visit_group_by_item(
        &mut self,
        item: &GroupByItem,
        context: RecursiveReferenceContext,
    ) -> Result<(), ParseError> {
        match item {
            GroupByItem::Expr(expr) => self.visit_expr(expr, context)?,
            GroupByItem::List(exprs) => {
                for expr in exprs {
                    self.visit_expr(expr, context)?;
                }
            }
            GroupByItem::Empty => {}
            GroupByItem::Rollup(items) | GroupByItem::Cube(items) | GroupByItem::Sets(items) => {
                for item in items {
                    self.visit_group_by_item(item, context)?;
                }
            }
        }
        Ok(())
    }

    fn visit_set_operation(
        &mut self,
        stmt: &SetOperationStatement,
        context: RecursiveReferenceContext,
    ) -> Result<(), ParseError> {
        if matches!(stmt.op, SetOperator::Intersect { .. })
            && stmt
                .inputs
                .iter()
                .filter(|input| select_statement_references_table(input, self.cte_name))
                .take(2)
                .count()
                > 1
        {
            let position = stmt
                .inputs
                .iter()
                .flat_map(|input| select_statement_table_reference_locations(input, self.cte_name))
                .nth(1);
            return Err(positioned_if_available(
                ParseError::InvalidRecursion(format!(
                    "recursive reference to query \"{}\" must not appear more than once",
                    self.cte_name
                )),
                position,
            ));
        }
        let input_context = match stmt.op {
            SetOperator::Union { .. } => context,
            SetOperator::Intersect { .. } => RecursiveReferenceContext::Intersect,
            SetOperator::Except { .. } => RecursiveReferenceContext::Except,
        };
        for input in &stmt.inputs {
            self.visit_select(input, input_context)?;
        }
        Ok(())
    }

    fn visit_from(
        &mut self,
        from: &FromItem,
        context: RecursiveReferenceContext,
    ) -> Result<(), ParseError> {
        match from {
            FromItem::Table { name, location, .. } => self.note_reference(name, context, *location),
            FromItem::Values { rows } => {
                for row in rows {
                    for expr in row {
                        self.visit_expr(expr, context)?;
                    }
                }
                Ok(())
            }
            FromItem::Expression { expr, .. } => self.visit_expr(expr, context),
            FromItem::FunctionCall { args, .. } => {
                for arg in args {
                    self.visit_expr(&arg.value, context)?;
                }
                Ok(())
            }
            FromItem::TableSample { source, sample } => {
                self.visit_from(source, context)?;
                for arg in &sample.args {
                    self.visit_expr(arg, context)?;
                }
                if let Some(repeatable) = &sample.repeatable {
                    self.visit_expr(repeatable, context)?;
                }
                Ok(())
            }
            FromItem::RowsFrom { functions, .. } => {
                for function in functions {
                    for arg in &function.args {
                        self.visit_expr(&arg.value, context)?;
                    }
                }
                Ok(())
            }
            FromItem::JsonTable(table) => {
                self.visit_expr(&table.context, context)?;
                for arg in &table.passing {
                    self.visit_expr(&arg.expr, context)?;
                }
                for column in &table.columns {
                    self.visit_json_table_column(column, context)?;
                }
                if let Some(JsonTableBehavior::Default(expr)) = &table.on_error {
                    self.visit_expr(expr, context)?;
                }
                Ok(())
            }
            FromItem::XmlTable(table) => {
                for namespace in &table.namespaces {
                    self.visit_expr(&namespace.uri, context)?;
                }
                self.visit_expr(&table.row_path, context)?;
                self.visit_expr(&table.document, context)?;
                for column in &table.columns {
                    self.visit_xml_table_column(column, context)?;
                }
                Ok(())
            }
            FromItem::Lateral(source) | FromItem::Alias { source, .. } => {
                self.visit_from(source, context)
            }
            FromItem::DerivedTable(select) => self.visit_select(select, context),
            FromItem::Join {
                left,
                right,
                kind,
                constraint,
            } => {
                let left_context = match kind {
                    JoinKind::Right | JoinKind::Full
                        if context == RecursiveReferenceContext::Ok =>
                    {
                        RecursiveReferenceContext::OuterJoin
                    }
                    _ => context,
                };
                let right_context = match kind {
                    JoinKind::Left | JoinKind::Full if context == RecursiveReferenceContext::Ok => {
                        RecursiveReferenceContext::OuterJoin
                    }
                    _ => context,
                };
                self.visit_from(left, left_context)?;
                self.visit_from(right, right_context)?;
                match constraint {
                    JoinConstraint::None | JoinConstraint::Natural | JoinConstraint::Using(_) => {
                        Ok(())
                    }
                    JoinConstraint::On(expr) => self.visit_expr(expr, context),
                }
            }
        }
    }

    fn visit_json_table_column(
        &mut self,
        column: &JsonTableColumn,
        context: RecursiveReferenceContext,
    ) -> Result<(), ParseError> {
        match column {
            JsonTableColumn::Regular {
                on_empty, on_error, ..
            } => {
                if let Some(JsonTableBehavior::Default(expr)) = on_empty {
                    self.visit_expr(expr, context)?;
                }
                if let Some(JsonTableBehavior::Default(expr)) = on_error {
                    self.visit_expr(expr, context)?;
                }
            }
            JsonTableColumn::Nested { columns, .. } => {
                for column in columns {
                    self.visit_json_table_column(column, context)?;
                }
            }
            JsonTableColumn::Ordinality { .. } | JsonTableColumn::Exists { .. } => {}
        }
        Ok(())
    }

    fn visit_xml_table_column(
        &mut self,
        column: &XmlTableColumn,
        context: RecursiveReferenceContext,
    ) -> Result<(), ParseError> {
        match column {
            XmlTableColumn::Regular { path, default, .. } => {
                if let Some(expr) = path {
                    self.visit_expr(expr, context)?;
                }
                if let Some(expr) = default {
                    self.visit_expr(expr, context)?;
                }
            }
            XmlTableColumn::Ordinality { .. } => {}
        }
        Ok(())
    }

    fn visit_expr(
        &mut self,
        expr: &SqlExpr,
        context: RecursiveReferenceContext,
    ) -> Result<(), ParseError> {
        match expr {
            SqlExpr::Column(_)
            | SqlExpr::Parameter(_)
            | SqlExpr::ParamRef(_)
            | SqlExpr::Default
            | SqlExpr::Const(_)
            | SqlExpr::IntegerLiteral(_)
            | SqlExpr::NumericLiteral(_)
            | SqlExpr::Random
            | SqlExpr::CurrentDate
            | SqlExpr::CurrentCatalog
            | SqlExpr::CurrentSchema
            | SqlExpr::CurrentUser
            | SqlExpr::SessionUser
            | SqlExpr::User
            | SqlExpr::SystemUser
            | SqlExpr::CurrentRole
            | SqlExpr::CurrentTime { .. }
            | SqlExpr::CurrentTimestamp { .. }
            | SqlExpr::LocalTime { .. }
            | SqlExpr::LocalTimestamp { .. } => Ok(()),
            SqlExpr::Collate { expr, .. } => self.visit_expr(expr, context),
            SqlExpr::UnaryPlus(expr)
            | SqlExpr::Negate(expr)
            | SqlExpr::BitNot(expr)
            | SqlExpr::Subscript { expr, .. }
            | SqlExpr::PrefixOperator { expr, .. }
            | SqlExpr::Cast(expr, _)
            | SqlExpr::Not(expr)
            | SqlExpr::IsNull(expr)
            | SqlExpr::IsNotNull(expr)
            | SqlExpr::FieldSelect { expr, .. }
            | SqlExpr::GeometryUnaryOp { expr, .. } => self.visit_expr(expr, context),
            SqlExpr::Add(left, right)
            | SqlExpr::Sub(left, right)
            | SqlExpr::BitAnd(left, right)
            | SqlExpr::BitOr(left, right)
            | SqlExpr::BitXor(left, right)
            | SqlExpr::Shl(left, right)
            | SqlExpr::Shr(left, right)
            | SqlExpr::Mul(left, right)
            | SqlExpr::Div(left, right)
            | SqlExpr::Mod(left, right)
            | SqlExpr::Concat(left, right)
            | SqlExpr::Eq(left, right)
            | SqlExpr::NotEq(left, right)
            | SqlExpr::Lt(left, right)
            | SqlExpr::LtEq(left, right)
            | SqlExpr::Gt(left, right)
            | SqlExpr::GtEq(left, right)
            | SqlExpr::RegexMatch(left, right)
            | SqlExpr::And(left, right)
            | SqlExpr::Or(left, right)
            | SqlExpr::IsDistinctFrom(left, right)
            | SqlExpr::IsNotDistinctFrom(left, right)
            | SqlExpr::Overlaps(left, right)
            | SqlExpr::ArrayOverlap(left, right)
            | SqlExpr::ArrayContains(left, right)
            | SqlExpr::ArrayContained(left, right)
            | SqlExpr::JsonbContains(left, right)
            | SqlExpr::JsonbContained(left, right)
            | SqlExpr::JsonbExists(left, right)
            | SqlExpr::JsonbExistsAny(left, right)
            | SqlExpr::JsonbExistsAll(left, right)
            | SqlExpr::JsonbPathExists(left, right)
            | SqlExpr::JsonbPathMatch(left, right)
            | SqlExpr::JsonGet(left, right)
            | SqlExpr::JsonGetText(left, right)
            | SqlExpr::JsonPath(left, right)
            | SqlExpr::JsonPathText(left, right)
            | SqlExpr::AtTimeZone {
                expr: left,
                zone: right,
            }
            | SqlExpr::GeometryBinaryOp { left, right, .. } => {
                self.visit_expr(left, context)?;
                self.visit_expr(right, context)
            }
            SqlExpr::BinaryOperator { left, right, .. } => {
                self.visit_expr(left, context)?;
                self.visit_expr(right, context)
            }
            SqlExpr::Like {
                expr,
                pattern,
                escape,
                ..
            }
            | SqlExpr::Similar {
                expr,
                pattern,
                escape,
                ..
            } => {
                self.visit_expr(expr, context)?;
                self.visit_expr(pattern, context)?;
                if let Some(escape) = escape {
                    self.visit_expr(escape, context)?;
                }
                Ok(())
            }
            SqlExpr::Case {
                arg,
                args,
                defresult,
            } => {
                if let Some(arg) = arg {
                    self.visit_expr(arg, context)?;
                }
                for arm in args {
                    self.visit_expr(&arm.expr, context)?;
                    self.visit_expr(&arm.result, context)?;
                }
                if let Some(defresult) = defresult {
                    self.visit_expr(defresult, context)?;
                }
                Ok(())
            }
            SqlExpr::ArrayLiteral(items) | SqlExpr::Row(items) => {
                for item in items {
                    self.visit_expr(item, context)?;
                }
                Ok(())
            }
            SqlExpr::FuncCall {
                args,
                order_by,
                filter,
                over,
                ..
            } => {
                for arg in args.args() {
                    self.visit_expr(&arg.value, context)?;
                }
                for item in order_by {
                    self.visit_expr(&item.expr, context)?;
                }
                if let Some(filter) = filter {
                    self.visit_expr(filter, context)?;
                }
                if let Some(over) = over {
                    for expr in &over.partition_by {
                        self.visit_expr(expr, context)?;
                    }
                    for item in &over.order_by {
                        self.visit_expr(&item.expr, context)?;
                    }
                }
                Ok(())
            }
            SqlExpr::ScalarSubquery(subquery)
            | SqlExpr::ArraySubquery(subquery)
            | SqlExpr::Exists(subquery) => {
                self.visit_select(subquery, RecursiveReferenceContext::Subquery)
            }
            SqlExpr::InSubquery {
                expr,
                subquery,
                negated: _,
            } => {
                self.visit_expr(expr, context)?;
                self.visit_select(subquery, RecursiveReferenceContext::Subquery)
            }
            SqlExpr::QuantifiedSubquery { left, subquery, .. } => {
                self.visit_expr(left, context)?;
                self.visit_select(subquery, RecursiveReferenceContext::Subquery)
            }
            SqlExpr::QuantifiedArray { left, array, .. } => {
                self.visit_expr(left, context)?;
                self.visit_expr(array, context)
            }
            SqlExpr::ArraySubscript { array, subscripts } => {
                self.visit_expr(array, context)?;
                for subscript in subscripts {
                    if let Some(lower) = &subscript.lower {
                        self.visit_expr(lower, context)?;
                    }
                    if let Some(upper) = &subscript.upper {
                        self.visit_expr(upper, context)?;
                    }
                }
                Ok(())
            }
            SqlExpr::Xml(xml) => {
                for child in xml.child_exprs() {
                    self.visit_expr(child, context)?;
                }
                Ok(())
            }
            SqlExpr::JsonQueryFunction(func) => {
                for child in func.child_exprs() {
                    self.visit_expr(child, context)?;
                }
                Ok(())
            }
        }
    }

    fn note_reference(
        &mut self,
        table_name: &str,
        context: RecursiveReferenceContext,
        position: Option<usize>,
    ) -> Result<(), ParseError> {
        if !table_name.eq_ignore_ascii_case(self.cte_name) {
            return Ok(());
        }
        recursive_reference_error(context, self.cte_name, position)?;
        self.self_refcount += 1;
        if self.self_refcount > 1 {
            return Err(positioned_if_available(
                ParseError::InvalidRecursion(format!(
                    "recursive reference to query \"{}\" must not appear more than once",
                    self.cte_name
                )),
                position,
            ));
        }
        Ok(())
    }
}

fn validate_recursive_cte_recursive_term(
    stmt: &SelectStatement,
    cte_name: &str,
) -> Result<(), ParseError> {
    validate_recursive_cte_recursive_term_decorations(stmt)?;
    RecursiveReferenceChecker::new(cte_name).validate_recursive_term(stmt)
}

fn validate_recursive_cte_non_recursive_term(
    body: &CteBody,
    cte_name: &str,
    top_level_with_names: &[String],
) -> Result<(), ParseError> {
    RecursiveReferenceChecker::new(cte_name)
        .with_top_level_with_names(top_level_with_names)
        .validate_non_recursive_term(body)
}

fn validate_recursive_term_target_operator_errors(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    visible_agg_scope: Option<&VisibleAggregateScope>,
    outer_ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<(), ParseError> {
    if stmt.set_operation.is_some() {
        return Ok(());
    }
    with_visible_aggregate_scope(visible_agg_scope.cloned(), || {
        let local_ctes = match bind_ctes(
            stmt.with_recursive,
            &stmt.with,
            catalog,
            outer_scopes,
            grouped_outer.clone(),
            outer_ctes,
            expanded_views,
        ) {
            Ok(ctes) => ctes,
            Err(err) if parse_error_is_undefined_operator(&err) => return Err(err),
            Err(_) => return Ok(()),
        };
        let mut visible_ctes = local_ctes;
        visible_ctes.extend_from_slice(outer_ctes);
        let scope = match &stmt.from {
            Some(from) => match bind_from_item_with_ctes(
                from,
                catalog,
                outer_scopes,
                grouped_outer.as_ref(),
                &visible_ctes,
                expanded_views,
            ) {
                Ok((_, scope)) => scope,
                Err(err) if parse_error_is_undefined_operator(&err) => return Err(err),
                Err(_) => return Ok(()),
            },
            None => empty_scope(),
        };
        for target in &stmt.targets {
            match bind_typed_expr_with_outer_and_ctes(
                &target.expr,
                &scope,
                catalog,
                outer_scopes,
                grouped_outer.as_ref(),
                &visible_ctes,
            ) {
                Ok(_) => {}
                Err(err) if parse_error_is_undefined_operator(&err) => return Err(err),
                Err(_) => {}
            }
        }
        Ok(())
    })
}

fn parse_error_is_undefined_operator(err: &ParseError) -> bool {
    match err {
        ParseError::UndefinedOperator { .. } => true,
        ParseError::Positioned { source, .. } | ParseError::WithContext { source, .. } => {
            parse_error_is_undefined_operator(source)
        }
        _ => false,
    }
}

fn validate_recursive_cte_recursive_term_decorations(
    stmt: &SelectStatement,
) -> Result<(), ParseError> {
    if let Some(position) = select_statement_recursive_term_aggregate_position(stmt) {
        return Err(ParseError::FeatureNotSupportedMessage(
            "aggregate functions are not allowed in a recursive query's recursive term".into(),
        )
        .with_position(position));
    }
    if !stmt.order_by.is_empty() {
        let position = stmt
            .order_by
            .first()
            .and_then(|item| item.location)
            .or(stmt.order_by_location);
        return Err(positioned_if_available(
            ParseError::FeatureNotSupportedMessage(
                "ORDER BY in a recursive query is not implemented".into(),
            ),
            position,
        ));
    }
    if stmt.offset.is_some() {
        return Err(positioned_if_available(
            ParseError::FeatureNotSupportedMessage(
                "OFFSET in a recursive query is not implemented".into(),
            ),
            stmt.offset_location,
        ));
    }
    if stmt.limit.is_some() {
        return Err(positioned_if_available(
            ParseError::FeatureNotSupportedMessage(
                "LIMIT in a recursive query is not implemented".into(),
            ),
            stmt.limit_location,
        ));
    }
    if stmt.locking_clause.is_some() {
        return Err(positioned_if_available(
            ParseError::FeatureNotSupportedMessage(
                "FOR UPDATE/SHARE in a recursive query is not implemented".into(),
            ),
            stmt.locking_location,
        ));
    }
    Ok(())
}

fn select_statement_recursive_term_aggregate_position(stmt: &SelectStatement) -> Option<usize> {
    stmt.targets
        .iter()
        .find(|target| sql_expr_contains_aggregate_call(&target.expr))
        .and_then(|target| target.location)
        .or_else(|| {
            stmt.having
                .as_ref()
                .filter(|expr| sql_expr_contains_aggregate_call(expr))
                .map(|_| stmt.targets.first().and_then(|target| target.location))
                .flatten()
        })
        .or_else(|| {
            stmt.order_by
                .iter()
                .find(|item| sql_expr_contains_aggregate_call(&item.expr))
                .and_then(|item| item.location)
        })
        .or_else(|| {
            stmt.set_operation.as_ref().and_then(|setop| {
                setop
                    .inputs
                    .iter()
                    .find_map(select_statement_recursive_term_aggregate_position)
            })
        })
}

fn sql_expr_contains_aggregate_call(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Column(_)
        | SqlExpr::Parameter(_)
        | SqlExpr::ParamRef(_)
        | SqlExpr::Default
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentCatalog
        | SqlExpr::CurrentSchema
        | SqlExpr::CurrentUser
        | SqlExpr::SessionUser
        | SqlExpr::User
        | SqlExpr::SystemUser
        | SqlExpr::CurrentRole
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. } => false,
        SqlExpr::Collate { expr, .. }
        | SqlExpr::UnaryPlus(expr)
        | SqlExpr::Negate(expr)
        | SqlExpr::BitNot(expr)
        | SqlExpr::Subscript { expr, .. }
        | SqlExpr::PrefixOperator { expr, .. }
        | SqlExpr::Cast(expr, _)
        | SqlExpr::Not(expr)
        | SqlExpr::IsNull(expr)
        | SqlExpr::IsNotNull(expr)
        | SqlExpr::FieldSelect { expr, .. }
        | SqlExpr::GeometryUnaryOp { expr, .. } => sql_expr_contains_aggregate_call(expr),
        SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::BitAnd(left, right)
        | SqlExpr::BitOr(left, right)
        | SqlExpr::BitXor(left, right)
        | SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::Mod(left, right)
        | SqlExpr::Concat(left, right)
        | SqlExpr::Eq(left, right)
        | SqlExpr::NotEq(left, right)
        | SqlExpr::Lt(left, right)
        | SqlExpr::LtEq(left, right)
        | SqlExpr::Gt(left, right)
        | SqlExpr::GtEq(left, right)
        | SqlExpr::RegexMatch(left, right)
        | SqlExpr::And(left, right)
        | SqlExpr::Or(left, right)
        | SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right)
        | SqlExpr::Overlaps(left, right)
        | SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right)
        | SqlExpr::JsonbExists(left, right)
        | SqlExpr::JsonbExistsAny(left, right)
        | SqlExpr::JsonbExistsAll(left, right)
        | SqlExpr::JsonbPathExists(left, right)
        | SqlExpr::JsonbPathMatch(left, right)
        | SqlExpr::JsonGet(left, right)
        | SqlExpr::JsonGetText(left, right)
        | SqlExpr::JsonPath(left, right)
        | SqlExpr::JsonPathText(left, right)
        | SqlExpr::GeometryBinaryOp { left, right, .. } => {
            sql_expr_contains_aggregate_call(left) || sql_expr_contains_aggregate_call(right)
        }
        SqlExpr::AtTimeZone { expr, zone } => {
            sql_expr_contains_aggregate_call(expr) || sql_expr_contains_aggregate_call(zone)
        }
        SqlExpr::BinaryOperator { left, right, .. } => {
            sql_expr_contains_aggregate_call(left) || sql_expr_contains_aggregate_call(right)
        }
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            sql_expr_contains_aggregate_call(expr)
                || sql_expr_contains_aggregate_call(pattern)
                || escape
                    .as_ref()
                    .is_some_and(|expr| sql_expr_contains_aggregate_call(expr))
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            arg.as_ref()
                .is_some_and(|expr| sql_expr_contains_aggregate_call(expr))
                || args.iter().any(|arm| {
                    sql_expr_contains_aggregate_call(&arm.expr)
                        || sql_expr_contains_aggregate_call(&arm.result)
                })
                || defresult
                    .as_ref()
                    .is_some_and(|expr| sql_expr_contains_aggregate_call(expr))
        }
        SqlExpr::ArrayLiteral(items) | SqlExpr::Row(items) => {
            items.iter().any(sql_expr_contains_aggregate_call)
        }
        SqlExpr::FuncCall {
            name,
            args,
            order_by,
            within_group,
            filter,
            over,
            ..
        } => {
            is_builtin_aggregate_name(name)
                || args
                    .args()
                    .iter()
                    .any(|arg| sql_expr_contains_aggregate_call(&arg.value))
                || order_by
                    .iter()
                    .any(|item| sql_expr_contains_aggregate_call(&item.expr))
                || within_group.as_ref().is_some_and(|items| {
                    items
                        .iter()
                        .any(|item| sql_expr_contains_aggregate_call(&item.expr))
                })
                || filter
                    .as_ref()
                    .is_some_and(|expr| sql_expr_contains_aggregate_call(expr))
                || over.as_ref().is_some_and(|over| {
                    over.partition_by
                        .iter()
                        .any(sql_expr_contains_aggregate_call)
                        || over
                            .order_by
                            .iter()
                            .any(|item| sql_expr_contains_aggregate_call(&item.expr))
                })
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            sql_expr_contains_aggregate_call(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(|expr| sql_expr_contains_aggregate_call(expr))
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(|expr| sql_expr_contains_aggregate_call(expr))
                })
        }
        SqlExpr::QuantifiedArray { left, array, .. } => {
            sql_expr_contains_aggregate_call(left) || sql_expr_contains_aggregate_call(array)
        }
        SqlExpr::InSubquery { expr, .. } | SqlExpr::QuantifiedSubquery { left: expr, .. } => {
            sql_expr_contains_aggregate_call(expr)
        }
        SqlExpr::ScalarSubquery(_)
        | SqlExpr::ArraySubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::Xml(_)
        | SqlExpr::JsonQueryFunction(_) => false,
    }
}

fn is_builtin_aggregate_name(name: &str) -> bool {
    let unqualified = name.rsplit('.').next().unwrap_or(name);
    matches!(
        unqualified.to_ascii_lowercase().as_str(),
        "array_agg"
            | "avg"
            | "bit_and"
            | "bit_or"
            | "bool_and"
            | "bool_or"
            | "count"
            | "every"
            | "json_agg"
            | "json_object_agg"
            | "jsonb_agg"
            | "jsonb_object_agg"
            | "max"
            | "min"
            | "string_agg"
            | "sum"
            | "xmlagg"
    )
}

pub(crate) fn select_statement_references_table(stmt: &SelectStatement, table_name: &str) -> bool {
    if stmt
        .with
        .iter()
        .any(|cte| cte.name.eq_ignore_ascii_case(table_name))
    {
        return false;
    }
    stmt.from
        .as_ref()
        .is_some_and(|from| from_item_references_table(from, table_name))
        || stmt
            .with
            .iter()
            .any(|cte| cte_body_references_table(&cte.body, table_name))
        || stmt.set_operation.as_ref().is_some_and(|setop| {
            setop
                .inputs
                .iter()
                .any(|input| select_statement_references_table(input, table_name))
        })
        || stmt
            .targets
            .iter()
            .any(|target| sql_expr_references_table(&target.expr, table_name))
        || stmt
            .where_clause
            .as_ref()
            .is_some_and(|expr| sql_expr_references_table(expr, table_name))
        || stmt
            .group_by
            .iter()
            .any(|item| group_by_item_references_table(item, table_name))
        || stmt
            .having
            .as_ref()
            .is_some_and(|expr| sql_expr_references_table(expr, table_name))
        || stmt
            .order_by
            .iter()
            .any(|item| sql_expr_references_table(&item.expr, table_name))
}

fn select_statement_table_reference_locations(
    stmt: &SelectStatement,
    table_name: &str,
) -> Vec<usize> {
    if stmt
        .with
        .iter()
        .any(|cte| cte.name.eq_ignore_ascii_case(table_name))
    {
        return Vec::new();
    }
    let mut locations = Vec::new();
    if let Some(from) = &stmt.from {
        from_item_table_reference_locations(from, table_name, &mut locations);
    }
    if let Some(setop) = &stmt.set_operation {
        for input in &setop.inputs {
            locations.extend(select_statement_table_reference_locations(
                input, table_name,
            ));
        }
    }
    locations
}

fn cte_body_table_reference_locations(body: &CteBody, table_name: &str) -> Vec<usize> {
    match body {
        CteBody::Select(stmt) => select_statement_table_reference_locations(stmt, table_name),
        CteBody::Values(_) => Vec::new(),
        CteBody::RecursiveUnion {
            anchor, recursive, ..
        } => {
            let mut locations = cte_body_table_reference_locations(anchor, table_name);
            locations.extend(select_statement_table_reference_locations(
                recursive, table_name,
            ));
            locations
        }
        CteBody::Insert(stmt) => insert_statement_table_reference_locations(stmt, table_name),
        CteBody::Update(stmt) => update_statement_table_reference_locations(stmt, table_name),
        CteBody::Delete(stmt) => delete_statement_table_reference_locations(stmt, table_name),
        CteBody::Merge(stmt) => merge_statement_table_reference_locations(stmt, table_name),
    }
}

fn from_item_table_reference_locations(
    from: &FromItem,
    table_name: &str,
    locations: &mut Vec<usize>,
) {
    match from {
        FromItem::Table { name, location, .. } if name.eq_ignore_ascii_case(table_name) => {
            if let Some(location) = location {
                locations.push(*location);
            }
        }
        FromItem::Table { .. }
        | FromItem::Values { .. }
        | FromItem::Expression { .. }
        | FromItem::FunctionCall { .. } => {}
        FromItem::RowsFrom { .. } | FromItem::JsonTable(_) | FromItem::XmlTable(_) => {}
        FromItem::TableSample { source, .. }
        | FromItem::Lateral(source)
        | FromItem::Alias { source, .. } => {
            from_item_table_reference_locations(source, table_name, locations);
        }
        FromItem::DerivedTable(select) => {
            locations.extend(select_statement_table_reference_locations(
                select, table_name,
            ));
        }
        FromItem::Join { left, right, .. } => {
            from_item_table_reference_locations(left, table_name, locations);
            from_item_table_reference_locations(right, table_name, locations);
        }
    }
}

pub(crate) fn insert_statement_references_table(stmt: &InsertStatement, table_name: &str) -> bool {
    stmt.with
        .iter()
        .any(|cte| cte_body_references_table(&cte.body, table_name))
        || match &stmt.source {
            InsertSource::Values(rows) => rows
                .iter()
                .flatten()
                .any(|expr| sql_expr_references_table(expr, table_name)),
            InsertSource::Select(select) => select_statement_references_table(select, table_name),
            InsertSource::DefaultValues => false,
        }
        || stmt
            .returning
            .iter()
            .any(|item| sql_expr_references_table(&item.expr, table_name))
}

fn insert_statement_table_reference_locations(
    stmt: &InsertStatement,
    table_name: &str,
) -> Vec<usize> {
    let mut locations = stmt
        .with
        .iter()
        .flat_map(|cte| cte_body_table_reference_locations(&cte.body, table_name))
        .collect::<Vec<_>>();
    if let InsertSource::Select(select) = &stmt.source {
        locations.extend(select_statement_table_reference_locations(
            select, table_name,
        ));
    }
    locations
}

pub(crate) fn update_statement_references_table(stmt: &UpdateStatement, table_name: &str) -> bool {
    stmt.with
        .iter()
        .any(|cte| cte_body_references_table(&cte.body, table_name))
        || stmt
            .from
            .as_ref()
            .is_some_and(|from| from_item_references_table(from, table_name))
        || stmt
            .assignments
            .iter()
            .any(|assignment| sql_expr_references_table(&assignment.expr, table_name))
        || stmt
            .where_clause
            .as_ref()
            .is_some_and(|expr| sql_expr_references_table(expr, table_name))
        || stmt
            .returning
            .iter()
            .any(|item| sql_expr_references_table(&item.expr, table_name))
}

fn update_statement_table_reference_locations(
    stmt: &UpdateStatement,
    table_name: &str,
) -> Vec<usize> {
    let mut locations = stmt
        .with
        .iter()
        .flat_map(|cte| cte_body_table_reference_locations(&cte.body, table_name))
        .collect::<Vec<_>>();
    if let Some(from) = &stmt.from {
        from_item_table_reference_locations(from, table_name, &mut locations);
    }
    locations
}

pub(crate) fn delete_statement_references_table(stmt: &DeleteStatement, table_name: &str) -> bool {
    stmt.with
        .iter()
        .any(|cte| cte_body_references_table(&cte.body, table_name))
        || stmt.table_name.eq_ignore_ascii_case(table_name)
        || stmt
            .using
            .as_ref()
            .is_some_and(|using| from_item_references_table(using, table_name))
        || stmt
            .where_clause
            .as_ref()
            .is_some_and(|expr| sql_expr_references_table(expr, table_name))
        || stmt
            .returning
            .iter()
            .any(|item| sql_expr_references_table(&item.expr, table_name))
}

fn delete_statement_table_reference_locations(
    stmt: &DeleteStatement,
    table_name: &str,
) -> Vec<usize> {
    let mut locations = stmt
        .with
        .iter()
        .flat_map(|cte| cte_body_table_reference_locations(&cte.body, table_name))
        .collect::<Vec<_>>();
    if let Some(using) = &stmt.using {
        from_item_table_reference_locations(using, table_name, &mut locations);
    }
    locations
}

pub(crate) fn merge_statement_references_table(stmt: &MergeStatement, table_name: &str) -> bool {
    stmt.with
        .iter()
        .any(|cte| cte_body_references_table(&cte.body, table_name))
        || stmt.target_table.eq_ignore_ascii_case(table_name)
        || from_item_references_table(&stmt.source, table_name)
        || sql_expr_references_table(&stmt.join_condition, table_name)
        || stmt.when_clauses.iter().any(|clause| {
            clause
                .condition
                .as_ref()
                .is_some_and(|expr| sql_expr_references_table(expr, table_name))
                || match &clause.action {
                    MergeAction::Update { assignments } => assignments
                        .iter()
                        .any(|assignment| sql_expr_references_table(&assignment.expr, table_name)),
                    MergeAction::Insert { source, .. } => match source {
                        MergeInsertSource::Values(values) => values
                            .iter()
                            .any(|expr| sql_expr_references_table(expr, table_name)),
                        MergeInsertSource::DefaultValues => false,
                    },
                    MergeAction::Delete | MergeAction::DoNothing => false,
                }
        })
        || stmt
            .returning
            .iter()
            .any(|item| sql_expr_references_table(&item.expr, table_name))
}

fn merge_statement_table_reference_locations(
    stmt: &MergeStatement,
    table_name: &str,
) -> Vec<usize> {
    let mut locations = stmt
        .with
        .iter()
        .flat_map(|cte| cte_body_table_reference_locations(&cte.body, table_name))
        .collect::<Vec<_>>();
    from_item_table_reference_locations(&stmt.source, table_name, &mut locations);
    locations
}

fn from_item_references_table(item: &FromItem, table_name: &str) -> bool {
    match item {
        FromItem::Table { name, .. } => name.eq_ignore_ascii_case(table_name),
        FromItem::Lateral(source)
        | FromItem::Alias { source, .. }
        | FromItem::TableSample { source, .. } => from_item_references_table(source, table_name),
        FromItem::DerivedTable(select) => select_statement_references_table(select, table_name),
        FromItem::Join { left, right, .. } => {
            from_item_references_table(left, table_name)
                || from_item_references_table(right, table_name)
        }
        FromItem::JsonTable(table) => json_table_expr_references_table(table, table_name),
        FromItem::XmlTable(table) => xml_table_expr_references_table(table, table_name),
        FromItem::Values { .. }
        | FromItem::Expression { .. }
        | FromItem::FunctionCall { .. }
        | FromItem::RowsFrom { .. } => false,
    }
}

fn json_table_expr_references_table(table: &JsonTableExpr, table_name: &str) -> bool {
    sql_expr_references_table(&table.context, table_name)
        || table
            .passing
            .iter()
            .any(|arg| sql_expr_references_table(&arg.expr, table_name))
        || table
            .columns
            .iter()
            .any(|column| json_table_column_references_table(column, table_name))
        || matches!(
            &table.on_error,
            Some(JsonTableBehavior::Default(expr)) if sql_expr_references_table(expr, table_name)
        )
}

fn json_table_column_references_table(column: &JsonTableColumn, table_name: &str) -> bool {
    match column {
        JsonTableColumn::Regular {
            on_empty, on_error, ..
        } => {
            matches!(
                on_empty,
                Some(JsonTableBehavior::Default(expr)) if sql_expr_references_table(expr, table_name)
            ) || matches!(
                on_error,
                Some(JsonTableBehavior::Default(expr)) if sql_expr_references_table(expr, table_name)
            )
        }
        JsonTableColumn::Nested { columns, .. } => columns
            .iter()
            .any(|column| json_table_column_references_table(column, table_name)),
        JsonTableColumn::Ordinality { .. } | JsonTableColumn::Exists { .. } => false,
    }
}

fn xml_table_expr_references_table(table: &XmlTableExpr, table_name: &str) -> bool {
    table
        .namespaces
        .iter()
        .any(|namespace| sql_expr_references_table(&namespace.uri, table_name))
        || sql_expr_references_table(&table.row_path, table_name)
        || sql_expr_references_table(&table.document, table_name)
        || table
            .columns
            .iter()
            .any(|column| xml_table_column_references_table(column, table_name))
}

fn xml_table_column_references_table(column: &XmlTableColumn, table_name: &str) -> bool {
    match column {
        XmlTableColumn::Regular { path, default, .. } => {
            path.as_ref()
                .is_some_and(|expr| sql_expr_references_table(expr, table_name))
                || default
                    .as_ref()
                    .is_some_and(|expr| sql_expr_references_table(expr, table_name))
        }
        XmlTableColumn::Ordinality { .. } => false,
    }
}

fn group_by_item_references_table(item: &GroupByItem, table_name: &str) -> bool {
    match item {
        GroupByItem::Expr(expr) => sql_expr_references_table(expr, table_name),
        GroupByItem::List(exprs) => exprs
            .iter()
            .any(|expr| sql_expr_references_table(expr, table_name)),
        GroupByItem::Empty => false,
        GroupByItem::Rollup(items) | GroupByItem::Cube(items) | GroupByItem::Sets(items) => items
            .iter()
            .any(|item| group_by_item_references_table(item, table_name)),
    }
}

fn sql_expr_references_table(expr: &SqlExpr, table_name: &str) -> bool {
    match expr {
        SqlExpr::Column(name) => name
            .split('.')
            .rev()
            .skip(1)
            .any(|qualifier| qualifier.eq_ignore_ascii_case(table_name)),
        SqlExpr::Parameter(_)
        | SqlExpr::ParamRef(_)
        | SqlExpr::Default
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentCatalog
        | SqlExpr::CurrentSchema
        | SqlExpr::CurrentUser
        | SqlExpr::SessionUser
        | SqlExpr::User
        | SqlExpr::SystemUser
        | SqlExpr::CurrentRole
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. } => false,
        SqlExpr::Collate { expr: inner, .. } => sql_expr_references_table(inner, table_name),
        SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::PrefixOperator { expr: inner, .. }
        | SqlExpr::Cast(inner, _)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::FieldSelect { expr: inner, .. } => sql_expr_references_table(inner, table_name),
        SqlExpr::Xml(xml) => xml
            .child_exprs()
            .any(|child| sql_expr_references_table(child, table_name)),
        SqlExpr::JsonQueryFunction(func) => func
            .child_exprs()
            .iter()
            .any(|child| sql_expr_references_table(child, table_name)),
        SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::BitAnd(left, right)
        | SqlExpr::BitOr(left, right)
        | SqlExpr::BitXor(left, right)
        | SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::Mod(left, right)
        | SqlExpr::Concat(left, right)
        | SqlExpr::Eq(left, right)
        | SqlExpr::NotEq(left, right)
        | SqlExpr::Lt(left, right)
        | SqlExpr::LtEq(left, right)
        | SqlExpr::Gt(left, right)
        | SqlExpr::GtEq(left, right)
        | SqlExpr::RegexMatch(left, right)
        | SqlExpr::And(left, right)
        | SqlExpr::Or(left, right)
        | SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right)
        | SqlExpr::Overlaps(left, right)
        | SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right)
        | SqlExpr::JsonbExists(left, right)
        | SqlExpr::JsonbExistsAny(left, right)
        | SqlExpr::JsonbExistsAll(left, right)
        | SqlExpr::JsonbPathExists(left, right)
        | SqlExpr::JsonbPathMatch(left, right)
        | SqlExpr::JsonGet(left, right)
        | SqlExpr::JsonGetText(left, right)
        | SqlExpr::JsonPath(left, right)
        | SqlExpr::JsonPathText(left, right)
        | SqlExpr::AtTimeZone {
            expr: left,
            zone: right,
        }
        | SqlExpr::GeometryBinaryOp { left, right, .. } => {
            sql_expr_references_table(left, table_name)
                || sql_expr_references_table(right, table_name)
        }
        SqlExpr::BinaryOperator { left, right, .. } => {
            sql_expr_references_table(left, table_name)
                || sql_expr_references_table(right, table_name)
        }
        SqlExpr::Subscript { expr, .. } | SqlExpr::GeometryUnaryOp { expr, .. } => {
            sql_expr_references_table(expr, table_name)
        }
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            sql_expr_references_table(expr, table_name)
                || sql_expr_references_table(pattern, table_name)
                || escape
                    .as_ref()
                    .is_some_and(|expr| sql_expr_references_table(expr, table_name))
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            arg.as_ref()
                .is_some_and(|expr| sql_expr_references_table(expr, table_name))
                || args.iter().any(|arm| {
                    sql_expr_references_table(&arm.expr, table_name)
                        || sql_expr_references_table(&arm.result, table_name)
                })
                || defresult
                    .as_ref()
                    .is_some_and(|expr| sql_expr_references_table(expr, table_name))
        }
        SqlExpr::ArrayLiteral(items) | SqlExpr::Row(items) => items
            .iter()
            .any(|item| sql_expr_references_table(item, table_name)),
        SqlExpr::FuncCall { args, order_by, .. } => {
            args.args()
                .iter()
                .any(|arg| sql_expr_references_table(&arg.value, table_name))
                || order_by
                    .iter()
                    .any(|item| sql_expr_references_table(&item.expr, table_name))
        }
        SqlExpr::ScalarSubquery(subquery)
        | SqlExpr::ArraySubquery(subquery)
        | SqlExpr::Exists(subquery) => select_statement_references_table(subquery, table_name),
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated: _,
        } => {
            sql_expr_references_table(expr, table_name)
                || select_statement_references_table(subquery, table_name)
        }
        SqlExpr::QuantifiedSubquery { left, subquery, .. } => {
            sql_expr_references_table(left, table_name)
                || select_statement_references_table(subquery, table_name)
        }
        SqlExpr::QuantifiedArray { left, array, .. } => {
            sql_expr_references_table(left, table_name)
                || sql_expr_references_table(array, table_name)
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            sql_expr_references_table(array, table_name)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(|expr| sql_expr_references_table(expr, table_name))
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(|expr| sql_expr_references_table(expr, table_name))
                })
        }
    }
}

pub fn bind_create_table(
    stmt: &CreateTableStatement,
    catalog: &mut Catalog,
) -> Result<CatalogEntry, ParseError> {
    let (table_name, _) = normalize_create_table_name(stmt)?;
    catalog
        .create_table(table_name, create_relation_desc(stmt, catalog)?)
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
            crate::backend::catalog::catalog::CatalogError::TypeAlreadyExists(_) => {
                ParseError::UnexpectedToken {
                    expected: "valid catalog state",
                    actual: "catalog error".into(),
                }
            }
            crate::backend::catalog::catalog::CatalogError::Io(_)
            | crate::backend::catalog::catalog::CatalogError::Corrupt(_)
            | crate::backend::catalog::catalog::CatalogError::Interrupted(_) => {
                ParseError::UnexpectedToken {
                    expected: "valid catalog state",
                    actual: "catalog error".into(),
                }
            }
        })
}

pub fn pg_plan_query(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
) -> Result<PlannedStmt, ParseError> {
    pg_plan_query_with_config(stmt, catalog, PlannerConfig::default())
}

pub fn pg_plan_query_with_config(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> Result<PlannedStmt, ParseError> {
    build_plan_with_outer(stmt, catalog, &[], None, &[], &[], config)
}

pub fn pg_plan_query_with_outer(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_columns: &[(String, SqlType)],
) -> Result<PlannedStmt, ParseError> {
    let desc = RelationDesc {
        columns: outer_columns
            .iter()
            .map(|(name, sql_type)| column_desc(name.clone(), *sql_type, true))
            .collect(),
    };
    let outer_scope = scope_for_relation(None, &desc);
    build_plan_with_outer(
        stmt,
        catalog,
        &[outer_scope],
        None,
        &[],
        &[],
        PlannerConfig::default(),
    )
}

pub fn pg_plan_query_with_sql_function_args(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    input_args: &[(Option<String>, SqlType)],
) -> Result<PlannedStmt, ParseError> {
    build_plan_with_outer(
        stmt,
        catalog,
        &sql_function_arg_outer_scopes(catalog, input_args),
        None,
        &[],
        &[],
        PlannerConfig::default(),
    )
}

pub(crate) fn pg_plan_query_with_outer_scopes(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
) -> Result<PlannedStmt, ParseError> {
    build_plan_with_outer(
        stmt,
        catalog,
        outer_scopes,
        None,
        &[],
        &[],
        PlannerConfig::default(),
    )
}

pub(crate) fn pg_plan_query_with_outer_scopes_and_ctes(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    outer_ctes: &[BoundCte],
) -> Result<PlannedStmt, ParseError> {
    pg_plan_query_with_outer_scopes_and_ctes_config(
        stmt,
        catalog,
        outer_scopes,
        outer_ctes,
        PlannerConfig::default(),
    )
}

pub(crate) fn pg_plan_query_with_outer_scopes_and_ctes_config(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    outer_ctes: &[BoundCte],
    config: PlannerConfig,
) -> Result<PlannedStmt, ParseError> {
    build_plan_with_outer(stmt, catalog, outer_scopes, None, outer_ctes, &[], config)
}

pub fn build_plan(stmt: &SelectStatement, catalog: &dyn CatalogLookup) -> Result<Plan, ParseError> {
    Ok(pg_plan_query(stmt, catalog)?.plan_tree)
}

pub fn pg_plan_values_query(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
) -> Result<PlannedStmt, ParseError> {
    pg_plan_values_query_with_config(stmt, catalog, PlannerConfig::default())
}

pub fn pg_plan_values_query_with_config(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> Result<PlannedStmt, ParseError> {
    build_values_plan_with_outer(stmt, catalog, &[], None, &[], &[], config)
}

pub fn pg_plan_values_query_with_outer(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
    outer_columns: &[(String, SqlType)],
) -> Result<PlannedStmt, ParseError> {
    let desc = RelationDesc {
        columns: outer_columns
            .iter()
            .map(|(name, sql_type)| column_desc(name.clone(), *sql_type, true))
            .collect(),
    };
    let outer_scope = scope_for_relation(None, &desc);
    build_values_plan_with_outer(
        stmt,
        catalog,
        &[outer_scope],
        None,
        &[],
        &[],
        PlannerConfig::default(),
    )
}

pub fn pg_plan_values_query_with_sql_function_args(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
    input_args: &[(Option<String>, SqlType)],
) -> Result<PlannedStmt, ParseError> {
    build_values_plan_with_outer(
        stmt,
        catalog,
        &sql_function_arg_outer_scopes(catalog, input_args),
        None,
        &[],
        &[],
        PlannerConfig::default(),
    )
}

fn sql_function_arg_outer_scopes(
    catalog: &dyn CatalogLookup,
    input_args: &[(Option<String>, SqlType)],
) -> Vec<BoundScope> {
    let mut scalar_columns = Vec::new();
    let mut scopes = Vec::new();
    for (name, ty) in input_args {
        let Some(name) = name.as_ref() else {
            continue;
        };
        if matches!(ty.kind, SqlTypeKind::Composite)
            && ty.typrelid != 0
            && let Some(relation) = catalog.lookup_relation_by_oid(ty.typrelid)
        {
            let desc = RelationDesc {
                columns: relation
                    .desc
                    .columns
                    .into_iter()
                    .filter(|column| !column.dropped)
                    .collect(),
            };
            scopes.push(scope_for_relation(Some(name), &desc));
        } else {
            scalar_columns.push((name.clone(), *ty));
        }
    }
    if !scalar_columns.is_empty() {
        let desc = RelationDesc {
            columns: scalar_columns
                .into_iter()
                .map(|(name, sql_type)| column_desc(name, sql_type, true))
                .collect(),
        };
        scopes.insert(0, scope_for_relation(None, &desc));
    }
    scopes
}

pub(crate) fn pg_plan_values_query_with_outer_scopes(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
) -> Result<PlannedStmt, ParseError> {
    build_values_plan_with_outer(
        stmt,
        catalog,
        outer_scopes,
        None,
        &[],
        &[],
        PlannerConfig::default(),
    )
}

pub(crate) fn pg_plan_values_query_with_outer_scopes_and_ctes(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    outer_ctes: &[BoundCte],
) -> Result<PlannedStmt, ParseError> {
    pg_plan_values_query_with_outer_scopes_and_ctes_config(
        stmt,
        catalog,
        outer_scopes,
        outer_ctes,
        PlannerConfig::default(),
    )
}

pub(crate) fn pg_plan_values_query_with_outer_scopes_and_ctes_config(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    outer_ctes: &[BoundCte],
    config: PlannerConfig,
) -> Result<PlannedStmt, ParseError> {
    build_values_plan_with_outer(stmt, catalog, outer_scopes, None, outer_ctes, &[], config)
}

pub(crate) fn bound_cte_from_materialized_rows(
    name: String,
    desc: &RelationDesc,
    rows: &[Vec<Value>],
) -> BoundCte {
    let cte_id = NEXT_CTE_ID.fetch_add(1, Ordering::Relaxed);
    let visible_indexes = desc
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| (!column.dropped).then_some(index))
        .collect::<Vec<_>>();
    let output_columns = visible_indexes
        .iter()
        .map(|index| {
            let column = &desc.columns[*index];
            QueryColumn {
                name: column.name.clone(),
                sql_type: column.sql_type,
                wire_type_oid: None,
            }
        })
        .collect::<Vec<_>>();
    let values_rows = rows
        .iter()
        .map(|row| {
            visible_indexes
                .iter()
                .map(|index| Expr::Const(row.get(*index).cloned().unwrap_or(Value::Null)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let plan = AnalyzedFrom::values(values_rows, output_columns.clone());
    BoundCte {
        name,
        cte_id,
        plan: Query {
            command_type: crate::include::executor::execdesc::CommandType::Select,
            depends_on_row_security: false,
            rtable: plan.rtable,
            jointree: plan.jointree,
            target_list: identity_target_list(&output_columns, &plan.output_exprs),
            distinct: false,
            distinct_on: Vec::new(),
            where_qual: None,
            group_by: Vec::new(),
            group_by_refs: Vec::new(),
            grouping_sets: Vec::new(),
            accumulators: Vec::new(),
            window_clauses: Vec::new(),
            having_qual: None,
            sort_clause: Vec::new(),
            constraint_deps: Vec::new(),
            limit_count: None,
            limit_offset: None,
            locking_clause: None,
            locking_targets: Vec::new(),
            locking_nowait: false,
            row_marks: Vec::new(),
            has_target_srfs: false,
            recursive_union: None,
            set_operation: None,
        },
        desc: RelationDesc {
            columns: output_columns
                .into_iter()
                .map(|column| column_desc(column.name, column.sql_type, true))
                .collect(),
        },
        self_reference: false,
        worktable_id: 0,
    }
}

pub(crate) fn bound_cte_from_query_rows(
    name: String,
    output_columns: Vec<QueryColumn>,
    rows: &[Vec<Value>],
) -> BoundCte {
    let cte_id = NEXT_CTE_ID.fetch_add(1, Ordering::Relaxed);
    let values_rows = rows
        .iter()
        .map(|row| row.iter().cloned().map(Expr::Const).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let plan = AnalyzedFrom::values(values_rows, output_columns.clone());
    BoundCte {
        name,
        cte_id,
        plan: Query {
            command_type: crate::include::executor::execdesc::CommandType::Select,
            depends_on_row_security: false,
            rtable: plan.rtable,
            jointree: plan.jointree,
            target_list: identity_target_list(&output_columns, &plan.output_exprs),
            distinct: false,
            distinct_on: Vec::new(),
            where_qual: None,
            group_by: Vec::new(),
            group_by_refs: Vec::new(),
            grouping_sets: Vec::new(),
            accumulators: Vec::new(),
            window_clauses: Vec::new(),
            having_qual: None,
            sort_clause: Vec::new(),
            constraint_deps: Vec::new(),
            limit_count: None,
            limit_offset: None,
            locking_clause: None,
            locking_targets: Vec::new(),
            locking_nowait: false,
            row_marks: Vec::new(),
            has_target_srfs: false,
            recursive_union: None,
            set_operation: None,
        },
        desc: RelationDesc {
            columns: output_columns
                .into_iter()
                .map(|column| column_desc(column.name, column.sql_type, true))
                .collect(),
        },
        self_reference: false,
        worktable_id: 0,
    }
}

pub(crate) fn bound_cte_from_query_columns(
    name: String,
    output_columns: Vec<QueryColumn>,
) -> BoundCte {
    bound_cte_from_query_rows(name, output_columns, &[])
}

pub fn build_values_plan(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
) -> Result<Plan, ParseError> {
    Ok(pg_plan_values_query(stmt, catalog)?.plan_tree)
}

fn bind_values_query_with_outer(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    outer_ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<(Query, BoundScope), ParseError> {
    let local_ctes = bind_ctes(
        stmt.with_recursive,
        &stmt.with,
        catalog,
        outer_scopes,
        grouped_outer.clone(),
        outer_ctes,
        expanded_views,
    )?;
    let mut visible_ctes = local_ctes.clone();
    visible_ctes.extend_from_slice(outer_ctes);
    let (base, scope) = bind_values_rows(
        &stmt.rows,
        None,
        catalog,
        outer_scopes,
        grouped_outer.as_ref(),
        &visible_ctes,
    )?;
    let target_list = normalize_target_list(identity_target_list(
        &base.output_columns,
        &base.output_exprs,
    ));
    let sort_inputs = if stmt.order_by.is_empty() {
        Vec::new()
    } else {
        bind_order_by_items(&stmt.order_by, &target_list, catalog, |expr| {
            bind_expr_with_outer_and_ctes(
                expr,
                &scope,
                catalog,
                outer_scopes,
                grouped_outer.as_ref(),
                &visible_ctes,
            )
        })?
    };
    let sort_clause = build_sort_clause(sort_inputs, &target_list);
    let AnalyzedFrom {
        rtable,
        jointree,
        output_columns: _,
        output_exprs: _,
    } = base;
    Ok((
        Query {
            command_type: crate::include::executor::execdesc::CommandType::Select,
            depends_on_row_security: false,
            rtable,
            jointree,
            target_list,
            distinct: false,
            distinct_on: Vec::new(),
            where_qual: None,
            group_by: Vec::new(),
            group_by_refs: Vec::new(),
            grouping_sets: Vec::new(),
            accumulators: Vec::new(),
            window_clauses: Vec::new(),
            having_qual: None,
            sort_clause,
            constraint_deps: Vec::new(),
            limit_count: stmt.limit,
            limit_offset: stmt.offset,
            locking_clause: None,
            locking_targets: Vec::new(),
            locking_nowait: false,
            row_marks: Vec::new(),
            has_target_srfs: false,
            recursive_union: None,
            set_operation: None,
        },
        scope,
    ))
}

fn build_values_plan_with_outer(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    outer_ctes: &[BoundCte],
    expanded_views: &[u32],
    config: PlannerConfig,
) -> Result<PlannedStmt, ParseError> {
    let (query, _) = analyze_values_query_with_outer(
        stmt,
        catalog,
        outer_scopes,
        grouped_outer,
        outer_ctes,
        expanded_views,
    )?;
    let [query] = pg_rewrite_query(query, catalog)?
        .try_into()
        .expect("values rewrite should return a single query");
    let query = if config.fold_constants {
        crate::backend::optimizer::fold_query_constants(query)?
    } else {
        query
    };
    planner_with_config(query, catalog, config)
}

fn bind_select_query_with_outer(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    visible_agg_scope: Option<&VisibleAggregateScope>,
    outer_ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<(Query, BoundScope), ParseError> {
    with_visible_aggregate_scope(visible_agg_scope.cloned(), || {
        let local_ctes = bind_ctes(
            stmt.with_recursive,
            &stmt.with,
            catalog,
            outer_scopes,
            grouped_outer.clone(),
            outer_ctes,
            expanded_views,
        )?;
        let mut visible_ctes = local_ctes.clone();
        visible_ctes.extend_from_slice(outer_ctes);

        if stmt.set_operation.is_some() {
            return bind_set_operation_query_with_outer(
                stmt,
                catalog,
                outer_scopes,
                grouped_outer,
                &visible_ctes,
                expanded_views,
            );
        }

        let (mut base, scope) = if let Some(from) = &stmt.from {
            bind_from_item_with_ctes(
                from,
                catalog,
                outer_scopes,
                grouped_outer.as_ref(),
                &visible_ctes,
                expanded_views,
            )?
        } else {
            (AnalyzedFrom::result(), empty_scope())
        };

        let target_exprs = stmt
            .targets
            .iter()
            .map(|target| &target.expr)
            .collect::<Vec<_>>();
        let target_aggs = collect_local_aggregates(
            &target_exprs,
            &scope,
            catalog,
            outer_scopes,
            grouped_outer.as_ref(),
            &visible_ctes,
            expanded_views,
        )?;
        if let Some(predicate) = &stmt.where_clause {
            analyze_expr_aggregates_in_clause(
                predicate,
                AggregateClauseKind::Where,
                &scope,
                catalog,
                outer_scopes,
                grouped_outer.as_ref(),
                &visible_ctes,
                expanded_views,
            )?;
            reject_window_clause(predicate, "WHERE")?;
        }
        let lower_distinct_to_grouping = stmt.distinct
            && stmt.distinct_on.is_empty()
            && stmt.group_by.is_empty()
            && target_aggs.is_empty()
            && stmt.having.is_none()
            && (stmt.limit.is_some() || stmt.offset.is_some());
        let expanded_group_by = if lower_distinct_to_grouping {
            ExpandedGroupBy {
                group_by: stmt
                    .targets
                    .iter()
                    .map(|target| target.expr.clone())
                    .collect::<Vec<_>>(),
                grouping_sets: Vec::new(),
                has_explicit_grouping_sets: false,
            }
        } else {
            expand_group_by_items(stmt, &scope)?
        };
        let mut effective_group_by = expanded_group_by.group_by.clone();
        let mut constraint_deps = if expanded_group_by.has_explicit_grouping_sets {
            Vec::new()
        } else {
            expand_group_by_with_primary_key_dependencies(&mut effective_group_by, &scope, catalog)
        };

        for group_expr in &effective_group_by {
            analyze_expr_aggregates_in_clause(
                group_expr,
                AggregateClauseKind::GroupBy,
                &scope,
                catalog,
                outer_scopes,
                grouped_outer.as_ref(),
                &visible_ctes,
                expanded_views,
            )?;
            reject_window_clause(group_expr, "GROUP BY")?;
        }
        let having_agg_summary = if let Some(having) = &stmt.having {
            reject_window_clause(having, "HAVING")?;
            Some(analyze_expr_aggregates_in_clause(
                having,
                AggregateClauseKind::Having,
                &scope,
                catalog,
                outer_scopes,
                grouped_outer.as_ref(),
                &visible_ctes,
                expanded_views,
            )?)
        } else {
            None
        };

        let bound_where_qual = stmt
            .where_clause
            .as_ref()
            .map(|predicate| {
                bind_expr_with_outer_and_ctes(
                    predicate,
                    &scope,
                    catalog,
                    outer_scopes,
                    grouped_outer.as_ref(),
                    &visible_ctes,
                )
            })
            .transpose()?;
        if bound_where_qual
            .as_ref()
            .is_some_and(expr_contains_set_returning)
        {
            return Err(ParseError::FeatureNotSupported(
                "set-returning functions are not allowed in WHERE".into(),
            ));
        }

        let needs_agg = !effective_group_by.is_empty()
            || expanded_group_by.has_explicit_grouping_sets
            || !target_aggs.is_empty()
            || stmt.having.is_some();

        let can_skip_scan_for_degenerate_having = needs_agg
            && effective_group_by.is_empty()
            && target_aggs.is_empty()
            && stmt.having.as_ref().is_some_and(|having| {
                !having_agg_summary
                    .as_ref()
                    .is_some_and(|summary| summary.has_local_agg)
                    && !expr_references_input_scope(having)
            })
            && stmt
                .targets
                .iter()
                .all(|target| !expr_references_input_scope(&target.expr));

        if can_skip_scan_for_degenerate_having {
            base = AnalyzedFrom::result();
        }

        let where_qual = if can_skip_scan_for_degenerate_having {
            None
        } else {
            bound_where_qual
        };

        let window_state = Rc::new(RefCell::new(WindowBindingState::default()));
        register_named_window_specs(&window_state, &stmt.window_clauses)?;

        let mut aggs = target_aggs;
        if let Some(summary) = &having_agg_summary {
            for agg in &summary.local_aggs {
                if !aggs.contains(agg) {
                    aggs.push(agg.clone());
                }
            }
        }
        let aggs = dedupe_local_aggregate_list(&aggs, &scope, catalog, outer_scopes, &visible_ctes);
        let local_agg_scope = build_local_aggregate_scope(
            &scope,
            grouped_outer.as_ref(),
            &aggs,
            &effective_group_by,
            &[],
        );

        with_local_aggregate_scope(local_agg_scope, || {
            if needs_agg {
                let group_key_refs = (1..=effective_group_by.len()).collect::<Vec<_>>();
                let group_keys: Vec<Expr> = effective_group_by
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
                let group_key_exprs = group_keys
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(index, expr)| {
                        Expr::GroupingKey(Box::new(
                            crate::include::nodes::primnodes::GroupingKeyExpr {
                                expr: Box::new(expr),
                                ref_id: group_key_refs[index],
                            },
                        ))
                    })
                    .collect::<Vec<_>>();
                let output_group_key_exprs = if expanded_group_by.has_explicit_grouping_sets {
                    group_key_exprs.clone()
                } else {
                    group_keys.clone()
                };
                let grouping_sets = if expanded_group_by.has_explicit_grouping_sets {
                    bind_grouping_sets(
                        &expanded_group_by.grouping_sets,
                        &group_key_exprs,
                        &group_key_refs,
                        &scope,
                        catalog,
                        outer_scopes,
                        grouped_outer.as_ref(),
                        &visible_ctes,
                    )?
                } else {
                    Vec::new()
                };
                validate_grouping_set_capabilities(
                    &grouping_sets,
                    &group_key_exprs,
                    &group_key_refs,
                    &aggs,
                )?;
                let rewritten_group_keys = group_keys.clone();
                let local_agg_scope = build_local_aggregate_scope(
                    &scope,
                    grouped_outer.as_ref(),
                    &aggs,
                    &effective_group_by,
                    &output_group_key_exprs,
                );

                return with_local_aggregate_scope(local_agg_scope, || {
                    with_grouped_agg_cte_context(&visible_ctes, &local_ctes, || {
                        let accumulators: Vec<AggAccum> = aggs
                        .iter()
                        .map(|agg| {
                            let within_group_aggkind = (!agg.order_by.is_empty())
                                .then(|| {
                                    aggregate_call_kind_matches_catalog(
                                        catalog,
                                        &agg.name,
                                        &agg.args,
                                        Some(&agg.order_by),
                                    )
                                })
                                .flatten();
                            let builtin_hypothetical =
                                resolve_builtin_hypothetical_aggregate(&agg.name).is_some();
                            let builtin_ordered_set =
                                resolve_builtin_ordered_set_aggregate(&agg.name).is_some();
                            let hypothetical =
                                (builtin_hypothetical || within_group_aggkind == Some('h'))
                                    && !agg.direct_args.is_empty();
                            let ordered_set =
                                (builtin_ordered_set || within_group_aggkind == Some('o'))
                                    && !agg.order_by.is_empty();
                            if aggregate_args_are_named(agg.args.args()) {
                                return Err(ParseError::UnexpectedToken {
                                    expected: "aggregate arguments without names",
                                    actual: agg.name.clone(),
                                });
                            }
                            if (hypothetical || ordered_set)
                                && aggregate_args_are_named(&agg.direct_args)
                            {
                                return Err(ParseError::UnexpectedToken {
                                    expected: "aggregate arguments without names",
                                    actual: agg.name.clone(),
                                });
                            }
                            let arg_values: Vec<SqlExpr> = agg
                                .args
                                .args()
                                .iter()
                                .map(|arg| arg.value.clone())
                                .collect();
                            for arg in &arg_values {
                                reject_nested_local_ctes_in_raw_agg_expr(arg)?;
                            }
                            if !hypothetical && !ordered_set {
                                validate_distinct_aggregate_order_by(
                                    &arg_values,
                                    &agg.order_by,
                                    agg.distinct,
                                )?;
                            }
                            if !hypothetical
                                && !ordered_set
                                && let Some(func) = resolve_builtin_aggregate(&agg.name)
                            {
                                validate_aggregate_arity(func, &arg_values)?;
                            }
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
                            let resolved = if hypothetical || ordered_set {
                                None
                            } else {
                                Some(
                                    resolve_aggregate_call(
                                        catalog,
                                        &agg.name,
                                        &arg_types,
                                        agg.func_variadic,
                                    )
                                    .ok_or_else(|| ParseError::UnexpectedToken {
                                        expected: "supported aggregate",
                                        actual: agg.name.clone(),
                                    })?,
                                )
                            };
                            let bound_args = arg_values
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
                                .collect::<Result<Vec<_>, _>>()?;
                            for arg in &bound_args {
                                reject_nested_local_ctes_in_agg_expr(arg)?;
                                if expr_contains_set_returning(arg) {
                                    return Err(ParseError::FeatureNotSupported(
                                        "set-returning functions are not allowed in aggregate arguments"
                                            .into(),
                                    ));
                                }
                            }
                            let bound_direct_args = if hypothetical || ordered_set {
                                for arg in &agg.direct_args {
                                    reject_nested_local_ctes_in_raw_agg_expr(&arg.value)?;
                                }
                                agg.direct_args
                                    .iter()
                                    .map(|arg| {
                                        bind_expr_with_outer_and_ctes(
                                            &arg.value,
                                            &scope,
                                            catalog,
                                            outer_scopes,
                                            grouped_outer.as_ref(),
                                            &visible_ctes,
                                        )
                                    })
                                    .collect::<Result<Vec<_>, _>>()?
                            } else {
                                Vec::new()
                            };
                            for arg in &bound_direct_args {
                                reject_nested_local_ctes_in_agg_expr(arg)?;
                                if expr_contains_set_returning(arg) {
                                    return Err(ParseError::FeatureNotSupported(
                                        "set-returning functions are not allowed in ordered-set aggregate direct arguments"
                                            .into(),
                                    ));
                                }
                            }
                            let bound_filter = agg
                                .filter
                                .as_ref()
                                .map(|expr| {
                                    reject_nested_local_ctes_in_raw_agg_expr(expr)?;
                                    bind_expr_with_outer_and_ctes(
                                        expr,
                                        &scope,
                                        catalog,
                                        outer_scopes,
                                        grouped_outer.as_ref(),
                                        &visible_ctes,
                                    )
                                })
                                .transpose()?;
                            if let Some(filter) = &bound_filter {
                                reject_nested_local_ctes_in_agg_expr(filter)?;
                                if expr_contains_set_returning(filter) {
                                    return Err(ParseError::FeatureNotSupported(
                                        "set-returning functions are not allowed in aggregate FILTER"
                                            .into(),
                                    ));
                                }
                            }
                            let direct_arg_types = if hypothetical || ordered_set {
                                agg.direct_args
                                    .iter()
                                    .map(|arg| {
                                        infer_sql_expr_type_with_ctes(
                                            &arg.value,
                                            &scope,
                                            catalog,
                                            outer_scopes,
                                            grouped_outer.as_ref(),
                                            &visible_ctes,
                                        )
                                    })
                                    .collect::<Vec<_>>()
                            } else {
                                Vec::new()
                            };
                            let bound_order_exprs = agg
                                .order_by
                                .iter()
                                .map(|item| {
                                    reject_nested_local_ctes_in_raw_agg_expr(&item.expr)?;
                                    bind_expr_with_outer_and_ctes(
                                        &item.expr,
                                        &scope,
                                        catalog,
                                        outer_scopes,
                                        grouped_outer.as_ref(),
                                        &visible_ctes,
                                    )
                                })
                                .collect::<Result<Vec<_>, _>>()?;
                            for item in &bound_order_exprs {
                                reject_nested_local_ctes_in_agg_expr(item)?;
                                if expr_contains_set_returning(item) {
                                    return Err(ParseError::FeatureNotSupported(
                                        "set-returning functions are not allowed in aggregate ORDER BY"
                                            .into(),
                                    ));
                                }
                            }
                            let (coerced_direct_args, coerced_args, bound_order_by) =
                                if hypothetical && builtin_hypothetical {
                                    let direct_arg_types = agg
                                        .direct_args
                                        .iter()
                                        .map(|arg| {
                                            infer_sql_expr_type_with_ctes(
                                                &arg.value,
                                                &scope,
                                                catalog,
                                                outer_scopes,
                                                grouped_outer.as_ref(),
                                                &visible_ctes,
                                            )
                                        })
                                        .collect::<Vec<_>>();
                                    coerce_hypothetical_aggregate_inputs(
                                        &agg.name,
                                        &agg.direct_args,
                                        &direct_arg_types,
                                        bound_direct_args,
                                        agg.args.args(),
                                        &arg_types,
                                        bound_args,
                                        &agg.order_by,
                                        bound_order_exprs,
                                        catalog,
                                    )?
                                } else if ordered_set && builtin_ordered_set {
                                    let direct_arg_types = agg
                                        .direct_args
                                        .iter()
                                        .map(|arg| {
                                            infer_sql_expr_type_with_ctes(
                                                &arg.value,
                                                &scope,
                                                catalog,
                                                outer_scopes,
                                                grouped_outer.as_ref(),
                                                &visible_ctes,
                                            )
                                        })
                                        .collect::<Vec<_>>();
                                    coerce_ordered_set_aggregate_inputs(
                                        &agg.name,
                                        &agg.direct_args,
                                        &direct_arg_types,
                                        bound_direct_args,
                                        agg.args.args(),
                                        &arg_types,
                                        bound_args,
                                        &agg.order_by,
                                        bound_order_exprs,
                                        catalog,
                                    )?
                                } else if hypothetical || ordered_set {
                                    let expected_aggkind = if hypothetical { 'h' } else { 'o' };
                                    let resolved_catalog =
                                        resolve_catalog_within_group_aggregate_call(
                                            catalog,
                                            &agg.name,
                                            &direct_arg_types,
                                            &arg_types,
                                            agg.func_variadic,
                                            expected_aggkind,
                                        )
                                        .ok_or_else(|| ParseError::UnexpectedToken {
                                            expected: "supported aggregate",
                                            actual: agg.name.clone(),
                                        })?;
                                    coerce_catalog_within_group_aggregate_inputs(
                                        &agg.direct_args,
                                        &direct_arg_types,
                                        bound_direct_args,
                                        agg.args.args(),
                                        &arg_types,
                                        bound_args,
                                        &agg.order_by,
                                        bound_order_exprs,
                                        &resolved_catalog.declared_arg_types,
                                        catalog,
                                    )?
                                } else {
                                    let bound_order_by = bound_order_exprs
                                        .into_iter()
                                        .zip(agg.order_by.iter())
                                        .map(|(bound_expr, item)| {
                                            build_bound_order_by_entry(item, bound_expr, 0, catalog)
                                        })
                                        .collect::<Result<Vec<_>, ParseError>>()?;
                                    let resolved = resolved.as_ref().expect(
                                        "non-hypothetical aggregate resolution should exist",
                                    );
                                    let coerced_args = bound_args
                                        .into_iter()
                                        .zip(arg_types.iter().copied())
                                        .zip(resolved.declared_arg_types.iter().copied())
                                        .map(|((arg, actual_type), declared_type)| {
                                            coerce_bound_expr(
                                                arg,
                                                actual_type,
                                                aggregate_arg_type_for_coercion(
                                                    resolved.builtin_impl,
                                                    actual_type,
                                                    declared_type,
                                                ),
                                            )
                                        })
                                        .collect();
                                    let coerced_args = preserve_array_agg_array_arg_type(
                                        resolved.builtin_impl,
                                        &arg_types,
                                        &arg_values,
                                        coerced_args,
                                        catalog,
                                    );
                                    (Vec::new(), coerced_args, bound_order_by)
                                };
                            let (aggfnoid, agg_variadic, sql_type) =
                                if hypothetical && builtin_hypothetical {
                                let resolved =
                                    resolve_hypothetical_aggregate_call(&agg.name).ok_or_else(
                                        || ParseError::UnexpectedToken {
                                            expected: "supported aggregate",
                                            actual: agg.name.clone(),
                                        },
                                    )?;
                                (resolved.proc_oid, false, resolved.result_type)
                            } else if ordered_set && builtin_ordered_set {
                                let resolved = resolve_ordered_set_aggregate_call(
                                    &agg.name,
                                    &direct_arg_types,
                                    &arg_types,
                                )
                                .ok_or_else(|| ParseError::UnexpectedToken {
                                    expected: "supported aggregate",
                                    actual: agg.name.clone(),
                                })?;
                                (resolved.proc_oid, false, resolved.result_type)
                            } else if hypothetical || ordered_set {
                                let expected_aggkind = if hypothetical { 'h' } else { 'o' };
                                let resolved = resolve_catalog_within_group_aggregate_call(
                                    catalog,
                                    &agg.name,
                                    &direct_arg_types,
                                    &arg_types,
                                    agg.func_variadic,
                                    expected_aggkind,
                                )
                                .ok_or_else(|| ParseError::UnexpectedToken {
                                    expected: "supported aggregate",
                                    actual: agg.name.clone(),
                                })?;
                                (resolved.proc_oid, resolved.func_variadic, resolved.result_type)
                            } else {
                                let resolved = resolved
                                    .as_ref()
                                    .expect("non-hypothetical aggregate resolution should exist");
                                (
                                    resolved.proc_oid,
                                    resolved.func_variadic,
                                    resolved.result_type,
                                )
                            };
                            Ok(AggAccum {
                                aggfnoid,
                                agg_variadic,
                                direct_args: coerced_direct_args,
                                args: coerced_args,
                                order_by: bound_order_by,
                                filter: bound_filter,
                                distinct: agg.distinct,
                                sql_type,
                            })
                        })
                        .collect::<Result<_, _>>()?;

                        let n_keys = group_keys.len();
                        let mut output_columns: Vec<QueryColumn> = Vec::new();
                        for gk in &effective_group_by {
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
                                wire_type_oid: None,
                            });
                        }
                        for agg in &aggs {
                            let within_group_aggkind = (!agg.order_by.is_empty())
                                .then(|| {
                                    aggregate_call_kind_matches_catalog(
                                        catalog,
                                        &agg.name,
                                        &agg.args,
                                        Some(&agg.order_by),
                                    )
                                })
                                .flatten();
                            let builtin_hypothetical =
                                resolve_builtin_hypothetical_aggregate(&agg.name).is_some();
                            let builtin_ordered_set =
                                resolve_builtin_ordered_set_aggregate(&agg.name).is_some();
                            let hypothetical = (builtin_hypothetical
                                || within_group_aggkind == Some('h'))
                                && !agg.direct_args.is_empty();
                            let ordered_set = (builtin_ordered_set
                                || within_group_aggkind == Some('o'))
                                && !agg.order_by.is_empty();
                            let arg_values: Vec<SqlExpr> = agg
                                .args
                                .args()
                                .iter()
                                .map(|arg| arg.value.clone())
                                .collect();
                            let arg_types = arg_values
                                .iter()
                                .map(|expr| {
                                    infer_sql_expr_type_with_ctes(
                                        expr,
                                        &scope,
                                        catalog,
                                        outer_scopes,
                                        grouped_outer.as_ref(),
                                        &visible_ctes,
                                    )
                                })
                                .collect::<Vec<_>>();
                            let direct_arg_types = if hypothetical || ordered_set {
                                agg.direct_args
                                    .iter()
                                    .map(|arg| {
                                        infer_sql_expr_type_with_ctes(
                                            &arg.value,
                                            &scope,
                                            catalog,
                                            outer_scopes,
                                            grouped_outer.as_ref(),
                                            &visible_ctes,
                                        )
                                    })
                                    .collect::<Vec<_>>()
                            } else {
                                Vec::new()
                            };
                            let result_type = if hypothetical && builtin_hypothetical {
                                resolve_hypothetical_aggregate_call(&agg.name)
                                    .map(|resolved| resolved.result_type)
                                    .ok_or_else(|| ParseError::UnexpectedToken {
                                        expected: "supported aggregate",
                                        actual: agg.name.clone(),
                                    })?
                            } else if ordered_set && builtin_ordered_set {
                                resolve_ordered_set_aggregate_call(
                                    &agg.name,
                                    &direct_arg_types,
                                    &arg_types,
                                )
                                .map(|resolved| resolved.result_type)
                                .ok_or_else(|| {
                                    ParseError::UnexpectedToken {
                                        expected: "supported aggregate",
                                        actual: agg.name.clone(),
                                    }
                                })?
                            } else if hypothetical || ordered_set {
                                let expected_aggkind = if hypothetical { 'h' } else { 'o' };
                                resolve_catalog_within_group_aggregate_call(
                                    catalog,
                                    &agg.name,
                                    &direct_arg_types,
                                    &arg_types,
                                    agg.func_variadic,
                                    expected_aggkind,
                                )
                                .map(|resolved| resolved.result_type)
                                .ok_or_else(|| {
                                    ParseError::UnexpectedToken {
                                        expected: "supported aggregate",
                                        actual: agg.name.clone(),
                                    }
                                })?
                            } else {
                                resolve_aggregate_call(
                                    catalog,
                                    &agg.name,
                                    &arg_types,
                                    agg.func_variadic,
                                )
                                .map(|resolved| resolved.result_type)
                                .ok_or_else(|| {
                                    ParseError::UnexpectedToken {
                                        expected: "supported aggregate",
                                        actual: agg.name.clone(),
                                    }
                                })?
                            };
                            output_columns.push(QueryColumn {
                                name: agg.name.clone(),
                                sql_type: result_type,
                                wire_type_oid: None,
                            });
                        }

                        let (
                            (having, target_list, sort_clause, distinct_on, has_target_srfs),
                            mut functional_constraint_deps,
                        ) = with_functional_grouping_constraint_tracking(|| {
                            let having = stmt
                                .having
                                .as_ref()
                                .map(|e| {
                                    bind_agg_output_expr_in_clause(
                                        e,
                                        UngroupedColumnClause::Having,
                                        &effective_group_by,
                                        &output_group_key_exprs,
                                        &scope,
                                        catalog,
                                        outer_scopes,
                                        grouped_outer.as_ref(),
                                        &aggs,
                                        n_keys,
                                    )
                                })
                                .transpose()?;
                            if having.as_ref().is_some_and(expr_contains_set_returning) {
                                return Err(ParseError::FeatureNotSupported(
                                    "set-returning functions are not allowed in HAVING".into(),
                                ));
                            }

                            let targets: Vec<TargetEntry> = with_window_binding(
                                window_state.clone(),
                                true,
                                || {
                                    if stmt.targets.len() == 1
                                        && matches!(stmt.targets[0].expr, SqlExpr::Column(ref name) if name == "*")
                                    {
                                        let mut targets = Vec::with_capacity(output_columns.len());
                                        for (i, name) in
                                            output_columns.iter().enumerate().take(n_keys)
                                        {
                                            targets.push(
                                                TargetEntry::new(
                                                    name.name.clone(),
                                                    output_group_key_exprs
                                                        .get(i)
                                                        .cloned()
                                                        .unwrap_or_else(|| {
                                                            panic!(
                                                                "aggregate SELECT * missing grouped key expr for target position {}",
                                                                i + 1
                                                            )
                                                        }),
                                                    name.sql_type,
                                                    i + 1,
                                                )
                                                .with_input_resno(i + 1),
                                            );
                                        }
                                        for (i, accum) in accumulators.iter().enumerate() {
                                            let target_index = n_keys + i;
                                            let name = output_columns
                                                .get(target_index)
                                                .expect("aggregate output column")
                                                .name
                                                .clone();
                                            targets.push(TargetEntry::new(
                                                name,
                                                Expr::aggref(
                                                    accum.aggfnoid,
                                                    accum.sql_type,
                                                    accum.agg_variadic,
                                                    accum.distinct,
                                                    accum.direct_args.clone(),
                                                    accum.args.clone(),
                                                    accum.order_by.clone(),
                                                    accum.filter.clone(),
                                                    i,
                                                ),
                                                accum.sql_type,
                                                target_index + 1,
                                            ));
                                        }
                                        Ok(targets)
                                    } else {
                                        let mut targets = Vec::new();
                                        let mut used_group_by_targets = Vec::new();
                                        for item in &stmt.targets {
                                            let expanded_items =
                                                grouped_select_item_exprs(item, &scope)?;
                                            for (output_name, item_expr) in expanded_items {
                                                let expr = if let Some(group_index) =
                                                    take_group_by_expr_match(
                                                        &item_expr,
                                                        &effective_group_by,
                                                        &mut used_group_by_targets,
                                                    ) {
                                                    output_group_key_exprs
                                                    .get(group_index)
                                                    .cloned()
                                                    .unwrap_or_else(|| {
                                                        panic!(
                                                            "aggregate output missing grouped key expr for target position {}",
                                                            group_index + 1
                                                        )
                                                    })
                                                } else {
                                                    bind_agg_output_expr_in_clause(
                                                        &item_expr,
                                                        UngroupedColumnClause::SelectTarget,
                                                        &effective_group_by,
                                                        &output_group_key_exprs,
                                                        &scope,
                                                        catalog,
                                                        outer_scopes,
                                                        grouped_outer.as_ref(),
                                                        &aggs,
                                                        n_keys,
                                                    )?
                                                };
                                                let sql_type = expr_sql_type_hint(&expr)
                                                    .unwrap_or_else(|| {
                                                        infer_sql_expr_type_with_ctes(
                                                            &item_expr,
                                                            &scope,
                                                            catalog,
                                                            outer_scopes,
                                                            grouped_outer.as_ref(),
                                                            &visible_ctes,
                                                        )
                                                    });
                                                targets.push(TargetEntry::new(
                                                    output_name,
                                                    expr,
                                                    sql_type,
                                                    targets.len() + 1,
                                                ));
                                            }
                                        }
                                        Ok(targets)
                                    }
                                },
                            )?;

                            let sort_inputs =
                                with_window_binding(window_state.clone(), true, || {
                                    if stmt.order_by.is_empty() {
                                        Ok(Vec::new())
                                    } else {
                                        bind_order_by_items(
                                            &stmt.order_by,
                                            &targets,
                                            catalog,
                                            |expr| {
                                                bind_agg_output_expr_in_clause(
                                                    expr,
                                                    UngroupedColumnClause::OrderBy,
                                                    &effective_group_by,
                                                    &output_group_key_exprs,
                                                    &scope,
                                                    catalog,
                                                    outer_scopes,
                                                    grouped_outer.as_ref(),
                                                    &aggs,
                                                    n_keys,
                                                )
                                            },
                                        )
                                    }
                                })?;
                            let sort_clause = build_sort_clause(sort_inputs, &targets);
                            let distinct_on = build_distinct_on_clause(
                                &stmt.distinct_on,
                                &sort_clause,
                                &targets,
                                catalog,
                                |expr| {
                                    bind_agg_output_expr_in_clause(
                                        expr,
                                        UngroupedColumnClause::SelectTarget,
                                        &effective_group_by,
                                        &output_group_key_exprs,
                                        &scope,
                                        catalog,
                                        outer_scopes,
                                        grouped_outer.as_ref(),
                                        &aggs,
                                        n_keys,
                                    )
                                },
                            )?;
                            let target_list = normalize_target_list(targets);
                            let has_target_srfs =
                                target_or_sort_clause_contains_srf(&target_list, &sort_clause);
                            if stmt.distinct
                                && !stmt.distinct_on.is_empty()
                                && target_list
                                    .iter()
                                    .any(|target| expr_contains_set_returning(&target.expr))
                            {
                                return Err(ParseError::FeatureNotSupportedMessage(
                                    "SELECT DISTINCT ON with set-returning functions is not supported"
                                        .into(),
                                ));
                            }

                            Ok((
                                having,
                                target_list,
                                sort_clause,
                                distinct_on,
                                has_target_srfs,
                            ))
                        })?;
                        constraint_deps.append(&mut functional_constraint_deps);
                        constraint_deps.sort_unstable();
                        constraint_deps.dedup();
                        let window_clauses = take_window_clauses(&window_state);

                        let query = Query {
                            command_type: crate::include::executor::execdesc::CommandType::Select,
                            depends_on_row_security: false,
                            rtable: base.rtable,
                            jointree: base.jointree,
                            target_list,
                            distinct: false,
                            distinct_on: Vec::new(),
                            where_qual,
                            group_by: rewritten_group_keys,
                            group_by_refs: group_key_refs,
                            grouping_sets,
                            accumulators,
                            window_clauses,
                            having_qual: having,
                            sort_clause,
                            constraint_deps,
                            limit_count: stmt.limit,
                            limit_offset: stmt.offset,
                            locking_clause: stmt.locking_clause,
                            locking_targets: stmt.locking_targets.clone(),
                            locking_nowait: stmt.locking_nowait,
                            row_marks: Vec::new(),
                            has_target_srfs,
                            recursive_union: None,
                            set_operation: None,
                        };
                        let query = apply_select_distinct(
                            query,
                            stmt.distinct && !lower_distinct_to_grouping,
                            distinct_on,
                        );
                        Ok((query, scope))
                    })
                });
            } else {
                with_grouped_agg_cte_context(&visible_ctes, &local_ctes, || {
                    let bound_targets = with_window_binding(window_state.clone(), true, || {
                        bind_select_targets(
                            &stmt.targets,
                            &scope,
                            catalog,
                            outer_scopes,
                            grouped_outer.as_ref(),
                            &visible_ctes,
                        )
                    })?;

                    let BoundSelectTargets::Plain(targets) = bound_targets;
                    let sort_inputs = with_window_binding(window_state.clone(), true, || {
                        if stmt.order_by.is_empty() {
                            Ok(Vec::new())
                        } else {
                            bind_order_by_items(&stmt.order_by, &targets, catalog, |expr| {
                                bind_expr_with_outer_and_ctes(
                                    expr,
                                    &scope,
                                    catalog,
                                    outer_scopes,
                                    grouped_outer.as_ref(),
                                    &visible_ctes,
                                )
                            })
                        }
                    })?;
                    let sort_clause = build_sort_clause(sort_inputs, &targets);
                    let distinct_on = build_distinct_on_clause(
                        &stmt.distinct_on,
                        &sort_clause,
                        &targets,
                        catalog,
                        |expr| {
                            bind_expr_with_outer_and_ctes(
                                expr,
                                &scope,
                                catalog,
                                outer_scopes,
                                grouped_outer.as_ref(),
                                &visible_ctes,
                            )
                        },
                    )?;
                    let window_clauses = take_window_clauses(&window_state);

                    let is_identity = targets.len() == base.output_columns.len()
                        && targets.iter().enumerate().all(|(i, t)| {
                            t.input_resno == Some(i + 1) && t.name == base.output_columns[i].name
                        });
                    let target_list = if is_identity {
                        normalize_target_list(identity_target_list(
                            &base.output_columns,
                            &base.output_exprs,
                        ))
                    } else {
                        normalize_target_list(targets)
                    };

                    let has_target_srfs =
                        target_or_sort_clause_contains_srf(&target_list, &sort_clause);
                    if stmt.distinct
                        && !stmt.distinct_on.is_empty()
                        && target_list
                            .iter()
                            .any(|target| expr_contains_set_returning(&target.expr))
                    {
                        return Err(ParseError::FeatureNotSupportedMessage(
                            "SELECT DISTINCT ON with set-returning functions is not supported"
                                .into(),
                        ));
                    }
                    let query = Query {
                        command_type: crate::include::executor::execdesc::CommandType::Select,
                        depends_on_row_security: false,
                        rtable: base.rtable,
                        jointree: base.jointree,
                        target_list,
                        distinct: false,
                        distinct_on: Vec::new(),
                        where_qual,
                        group_by: Vec::new(),
                        group_by_refs: Vec::new(),
                        grouping_sets: Vec::new(),
                        accumulators: Vec::new(),
                        window_clauses,
                        having_qual: None,
                        sort_clause,
                        constraint_deps: Vec::new(),
                        limit_count: stmt.limit,
                        limit_offset: stmt.offset,
                        locking_clause: stmt.locking_clause,
                        locking_targets: stmt.locking_targets.clone(),
                        locking_nowait: stmt.locking_nowait,
                        row_marks: Vec::new(),
                        has_target_srfs,
                        recursive_union: None,
                        set_operation: None,
                    };
                    let query = apply_select_distinct(query, stmt.distinct, distinct_on);
                    Ok((query, scope))
                })
            }
        })
    })
}

fn apply_select_distinct(query: Query, distinct: bool, distinct_on: Vec<SortGroupClause>) -> Query {
    Query {
        distinct,
        distinct_on: if distinct { distinct_on } else { Vec::new() },
        ..query
    }
}

fn set_operation_target_is_unknown_string_literal(stmt: &SelectStatement, index: usize) -> bool {
    if stmt.set_operation.is_some() {
        return false;
    }
    stmt.targets.get(index).is_some_and(|target| {
        matches!(
            target.expr,
            SqlExpr::Const(Value::Text(_)) | SqlExpr::Const(Value::TextRef(_, _))
        )
    })
}

fn set_operation_order_by_error_with_input_detail(err: ParseError, inputs: &[Query]) -> ParseError {
    let ParseError::UnknownColumn(name) = err else {
        return err;
    };
    let Some((input_index, _)) = inputs.iter().enumerate().skip(1).find(|(_, query)| {
        query
            .target_list
            .iter()
            .any(|target| target.name.eq_ignore_ascii_case(&name))
    }) else {
        return ParseError::UnknownColumn(name);
    };
    ParseError::DetailedError {
        message: format!("column \"{name}\" does not exist"),
        detail: Some(format!(
            "There is a column named \"{name}\" in table \"*SELECT* {}\", but it cannot be referenced from this part of the query.",
            input_index + 1
        )),
        hint: None,
        sqlstate: "42703",
    }
}

fn bind_set_operation_query_with_outer(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    visible_ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<(Query, BoundScope), ParseError> {
    let Some(set_operation) = stmt.set_operation.as_ref() else {
        return Err(ParseError::UnexpectedToken {
            expected: "set operation",
            actual: "simple SELECT".into(),
        });
    };
    let mut inputs = set_operation
        .inputs
        .iter()
        .map(|input| {
            let visible_agg_scope = current_visible_aggregate_scope();
            analyze_select_query_with_outer(
                input,
                catalog,
                outer_scopes,
                grouped_outer.clone(),
                visible_agg_scope.as_ref(),
                visible_ctes,
                expanded_views,
            )
            .map(|(query, _)| query)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let first_query = inputs.first().ok_or(ParseError::UnexpectedEof)?;
    let width = first_query.target_list.len();
    let output_names = first_query
        .target_list
        .iter()
        .map(|target| target.name.clone())
        .collect::<Vec<_>>();

    for query in &inputs[1..] {
        if query.target_list.len() != width {
            return Err(ParseError::UnexpectedToken {
                expected: "set-operation inputs with matching widths",
                actual: format!(
                    "set-operation input has {} columns but expected {width}",
                    query.target_list.len()
                ),
            });
        }
    }

    let mut output_types = Vec::with_capacity(width);
    for index in 0..width {
        let mut column_types = inputs
            .iter()
            .map(|query| query.target_list[index].sql_type)
            .collect::<Vec<_>>();
        for input_index in 0..column_types.len() {
            let Some(raw_expr) = set_operation.inputs[input_index]
                .targets
                .get(index)
                .map(|target| &target.expr)
            else {
                continue;
            };
            let Some(peer_type) = column_types
                .iter()
                .enumerate()
                .find(|(peer_index, peer_type)| {
                    *peer_index != input_index && !is_text_like_type(**peer_type)
                })
                .or_else(|| {
                    column_types
                        .iter()
                        .enumerate()
                        .find(|(peer_index, _)| *peer_index != input_index)
                })
                .map(|(_, peer_type)| *peer_type)
            else {
                continue;
            };
            column_types[input_index] = coerce_unknown_set_operation_literal_type(
                raw_expr,
                column_types[input_index],
                peer_type,
            );
        }

        let mut common = column_types[0];
        for next in column_types.iter().copied().skip(1) {
            common = resolve_common_scalar_type(common, next).ok_or_else(|| {
                ParseError::UnexpectedToken {
                    expected: "set-operation column types with a common type",
                    actual: format!(
                        "set-operation column {} has types {} and {}",
                        index + 1,
                        sql_type_name(common),
                        sql_type_name(next)
                    ),
                }
            })?;
        }
        output_types.push(common);
    }

    for query in &mut inputs {
        for (index, common_type) in output_types.iter().copied().enumerate() {
            let target = query
                .target_list
                .get_mut(index)
                .expect("set-operation target width checked earlier");
            if target.sql_type != common_type {
                target.expr = coerce_bound_expr(target.expr.clone(), target.sql_type, common_type);
                target.sql_type = common_type;
            }
        }
    }

    let output_columns = output_names
        .into_iter()
        .zip(output_types.iter().copied())
        .map(|(name, sql_type)| QueryColumn {
            name,
            sql_type,
            wire_type_oid: None,
        })
        .collect::<Vec<_>>();
    let output_exprs = output_columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            Expr::Var(Var {
                varno: 1,
                varattno: user_attrno(index),
                varlevelsup: 0,
                vartype: column.sql_type,
                collation_oid: None,
            })
        })
        .collect::<Vec<_>>();
    let target_list = normalize_target_list(identity_target_list(&output_columns, &output_exprs));
    let desc = RelationDesc {
        columns: output_columns
            .iter()
            .map(|column| column_desc(column.name.clone(), column.sql_type, true))
            .collect(),
    };
    let scope = scope_for_relation(None, &desc);
    let sort_inputs = if stmt.order_by.is_empty() {
        Vec::new()
    } else {
        bind_order_by_items(&stmt.order_by, &target_list, catalog, |expr| {
            bind_expr_with_outer_and_ctes(
                expr,
                &scope,
                catalog,
                outer_scopes,
                grouped_outer.as_ref(),
                visible_ctes,
            )
            .map_err(|err| set_operation_order_by_error_with_input_detail(err, &inputs))
        })?
    };
    let sort_clause = build_sort_clause(sort_inputs, &target_list);
    Ok((
        Query {
            command_type: crate::include::executor::execdesc::CommandType::Select,
            depends_on_row_security: false,
            rtable: Vec::new(),
            jointree: None,
            target_list,
            distinct: false,
            distinct_on: Vec::new(),
            where_qual: None,
            group_by: Vec::new(),
            group_by_refs: Vec::new(),
            grouping_sets: Vec::new(),
            accumulators: Vec::new(),
            window_clauses: Vec::new(),
            having_qual: None,
            sort_clause,
            constraint_deps: Vec::new(),
            limit_count: stmt.limit,
            limit_offset: stmt.offset,
            locking_clause: stmt.locking_clause,
            locking_targets: stmt.locking_targets.clone(),
            locking_nowait: stmt.locking_nowait,
            row_marks: Vec::new(),
            has_target_srfs: false,
            recursive_union: None,
            set_operation: Some(Box::new(SetOperationQuery {
                output_desc: desc.clone(),
                op: stmt.set_operation.as_ref().expect("set operation").op,
                inputs,
            })),
        },
        scope,
    ))
}

fn build_plan_with_outer(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    outer_ctes: &[BoundCte],
    expanded_views: &[u32],
    config: PlannerConfig,
) -> Result<PlannedStmt, ParseError> {
    let (query, _) = analyze_select_query_with_outer(
        stmt,
        catalog,
        outer_scopes,
        grouped_outer,
        None,
        outer_ctes,
        expanded_views,
    )?;
    let [query] = pg_rewrite_query(query, catalog)?
        .try_into()
        .expect("select rewrite should return a single query");
    let query = if config.fold_constants {
        crate::backend::optimizer::fold_query_constants(query)?
    } else {
        query
    };
    planner_with_config(query, catalog, config)
}
