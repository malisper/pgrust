mod agg;
mod agg_output;
mod agg_output_special;
mod coerce;
mod constraints;
mod create_table;
mod create_table_inherits;
mod expr;
mod functions;
mod geometry;
mod infer;
mod modify;
mod paths;
mod query;
mod ranges;
mod scope;
mod system_views;
mod views;
mod window;

use crate::RelFileLocator;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::{Value, cast_value};
use crate::backend::optimizer::planner;
use crate::backend::rewrite::pg_rewrite_query;
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::visible_catalog::VisibleCatalog;
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, PgCastRow, PgClassRow, PgConstraintRow, PgInheritsRow, PgLanguageRow,
    PgOperatorRow, PgProcRow, PgRewriteRow, PgStatisticRow, PgTypeRow, RECORD_TYPE_OID,
    bootstrap_pg_cast_rows, bootstrap_pg_language_rows, bootstrap_pg_operator_rows,
    bootstrap_pg_proc_rows, builtin_type_rows, proc_oid_for_builtin_aggregate_function,
};
use crate::include::nodes::plannodes::{Plan, PlannedStmt};
use crate::include::nodes::primnodes::{
    AggAccum, AggFunc, BuiltinScalarFunction, Expr, JsonTableFunction, OrderByEntry,
    ProjectSetTarget, QueryColumn, RelationDesc, SetReturningCall, SortGroupClause, TargetEntry,
    ToastRelationRef, Var, user_attrno,
};
use std::sync::atomic::{AtomicUsize, Ordering};

use super::parsenodes::*;
pub use crate::backend::catalog::catalog::{Catalog, CatalogEntry};

static NEXT_WORKTABLE_ID: AtomicUsize = AtomicUsize::new(1);
static NEXT_CTE_ID: AtomicUsize = AtomicUsize::new(1);
use crate::backend::utils::cache::relcache::RelCache;
use crate::backend::utils::cache::system_views::{build_pg_stats_rows, build_pg_views_rows};
use agg::*;
use agg_output::*;
pub use coerce::is_binary_coercible_type;
use coerce::*;
pub(crate) use constraints::*;
pub use create_table::*;
pub use create_table_inherits::*;
use expr::*;
use functions::*;
use geometry::*;
use infer::*;
pub use modify::{
    BoundArraySubscript, BoundAssignment, BoundAssignmentTarget, BoundDeleteStatement,
    BoundDeleteTarget, BoundInsertSource, BoundInsertStatement, BoundUpdateStatement,
    BoundUpdateTarget, PreparedInsert, bind_delete, bind_insert, bind_insert_prepared, bind_update,
};
pub use paths::BoundModifyRowSource;
use paths::bind_order_by_items;
pub(crate) use query::analyze_select_query_with_outer;
use query::{
    AnalyzedFrom, analyze_values_query_with_outer, identity_target_list, normalize_target_list,
};
pub use scope::BoundRelation;
use scope::*;
use std::cell::RefCell;
use std::rc::Rc;
use system_views::*;
pub(crate) use views::analyze_view_rule_sql;
use window::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundIndexRelation {
    pub name: String,
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub desc: RelationDesc,
    pub index_meta: crate::backend::utils::cache::relcache::IndexRelCacheEntry,
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
            }
        })
        .collect()
}

fn resolve_aggregate_call(
    catalog: &dyn CatalogLookup,
    func: AggFunc,
    arg_types: &[SqlType],
    func_variadic: bool,
) -> Option<ResolvedFunctionCall> {
    if matches!(
        func,
        AggFunc::Sum | AggFunc::Avg | AggFunc::Variance | AggFunc::Stddev
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

pub trait CatalogLookup {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation>;

    fn materialize_visible_catalog(&self) -> Option<VisibleCatalog> {
        None
    }

    fn relation_by_oid(&self, _relation_oid: u32) -> Option<BoundRelation> {
        None
    }

    fn index_relations_for_heap(&self, _relation_oid: u32) -> Vec<BoundIndexRelation> {
        Vec::new()
    }

    fn lookup_relation(&self, name: &str) -> Option<BoundRelation> {
        self.lookup_any_relation(name)
            .filter(|entry| entry.relkind == 'r')
    }

    fn lookup_relation_by_oid(&self, _relation_oid: u32) -> Option<BoundRelation> {
        None
    }

    fn proc_rows_by_name(&self, name: &str) -> Vec<PgProcRow> {
        let normalized = normalize_catalog_lookup_name(name);
        bootstrap_pg_proc_rows()
            .into_iter()
            .filter(|row| row.proname.eq_ignore_ascii_case(normalized))
            .collect()
    }

    fn proc_row_by_oid(&self, oid: u32) -> Option<PgProcRow> {
        bootstrap_pg_proc_rows()
            .into_iter()
            .find(|row| row.oid == oid)
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
        if sql_type.type_oid != 0 {
            return Some(sql_type.type_oid);
        }
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

    fn constraint_rows_for_relation(&self, _relation_oid: u32) -> Vec<PgConstraintRow> {
        Vec::new()
    }

    fn constraint_rows(&self) -> Vec<PgConstraintRow> {
        Vec::new()
    }

    fn class_row_by_oid(&self, _relation_oid: u32) -> Option<PgClassRow> {
        None
    }

    fn inheritance_parents(&self, _relation_oid: u32) -> Vec<PgInheritsRow> {
        Vec::new()
    }

    fn inheritance_children(&self, _relation_oid: u32) -> Vec<PgInheritsRow> {
        Vec::new()
    }

    fn find_all_inheritors(&self, relation_oid: u32) -> Vec<u32> {
        let mut out = vec![relation_oid];
        let mut pending = vec![relation_oid];
        while let Some(parent_oid) = pending.pop() {
            let mut child_oids = self
                .inheritance_children(parent_oid)
                .into_iter()
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

    fn pg_views_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stats_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }

    fn pg_stat_activity_rows(&self) -> Vec<Vec<Value>> {
        Vec::new()
    }
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

    fn proc_rows_by_name(&self, name: &str) -> Vec<PgProcRow> {
        CatCache::from_catalog(self)
            .proc_rows_by_name(name)
            .into_iter()
            .cloned()
            .collect()
    }

    fn proc_row_by_oid(&self, oid: u32) -> Option<PgProcRow> {
        CatCache::from_catalog(self).proc_by_oid(oid).cloned()
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        let relcache = RelCache::from_catalog(self);
        let mut rows = builtin_type_rows();
        rows.extend(composite_type_rows_from_relcache(&relcache));
        rows
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
            catcache.authid_rows(),
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

    fn relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.get_by_oid(relation_oid).map(|entry| BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            toast: toast_relation_from_cache(self, entry),
            namespace_oid: entry.namespace_oid,
            owner_oid: entry.owner_oid,
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

    fn type_rows(&self) -> Vec<PgTypeRow> {
        let mut rows = builtin_type_rows();
        rows.extend(composite_type_rows_from_relcache(self));
        rows
    }

    fn language_rows(&self) -> Vec<PgLanguageRow> {
        bootstrap_pg_language_rows().to_vec()
    }

    fn materialize_visible_catalog(&self) -> Option<VisibleCatalog> {
        Some(VisibleCatalog::new(self.clone(), None))
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
        relpersistence: entry.relpersistence,
        relkind: entry.relkind,
        desc: entry.desc.clone(),
    }
}

fn composite_type_rows_from_relcache(relcache: &RelCache) -> Vec<PgTypeRow> {
    relcache
        .entries()
        .filter_map(|(name, entry)| {
            (entry.row_type_oid != 0).then(|| PgTypeRow {
                oid: entry.row_type_oid,
                typname: name.rsplit('.').next().unwrap_or(name).to_string(),
                typnamespace: entry.namespace_oid,
                typowner: BOOTSTRAP_SUPERUSER_OID,
                typlen: -1,
                typalign: crate::include::access::htup::AttributeAlign::Double,
                typstorage: crate::include::access::htup::AttributeStorage::Extended,
                typrelid: entry.relation_oid,
                sql_type: SqlType::named_composite(entry.row_type_oid, entry.relation_oid),
            })
        })
        .collect()
}

#[derive(Default)]
pub(crate) struct LiteralDefaultCatalog;

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

pub(crate) fn raw_type_name_hint(raw: &RawTypeName) -> SqlType {
    match raw {
        RawTypeName::Builtin(ty) => *ty,
        RawTypeName::Serial(SerialKind::Small) => SqlType::new(SqlTypeKind::Int2),
        RawTypeName::Serial(SerialKind::Regular) => SqlType::new(SqlTypeKind::Int4),
        RawTypeName::Serial(SerialKind::Big) => SqlType::new(SqlTypeKind::Int8),
        RawTypeName::Named { array_bounds, .. } => {
            let mut ty = builtin_named_type_alias(raw_type_name_name(raw))
                .unwrap_or_else(|| SqlType::new(SqlTypeKind::Composite));
            for _ in 0..*array_bounds {
                ty = SqlType::array_of(ty);
            }
            ty
        }
        RawTypeName::Record => SqlType::record(RECORD_TYPE_OID),
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
        RawTypeName::Named { name, array_bounds } => {
            let mut ty = if let Some(alias) = builtin_named_type_alias(name) {
                alias
            } else {
                catalog
                    .type_rows()
                    .into_iter()
                    .find(|row| row.typname.eq_ignore_ascii_case(name))
                    .map(|row| row.sql_type)
                    .ok_or_else(|| ParseError::UnsupportedType(name.clone()))?
            };
            for _ in 0..*array_bounds {
                ty = SqlType::array_of(ty);
            }
            Ok(ty)
        }
    }
}

fn builtin_named_type_alias(name: &str) -> Option<SqlType> {
    if name.eq_ignore_ascii_case("float") {
        Some(SqlType::new(SqlTypeKind::Float8))
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
            persistence
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

fn apply_cte_column_names(
    mut query: Query,
    desc: RelationDesc,
    column_names: &[String],
) -> Result<(Query, RelationDesc), ParseError> {
    if column_names.is_empty() {
        return Ok((query, desc));
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
    for (index, column) in renamed_desc.columns.iter().enumerate() {
        if let Some(target) = query.target_list.get_mut(index) {
            target.name = column.name.clone();
            target.sql_type = column.sql_type;
            target.resno = index + 1;
        }
    }
    Ok((query, renamed_desc))
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
    match body {
        CteBody::Select(select) => {
            let (query, _) = analyze_select_query_with_outer(
                select,
                catalog,
                outer_scopes,
                grouped_outer,
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
                outer_scopes,
                grouped_outer,
                visible_ctes,
                expanded_views,
            )?;
            let desc = cte_query_desc(&query);
            Ok((query, desc))
        }
        CteBody::RecursiveUnion { .. } => Err(ParseError::FeatureNotSupported(
            "nested recursive UNION CTE bodies".into(),
        )),
    }
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
    let mut bound = Vec::with_capacity(ctes.len());
    for cte in ctes {
        let cte_id = NEXT_CTE_ID.fetch_add(1, Ordering::Relaxed);
        let mut visible = bound.clone();
        visible.extend_from_slice(outer_ctes);
        let (plan, desc) = match &cte.body {
            CteBody::RecursiveUnion {
                all,
                anchor,
                recursive,
            } => {
                if !with_recursive {
                    return Err(ParseError::FeatureNotSupported(
                        "recursive CTE requires WITH RECURSIVE".into(),
                    ));
                }
                let (anchor_query, anchor_desc) = analyze_non_recursive_cte_body(
                    anchor,
                    catalog,
                    outer_scopes,
                    grouped_outer.clone(),
                    &visible,
                    expanded_views,
                )?;
                let (anchor_query, desc) =
                    apply_cte_column_names(anchor_query, anchor_desc, &cte.column_names)?;
                let worktable_id = NEXT_WORKTABLE_ID.fetch_add(1, Ordering::Relaxed);
                let output_columns = desc
                    .columns
                    .iter()
                    .map(|column| QueryColumn {
                        name: column.name.clone(),
                        sql_type: column.sql_type,
                    })
                    .collect::<Vec<_>>();
                let worktable_plan = AnalyzedFrom::worktable(worktable_id, output_columns.clone());
                let mut recursive_visible = visible.clone();
                recursive_visible.push(BoundCte {
                    name: cte.name.clone(),
                    cte_id,
                    plan: Query {
                        command_type: crate::include::executor::execdesc::CommandType::Select,
                        rtable: worktable_plan.rtable.clone(),
                        jointree: worktable_plan.jointree.clone(),
                        target_list: identity_target_list(
                            &output_columns,
                            &worktable_plan.output_exprs,
                        ),
                        where_qual: None,
                        group_by: Vec::new(),
                        accumulators: Vec::new(),
                        window_clauses: Vec::new(),
                        having_qual: None,
                        sort_clause: Vec::new(),
                        limit_count: None,
                        limit_offset: 0,
                        project_set: None,
                        recursive_union: None,
                        set_operation: None,
                    },
                    desc: desc.clone(),
                    self_reference: true,
                    worktable_id,
                });
                let (recursive_query, _) = analyze_select_query_with_outer(
                    recursive,
                    catalog,
                    outer_scopes,
                    grouped_outer.clone(),
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
                        return Err(ParseError::UnexpectedToken {
                            expected: "recursive term column types matching non-recursive term",
                            actual: format!(
                                "recursive CTE column {} has type {} in the non-recursive term but {} in the recursive term",
                                index + 1,
                                sql_type_name(left.sql_type),
                                sql_type_name(right.sql_type)
                            ),
                        });
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
                        rtable: recursive_plan.rtable,
                        jointree: recursive_plan.jointree,
                        target_list,
                        where_qual: None,
                        group_by: Vec::new(),
                        accumulators: Vec::new(),
                        window_clauses: Vec::new(),
                        having_qual: None,
                        sort_clause: Vec::new(),
                        limit_count: None,
                        limit_offset: 0,
                        project_set: None,
                        recursive_union: Some(Box::new(RecursiveUnionQuery {
                            output_desc: desc.clone(),
                            anchor: anchor_query,
                            recursive: recursive_query,
                            distinct: !*all,
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
                )?;
                apply_cte_column_names(query, desc, &cte.column_names)?
            }
        };
        bound.push(BoundCte {
            name: cte.name.clone(),
            cte_id,
            plan,
            desc,
            self_reference: false,
            worktable_id: 0,
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
    build_plan_with_outer(stmt, catalog, &[], None, &[], &[])
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
    build_plan_with_outer(stmt, catalog, &[outer_scope], None, &[], &[])
}

pub fn build_plan(stmt: &SelectStatement, catalog: &dyn CatalogLookup) -> Result<Plan, ParseError> {
    Ok(pg_plan_query(stmt, catalog)?.plan_tree)
}

pub fn pg_plan_values_query(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
) -> Result<PlannedStmt, ParseError> {
    build_values_plan_with_outer(stmt, catalog, &[], None, &[], &[])
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
    build_values_plan_with_outer(stmt, catalog, &[outer_scope], None, &[], &[])
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
    let target_list = normalize_target_list(identity_target_list(
        &base.output_columns,
        &base.output_exprs,
    ));
    let sort_inputs = if stmt.order_by.is_empty() {
        Vec::new()
    } else {
        bind_order_by_items(&stmt.order_by, &target_list, |expr| {
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
            rtable,
            jointree,
            target_list,
            where_qual: None,
            group_by: Vec::new(),
            accumulators: Vec::new(),
            window_clauses: Vec::new(),
            having_qual: None,
            sort_clause,
            limit_count: stmt.limit,
            limit_offset: stmt.offset.unwrap_or(0),
            project_set: None,
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
    Ok(planner(query, catalog))
}

fn bind_select_query_with_outer(
    stmt: &SelectStatement,
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
    let mut visible_ctes = local_ctes;
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

    if stmt.targets.is_empty() && stmt.from.is_none() {
        return Err(ParseError::EmptySelectList);
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
    if let Some(predicate) = &stmt.where_clause {
        if expr_contains_agg(predicate) {
            return Err(ParseError::AggInWhere);
        }
        reject_window_clause(predicate, "WHERE")?;
    }
    for group_expr in &stmt.group_by {
        reject_window_clause(group_expr, "GROUP BY")?;
    }
    if let Some(having) = &stmt.having {
        reject_window_clause(having, "HAVING")?;
    }

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

    let needs_agg =
        !stmt.group_by.is_empty() || targets_contain_agg(&stmt.targets) || stmt.having.is_some();

    if needs_agg
        && select_targets_contain_set_returning_call(
            &stmt.targets,
            &scope,
            catalog,
            outer_scopes,
            grouped_outer.as_ref(),
            &visible_ctes,
        )
    {
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

    if can_skip_scan_for_degenerate_having {
        base = AnalyzedFrom::result();
    }

    let where_qual = if can_skip_scan_for_degenerate_having {
        None
    } else {
        bound_where_qual
    };

    let window_state = Rc::new(RefCell::new(WindowBindingState::default()));

    if needs_agg {
        let mut aggs: Vec<(AggFunc, Vec<SqlFunctionArg>, bool, bool, Option<SqlExpr>)> = Vec::new();
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
        let rewritten_group_keys = group_keys.clone();

        let accumulators: Vec<AggAccum> = aggs
            .iter()
            .map(|(func, args, distinct, func_variadic, filter)| {
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
                let resolved = resolve_aggregate_call(catalog, *func, &arg_types, *func_variadic);
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
                let bound_filter = filter
                    .as_ref()
                    .map(|expr| {
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
                let coerced_args = if let Some(resolved) = &resolved {
                    bound_args
                        .into_iter()
                        .zip(arg_types.iter().copied())
                        .zip(resolved.declared_arg_types.iter().copied())
                        .map(|((arg, actual_type), declared_type)| {
                            coerce_bound_expr(arg, actual_type, declared_type)
                        })
                        .collect()
                } else {
                    bound_args
                };
                Ok(AggAccum {
                    aggfnoid: resolved
                        .as_ref()
                        .map(|call| call.proc_oid)
                        .or_else(|| proc_oid_for_builtin_aggregate_function(*func))
                        .unwrap_or(0),
                    agg_variadic: resolved
                        .as_ref()
                        .map(|call| call.func_variadic)
                        .unwrap_or(*func_variadic),
                    args: coerced_args,
                    filter: bound_filter,
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
        for (func, args, _, _, _) in &aggs {
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
                    &group_keys,
                    &scope,
                    catalog,
                    outer_scopes,
                    grouped_outer.as_ref(),
                    &aggs,
                    n_keys,
                )
            })
            .transpose()?;

        let targets: Vec<TargetEntry> = with_window_binding(window_state.clone(), true, || {
            if stmt.targets.len() == 1
                && matches!(stmt.targets[0].expr, SqlExpr::Column(ref name) if name == "*")
            {
                let mut targets = Vec::with_capacity(output_columns.len());
                for (i, name) in output_columns.iter().enumerate().take(n_keys) {
                    targets.push(TargetEntry::new(
                        name.name.clone(),
                        group_keys.get(i).cloned().unwrap_or_else(|| {
                            panic!(
                                "aggregate SELECT * missing grouped key expr for target position {}",
                                i + 1
                            )
                        }),
                        name.sql_type,
                        i + 1,
                    )
                    .with_input_resno(i + 1));
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
                            accum.args.clone(),
                            accum.filter.clone(),
                            i,
                        ),
                        accum.sql_type,
                        target_index + 1,
                    ));
                }
                Ok(targets)
            } else {
                stmt.targets
                    .iter()
                    .enumerate()
                    .map(|(index, item)| {
                        Ok(TargetEntry::new(
                            item.output_name.clone(),
                            bind_agg_output_expr_in_clause(
                                &item.expr,
                                UngroupedColumnClause::SelectTarget,
                                &stmt.group_by,
                                &group_keys,
                                &scope,
                                catalog,
                                outer_scopes,
                                grouped_outer.as_ref(),
                                &aggs,
                                n_keys,
                            )?,
                            infer_sql_expr_type_with_ctes(
                                &item.expr,
                                &scope,
                                catalog,
                                outer_scopes,
                                grouped_outer.as_ref(),
                                &visible_ctes,
                            ),
                            index + 1,
                        ))
                    })
                    .collect::<Result<_, _>>()
            }
        })?;

        let sort_inputs = with_window_binding(window_state.clone(), true, || {
            if stmt.order_by.is_empty() {
                Ok(Vec::new())
            } else {
                bind_order_by_items(&stmt.order_by, &targets, |expr| {
                    bind_agg_output_expr_in_clause(
                        expr,
                        UngroupedColumnClause::SelectTarget,
                        &stmt.group_by,
                        &group_keys,
                        &scope,
                        catalog,
                        outer_scopes,
                        grouped_outer.as_ref(),
                        &aggs,
                        n_keys,
                    )
                })
            }
        })?;
        let targets = targets;
        let sort_inputs = sort_inputs;
        let sort_clause = build_sort_clause(sort_inputs, &targets);
        let target_list = normalize_target_list(targets);
        let window_clauses = take_window_clauses(&window_state);

        Ok((
            Query {
                command_type: crate::include::executor::execdesc::CommandType::Select,
                rtable: base.rtable,
                jointree: base.jointree,
                target_list,
                where_qual,
                group_by: rewritten_group_keys,
                accumulators,
                window_clauses,
                having_qual: having,
                sort_clause,
                limit_count: stmt.limit,
                limit_offset: stmt.offset.unwrap_or(0),
                project_set: None,
                recursive_union: None,
                set_operation: None,
            },
            scope,
        ))
    } else {
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

        match bound_targets {
            BoundSelectTargets::Plain(targets) => {
                let sort_inputs = with_window_binding(window_state.clone(), true, || {
                    if stmt.order_by.is_empty() {
                        Ok(Vec::new())
                    } else {
                        bind_order_by_items(&stmt.order_by, &targets, |expr| {
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

                Ok((
                    Query {
                        command_type: crate::include::executor::execdesc::CommandType::Select,
                        rtable: base.rtable,
                        jointree: base.jointree,
                        target_list,
                        where_qual,
                        group_by: Vec::new(),
                        accumulators: Vec::new(),
                        window_clauses,
                        having_qual: None,
                        sort_clause,
                        limit_count: stmt.limit,
                        limit_offset: stmt.offset.unwrap_or(0),
                        project_set: None,
                        recursive_union: None,
                        set_operation: None,
                    },
                    scope,
                ))
            }
            BoundSelectTargets::WithProjectSet {
                project_targets,
                final_targets,
            } => {
                let sort_inputs = with_window_binding(window_state.clone(), true, || {
                    if stmt.order_by.is_empty() {
                        Ok(Vec::new())
                    } else {
                        bind_order_by_items(&stmt.order_by, &final_targets, |expr| {
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
                let window_clauses = take_window_clauses(&window_state);
                if !window_clauses.is_empty() {
                    return Err(ParseError::FeatureNotSupported(
                        "queries mixing window functions with select-list set-returning functions"
                            .into(),
                    ));
                }
                let sort_clause = build_sort_clause(sort_inputs, &final_targets);
                let target_list = normalize_target_list(final_targets);
                Ok((
                    Query {
                        command_type: crate::include::executor::execdesc::CommandType::Select,
                        rtable: base.rtable,
                        jointree: base.jointree,
                        target_list,
                        where_qual,
                        group_by: Vec::new(),
                        accumulators: Vec::new(),
                        window_clauses,
                        having_qual: None,
                        sort_clause,
                        limit_count: stmt.limit,
                        limit_offset: stmt.offset.unwrap_or(0),
                        project_set: Some(project_targets),
                        recursive_union: None,
                        set_operation: None,
                    },
                    scope,
                ))
            }
        }
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
            analyze_select_query_with_outer(
                input,
                catalog,
                outer_scopes,
                grouped_outer.clone(),
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
        let mut common = inputs[0].target_list[index].sql_type;
        for query in &inputs[1..] {
            let next = query.target_list[index].sql_type;
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
        .map(|(name, sql_type)| QueryColumn { name, sql_type })
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
        bind_order_by_items(&stmt.order_by, &target_list, |expr| {
            bind_expr_with_outer_and_ctes(
                expr,
                &scope,
                catalog,
                outer_scopes,
                grouped_outer.as_ref(),
                visible_ctes,
            )
        })?
    };
    let sort_clause = build_sort_clause(sort_inputs, &target_list);
    Ok((
        Query {
            command_type: crate::include::executor::execdesc::CommandType::Select,
            rtable: Vec::new(),
            jointree: None,
            target_list,
            where_qual: None,
            group_by: Vec::new(),
            accumulators: Vec::new(),
            window_clauses: Vec::new(),
            having_qual: None,
            sort_clause,
            limit_count: stmt.limit,
            limit_offset: stmt.offset.unwrap_or(0),
            project_set: None,
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
) -> Result<PlannedStmt, ParseError> {
    let (query, _) = analyze_select_query_with_outer(
        stmt,
        catalog,
        outer_scopes,
        grouped_outer,
        outer_ctes,
        expanded_views,
    )?;
    let [query] = pg_rewrite_query(query, catalog)?
        .try_into()
        .expect("select rewrite should return a single query");
    Ok(planner(query, catalog))
}
