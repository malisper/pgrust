// :HACK: Keep the historical root parser/analyze path while semantic analysis
// moves into `pgrust_analyze`.
pub use pgrust_analyze::*;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use crate::backend::catalog::catalog::CatalogError;
pub use crate::backend::catalog::catalog::{Catalog, CatalogEntry};
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::relcache::RelCache;
use crate::backend::utils::cache::visible_catalog::VisibleCatalog;
use crate::include::catalog::{
    PgAttributeRow, PgClassRow, PgConstraintRow, PgIndexRow, PgInheritsRow, PgLanguageRow,
    PgNamespaceRow, PgOperatorRow, PgPartitionedTableRow, PgTypeRow, bootstrap_pg_language_rows,
    bootstrap_pg_namespace_rows, bootstrap_pg_operator_rows, builtin_type_rows,
};
use crate::include::nodes::parsenodes::{ParseError, SqlType};
use crate::include::nodes::pathnodes::PlannerIndexExprCacheEntry;
pub use pgrust_analyze::{
    BoundIndexRelation, BoundRelation, CatalogLookup,
    bound_index_relation_from_relcache_entry_with_heap_and_cache, normalize_create_table_name,
};

fn visible_catalog_from_catalog(catalog: &Catalog) -> VisibleCatalog {
    VisibleCatalog::new(
        RelCache::from_catalog(catalog),
        Some(CatCache::from_catalog(catalog)),
    )
}

impl Catalog {
    pub fn materialize_visible_catalog(&self) -> Option<VisibleCatalog> {
        Some(visible_catalog_from_catalog(self))
    }
}

impl CatalogLookup for Catalog {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation> {
        CatalogLookup::lookup_any_relation(&visible_catalog_from_catalog(self), name)
    }

    fn lookup_relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        CatalogLookup::lookup_relation_by_oid(&visible_catalog_from_catalog(self), relation_oid)
    }

    fn relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        CatalogLookup::relation_by_oid(&visible_catalog_from_catalog(self), relation_oid)
    }

    fn index_relations_for_heap(&self, relation_oid: u32) -> Vec<BoundIndexRelation> {
        CatalogLookup::index_relations_for_heap(&visible_catalog_from_catalog(self), relation_oid)
    }

    fn index_relations_for_heap_with_cache(
        &self,
        relation_oid: u32,
        index_expr_cache: &RefCell<BTreeMap<u32, PlannerIndexExprCacheEntry>>,
    ) -> Vec<BoundIndexRelation> {
        CatalogLookup::index_relations_for_heap_with_cache(
            &visible_catalog_from_catalog(self),
            relation_oid,
            index_expr_cache,
        )
    }

    fn index_row_by_oid(&self, index_oid: u32) -> Option<PgIndexRow> {
        CatalogLookup::index_row_by_oid(&visible_catalog_from_catalog(self), index_oid)
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        CatalogLookup::type_rows(&visible_catalog_from_catalog(self))
    }

    fn type_by_oid(&self, oid: u32) -> Option<PgTypeRow> {
        CatalogLookup::type_by_oid(&visible_catalog_from_catalog(self), oid)
    }

    fn type_by_name(&self, name: &str) -> Option<PgTypeRow> {
        CatalogLookup::type_by_name(&visible_catalog_from_catalog(self), name)
    }

    fn type_oid_for_sql_type(&self, sql_type: SqlType) -> Option<u32> {
        CatalogLookup::type_oid_for_sql_type(&visible_catalog_from_catalog(self), sql_type)
    }

    fn namespace_row_by_oid(&self, oid: u32) -> Option<PgNamespaceRow> {
        CatalogLookup::namespace_row_by_oid(&visible_catalog_from_catalog(self), oid)
    }

    fn namespace_rows(&self) -> Vec<PgNamespaceRow> {
        CatalogLookup::namespace_rows(&visible_catalog_from_catalog(self))
    }

    fn language_rows(&self) -> Vec<PgLanguageRow> {
        CatalogLookup::language_rows(&visible_catalog_from_catalog(self))
    }

    fn language_row_by_oid(&self, oid: u32) -> Option<PgLanguageRow> {
        CatalogLookup::language_row_by_oid(&visible_catalog_from_catalog(self), oid)
    }

    fn language_row_by_name(&self, name: &str) -> Option<PgLanguageRow> {
        CatalogLookup::language_row_by_name(&visible_catalog_from_catalog(self), name)
    }

    fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow> {
        CatalogLookup::class_row_by_oid(&visible_catalog_from_catalog(self), relation_oid)
    }

    fn class_rows(&self) -> Vec<PgClassRow> {
        CatalogLookup::class_rows(&visible_catalog_from_catalog(self))
    }

    fn attribute_rows(&self) -> Vec<PgAttributeRow> {
        CatalogLookup::attribute_rows(&visible_catalog_from_catalog(self))
    }

    fn attribute_rows_for_relation(&self, relation_oid: u32) -> Vec<PgAttributeRow> {
        CatalogLookup::attribute_rows_for_relation(
            &visible_catalog_from_catalog(self),
            relation_oid,
        )
    }

    fn constraint_rows_for_relation(&self, relation_oid: u32) -> Vec<PgConstraintRow> {
        CatalogLookup::constraint_rows_for_relation(
            &visible_catalog_from_catalog(self),
            relation_oid,
        )
    }

    fn constraint_rows(&self) -> Vec<PgConstraintRow> {
        CatalogLookup::constraint_rows(&visible_catalog_from_catalog(self))
    }

    fn rewrite_rows_for_relation(
        &self,
        relation_oid: u32,
    ) -> Vec<pgrust_catalog_data::PgRewriteRow> {
        CatalogLookup::rewrite_rows_for_relation(&visible_catalog_from_catalog(self), relation_oid)
    }

    fn rewrite_rows(&self) -> Vec<pgrust_catalog_data::PgRewriteRow> {
        CatalogLookup::rewrite_rows(&visible_catalog_from_catalog(self))
    }

    fn rewrite_row_by_oid(&self, rewrite_oid: u32) -> Option<pgrust_catalog_data::PgRewriteRow> {
        CatalogLookup::rewrite_row_by_oid(&visible_catalog_from_catalog(self), rewrite_oid)
    }

    fn event_trigger_rows(&self) -> Vec<crate::include::catalog::PgEventTriggerRow> {
        CatalogLookup::event_trigger_rows(&visible_catalog_from_catalog(self))
    }

    fn partitioned_table_row(&self, relation_oid: u32) -> Option<PgPartitionedTableRow> {
        CatalogLookup::partitioned_table_row(&visible_catalog_from_catalog(self), relation_oid)
    }

    fn partitioned_table_rows(&self) -> Vec<PgPartitionedTableRow> {
        CatalogLookup::partitioned_table_rows(&visible_catalog_from_catalog(self))
    }

    fn inheritance_parents(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
        CatalogLookup::inheritance_parents(&visible_catalog_from_catalog(self), relation_oid)
    }

    fn inheritance_children(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
        CatalogLookup::inheritance_children(&visible_catalog_from_catalog(self), relation_oid)
    }

    fn inheritance_rows(&self) -> Vec<PgInheritsRow> {
        CatalogLookup::inheritance_rows(&visible_catalog_from_catalog(self))
    }

    fn operator_by_oid(&self, oid: u32) -> Option<PgOperatorRow> {
        CatalogLookup::operator_by_oid(&visible_catalog_from_catalog(self), oid)
    }

    fn foreign_data_wrapper_rows(&self) -> Vec<crate::include::catalog::PgForeignDataWrapperRow> {
        CatalogLookup::foreign_data_wrapper_rows(&visible_catalog_from_catalog(self))
    }

    fn foreign_server_rows(&self) -> Vec<crate::include::catalog::PgForeignServerRow> {
        CatalogLookup::foreign_server_rows(&visible_catalog_from_catalog(self))
    }

    fn foreign_table_rows(&self) -> Vec<crate::include::catalog::PgForeignTableRow> {
        CatalogLookup::foreign_table_rows(&visible_catalog_from_catalog(self))
    }

    fn user_mapping_rows(&self) -> Vec<crate::include::catalog::PgUserMappingRow> {
        CatalogLookup::user_mapping_rows(&visible_catalog_from_catalog(self))
    }

    fn information_schema_foreign_data_wrappers_rows(
        &self,
        authids: Vec<crate::include::catalog::PgAuthIdRow>,
        auth_members: Vec<crate::include::catalog::PgAuthMembersRow>,
        wrappers: Vec<crate::include::catalog::PgForeignDataWrapperRow>,
        current_user_oid: u32,
    ) -> Vec<Vec<crate::include::nodes::datum::Value>> {
        crate::backend::utils::cache::system_views::build_information_schema_foreign_data_wrappers_rows(
            authids,
            auth_members,
            wrappers,
            current_user_oid,
        )
    }

    fn information_schema_foreign_data_wrapper_options_rows(
        &self,
        authids: Vec<crate::include::catalog::PgAuthIdRow>,
        auth_members: Vec<crate::include::catalog::PgAuthMembersRow>,
        wrappers: Vec<crate::include::catalog::PgForeignDataWrapperRow>,
        current_user_oid: u32,
    ) -> Vec<Vec<crate::include::nodes::datum::Value>> {
        crate::backend::utils::cache::system_views::build_information_schema_foreign_data_wrapper_options_rows(
            authids,
            auth_members,
            wrappers,
            current_user_oid,
        )
    }

    fn information_schema_foreign_servers_rows(
        &self,
        authids: Vec<crate::include::catalog::PgAuthIdRow>,
        auth_members: Vec<crate::include::catalog::PgAuthMembersRow>,
        wrappers: Vec<crate::include::catalog::PgForeignDataWrapperRow>,
        servers: Vec<crate::include::catalog::PgForeignServerRow>,
        current_user_oid: u32,
    ) -> Vec<Vec<crate::include::nodes::datum::Value>> {
        crate::backend::utils::cache::system_views::build_information_schema_foreign_servers_rows(
            authids,
            auth_members,
            wrappers,
            servers,
            current_user_oid,
        )
    }

    fn information_schema_foreign_server_options_rows(
        &self,
        authids: Vec<crate::include::catalog::PgAuthIdRow>,
        auth_members: Vec<crate::include::catalog::PgAuthMembersRow>,
        servers: Vec<crate::include::catalog::PgForeignServerRow>,
        current_user_oid: u32,
    ) -> Vec<Vec<crate::include::nodes::datum::Value>> {
        crate::backend::utils::cache::system_views::build_information_schema_foreign_server_options_rows(
            authids,
            auth_members,
            servers,
            current_user_oid,
        )
    }

    fn information_schema_user_mappings_rows(
        &self,
        authids: Vec<crate::include::catalog::PgAuthIdRow>,
        auth_members: Vec<crate::include::catalog::PgAuthMembersRow>,
        servers: Vec<crate::include::catalog::PgForeignServerRow>,
        mappings: Vec<crate::include::catalog::PgUserMappingRow>,
        current_user_oid: u32,
    ) -> Vec<Vec<crate::include::nodes::datum::Value>> {
        crate::backend::utils::cache::system_views::build_information_schema_user_mappings_rows(
            authids,
            auth_members,
            servers,
            mappings,
            current_user_oid,
        )
    }

    fn information_schema_user_mapping_options_rows(
        &self,
        authids: Vec<crate::include::catalog::PgAuthIdRow>,
        auth_members: Vec<crate::include::catalog::PgAuthMembersRow>,
        servers: Vec<crate::include::catalog::PgForeignServerRow>,
        mappings: Vec<crate::include::catalog::PgUserMappingRow>,
        current_user_oid: u32,
    ) -> Vec<Vec<crate::include::nodes::datum::Value>> {
        crate::backend::utils::cache::system_views::build_information_schema_user_mapping_options_rows(
            authids,
            auth_members,
            servers,
            mappings,
            current_user_oid,
        )
    }

    fn information_schema_usage_privileges_rows(
        &self,
        authids: Vec<crate::include::catalog::PgAuthIdRow>,
        auth_members: Vec<crate::include::catalog::PgAuthMembersRow>,
        wrappers: Vec<crate::include::catalog::PgForeignDataWrapperRow>,
        servers: Vec<crate::include::catalog::PgForeignServerRow>,
        current_user_oid: u32,
    ) -> Vec<Vec<crate::include::nodes::datum::Value>> {
        crate::backend::utils::cache::system_views::build_information_schema_usage_privileges_rows(
            authids,
            auth_members,
            wrappers,
            servers,
            current_user_oid,
        )
    }

    fn information_schema_foreign_tables_rows(
        &self,
        namespaces: Vec<crate::include::catalog::PgNamespaceRow>,
        classes: Vec<crate::include::catalog::PgClassRow>,
        servers: Vec<crate::include::catalog::PgForeignServerRow>,
        tables: Vec<crate::include::catalog::PgForeignTableRow>,
    ) -> Vec<Vec<crate::include::nodes::datum::Value>> {
        crate::backend::utils::cache::system_views::build_information_schema_foreign_tables_rows(
            namespaces, classes, servers, tables,
        )
    }

    fn information_schema_foreign_table_options_rows(
        &self,
        namespaces: Vec<crate::include::catalog::PgNamespaceRow>,
        classes: Vec<crate::include::catalog::PgClassRow>,
        tables: Vec<crate::include::catalog::PgForeignTableRow>,
    ) -> Vec<Vec<crate::include::nodes::datum::Value>> {
        crate::backend::utils::cache::system_views::build_information_schema_foreign_table_options_rows(
            namespaces, classes, tables,
        )
    }
}

impl CatalogLookup for RelCache {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation> {
        self.get_by_name(name)
            .map(|entry| relcache_bound_relation(self, entry))
    }

    fn lookup_relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.get_by_oid(relation_oid)
            .map(|entry| relcache_bound_relation(self, entry))
    }

    fn relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.get_by_oid(relation_oid)
            .map(|entry| relcache_bound_relation(self, entry))
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
            .map(|entry| relcache_bound_relation(self, entry));
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
        rows.extend(relcache_composite_type_rows(self));
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

    fn partitioned_table_row(&self, relation_oid: u32) -> Option<PgPartitionedTableRow> {
        self.get_by_oid(relation_oid)
            .and_then(|entry| entry.partitioned_table.clone())
    }
}

fn relcache_bound_relation(
    relcache: &RelCache,
    entry: &crate::backend::utils::cache::relcache::RelCacheEntry,
) -> BoundRelation {
    BoundRelation {
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        toast: (entry.reltoastrelid != 0)
            .then(|| relcache.get_by_oid(entry.reltoastrelid))
            .flatten()
            .map(|toast| crate::include::nodes::primnodes::ToastRelationRef {
                rel: toast.rel,
                relation_oid: toast.relation_oid,
            }),
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

fn relcache_composite_type_rows(relcache: &RelCache) -> Vec<PgTypeRow> {
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    for (name, entry) in relcache.entries() {
        let relname = name.rsplit('.').next().unwrap_or(name);
        if entry.row_type_oid != 0 && seen.insert(entry.row_type_oid) {
            rows.push(crate::include::catalog::composite_type_row_with_owner(
                relname,
                entry.row_type_oid,
                entry.namespace_oid,
                entry.owner_oid,
                entry.relation_oid,
                entry.array_type_oid,
            ));
        }
        if entry.array_type_oid != 0 && seen.insert(entry.array_type_oid) {
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
    }
    rows
}

pub fn bind_create_table(
    stmt: &crate::include::nodes::parsenodes::CreateTableStatement,
    catalog: &mut Catalog,
) -> Result<CatalogEntry, ParseError> {
    let (table_name, _) = normalize_create_table_name(stmt)?;
    catalog
        .create_table(table_name, create_relation_desc(stmt, catalog)?)
        .map_err(catalog_error_to_parse_error)
}

pub fn create_relation_desc(
    stmt: &crate::include::nodes::parsenodes::CreateTableStatement,
    catalog: &dyn CatalogLookup,
) -> Result<crate::include::nodes::primnodes::RelationDesc, ParseError> {
    with_root_analyze_services_and_notices(|| pgrust_analyze::create_relation_desc(stmt, catalog))
}

pub fn lower_create_table_with_catalog(
    stmt: &crate::include::nodes::parsenodes::CreateTableStatement,
    catalog: &dyn CatalogLookup,
    persistence: crate::include::nodes::parsenodes::TablePersistence,
) -> Result<pgrust_analyze::LoweredCreateTable, ParseError> {
    with_root_analyze_services_and_notices(|| {
        pgrust_analyze::lower_create_table_with_catalog(stmt, catalog, persistence)
    })
}

pub fn lower_partition_bound_for_relation(
    relation: &pgrust_analyze::BoundRelation,
    bound: &crate::include::nodes::parsenodes::RawPartitionBoundSpec,
    catalog: &dyn CatalogLookup,
) -> Result<pgrust_analyze::PartitionBoundSpec, ParseError> {
    with_root_analyze_services_and_notices(|| {
        pgrust_analyze::lower_partition_bound_for_relation(relation, bound, catalog)
    })
}

pub fn bind_expr_with_outer_and_ctes(
    expr: &crate::include::nodes::parsenodes::SqlExpr,
    scope: &pgrust_analyze::BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[pgrust_analyze::BoundScope],
    grouped_outer: Option<&pgrust_analyze::GroupedOuterScope>,
    ctes: &[pgrust_analyze::BoundCte],
) -> Result<crate::include::nodes::primnodes::Expr, ParseError> {
    with_root_analyze_services_and_notices(|| {
        pgrust_analyze::bind_expr_with_outer_and_ctes(
            expr,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
        )
    })
}

pub fn bind_scalar_expr_in_named_slot_scope(
    expr: &crate::include::nodes::parsenodes::SqlExpr,
    relation_scopes: &[(String, Vec<pgrust_analyze::SlotScopeColumn>)],
    columns: &[pgrust_analyze::SlotScopeColumn],
    catalog: &dyn CatalogLookup,
    ctes: &[pgrust_analyze::BoundCte],
) -> Result<(crate::include::nodes::primnodes::Expr, SqlType), ParseError> {
    with_root_analyze_services_and_notices(|| {
        pgrust_analyze::bind_scalar_expr_in_named_slot_scope(
            expr,
            relation_scopes,
            columns,
            catalog,
            ctes,
        )
    })
}

fn catalog_error_to_parse_error(err: CatalogError) -> ParseError {
    match err {
        CatalogError::TableAlreadyExists(name) => ParseError::TableAlreadyExists(name),
        CatalogError::UnknownTable(name) => ParseError::TableDoesNotExist(name),
        CatalogError::UnknownColumn(name) => ParseError::UnknownColumn(name),
        CatalogError::UnknownType(name) => ParseError::UnsupportedType(name),
        CatalogError::UniqueViolation(name) => {
            let _ = name;
            ParseError::UnexpectedToken {
                expected: "valid catalog state",
                actual: "catalog error".into(),
            }
        }
        CatalogError::TypeAlreadyExists(_)
        | CatalogError::Io(_)
        | CatalogError::Corrupt(_)
        | CatalogError::Interrupted(_) => ParseError::UnexpectedToken {
            expected: "valid catalog state",
            actual: "catalog error".into(),
        },
    }
}

struct RootAnalyzeServices;

static ROOT_ANALYZE_SERVICES: RootAnalyzeServices = RootAnalyzeServices;

pub(crate) fn with_root_analyze_services<T>(f: impl FnOnce() -> T) -> T {
    pgrust_analyze::with_analyze_services(&ROOT_ANALYZE_SERVICES, f)
}

fn drain_analyzer_notices_to_backend() {
    for notice in pgrust_analyze::take_notices() {
        if let Some(detail) = notice.detail {
            crate::backend::utils::misc::notices::push_notice_with_detail(notice.message, detail);
        } else {
            crate::backend::utils::misc::notices::push_notice(notice.message);
        }
    }
}

fn with_root_analyze_services_and_notices<T>(f: impl FnOnce() -> T) -> T {
    pgrust_analyze::clear_notices();
    let result = with_root_analyze_services(f);
    drain_analyzer_notices_to_backend();
    result
}

fn parse_error_from_exec(err: crate::backend::executor::ExecError) -> ParseError {
    match err {
        crate::backend::executor::ExecError::Parse(err) => err,
        crate::backend::executor::ExecError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        } => ParseError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        },
        crate::backend::executor::ExecError::DivisionByZero(_) => ParseError::DetailedError {
            message: "division by zero".into(),
            detail: None,
            hint: None,
            sqlstate: "22012",
        },
        other => {
            ParseError::FeatureNotSupportedMessage(format!("analyze service failed: {other:?}"))
        }
    }
}

impl pgrust_analyze::AnalyzeServices for RootAnalyzeServices {
    fn cast_value(
        &self,
        value: crate::include::nodes::datum::Value,
        ty: SqlType,
    ) -> Result<crate::include::nodes::datum::Value, ParseError> {
        crate::backend::executor::cast_value(value, ty).map_err(parse_error_from_exec)
    }

    fn cast_value_with_source_type(
        &self,
        value: crate::include::nodes::datum::Value,
        source_type: Option<SqlType>,
        ty: SqlType,
        catalog: Option<&dyn CatalogLookup>,
    ) -> Result<crate::include::nodes::datum::Value, ParseError> {
        crate::backend::executor::cast_value_with_source_type_catalog_and_config(
            value,
            source_type,
            ty,
            catalog,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        )
        .map_err(parse_error_from_exec)
    }

    fn compare_order_values(
        &self,
        left: &crate::include::nodes::datum::Value,
        right: &crate::include::nodes::datum::Value,
        collation_oid: Option<u32>,
        nulls_first: Option<bool>,
        descending: bool,
    ) -> Result<std::cmp::Ordering, ParseError> {
        crate::backend::executor::compare_order_values(
            left,
            right,
            collation_oid,
            nulls_first,
            descending,
        )
        .map_err(parse_error_from_exec)
    }

    fn fold_expr_constants(
        &self,
        expr: crate::include::nodes::primnodes::Expr,
    ) -> Result<crate::include::nodes::primnodes::Expr, ParseError> {
        crate::backend::optimizer::fold_expr_constants(expr)
    }

    fn fold_query_constants(
        &self,
        query: crate::include::nodes::parsenodes::Query,
    ) -> Result<crate::include::nodes::parsenodes::Query, ParseError> {
        crate::backend::optimizer::fold_query_constants(query)
    }

    fn planner_with_config(
        &self,
        query: crate::include::nodes::parsenodes::Query,
        catalog: &dyn CatalogLookup,
        config: crate::include::nodes::pathnodes::PlannerConfig,
    ) -> Result<crate::include::nodes::plannodes::PlannedStmt, ParseError> {
        crate::backend::optimizer::planner_with_config(query, catalog, config)
    }

    fn pg_rewrite_query(
        &self,
        query: crate::include::nodes::parsenodes::Query,
        catalog: &dyn CatalogLookup,
    ) -> Result<Vec<crate::include::nodes::parsenodes::Query>, ParseError> {
        crate::backend::rewrite::pg_rewrite_query(query, catalog)
    }

    fn render_relation_expr_sql(
        &self,
        expr: &crate::include::nodes::primnodes::Expr,
        relation_name: Option<&str>,
        desc: &crate::include::nodes::primnodes::RelationDesc,
        catalog: &dyn CatalogLookup,
    ) -> String {
        crate::backend::rewrite::render_relation_expr_sql(expr, relation_name, desc, catalog)
    }

    fn render_relation_expr_sql_for_information_schema(
        &self,
        expr: &crate::include::nodes::primnodes::Expr,
        relation_name: Option<&str>,
        desc: &crate::include::nodes::primnodes::RelationDesc,
        catalog: &dyn CatalogLookup,
    ) -> String {
        crate::backend::rewrite::render_relation_expr_sql_for_information_schema(
            expr,
            relation_name,
            desc,
            catalog,
        )
    }

    fn format_view_definition(
        &self,
        relation_oid: u32,
        relation_desc: &crate::include::nodes::primnodes::RelationDesc,
        catalog: &dyn CatalogLookup,
    ) -> Result<String, ParseError> {
        crate::backend::rewrite::format_view_definition(relation_oid, relation_desc, catalog)
    }

    fn split_stored_view_definition_sql<'a>(
        &self,
        sql: &'a str,
    ) -> (&'a str, crate::include::nodes::parsenodes::ViewCheckOption) {
        crate::backend::rewrite::split_stored_view_definition_sql(sql)
    }

    fn load_view_return_query(
        &self,
        relation_oid: u32,
        relation_desc: &crate::include::nodes::primnodes::RelationDesc,
        alias: Option<&str>,
        catalog: &dyn CatalogLookup,
        expanded_views: &[u32],
    ) -> Result<crate::include::nodes::parsenodes::Query, ParseError> {
        crate::backend::rewrite::load_view_return_query(
            relation_oid,
            relation_desc,
            alias,
            catalog,
            expanded_views,
        )
    }

    fn load_view_return_select(
        &self,
        relation_oid: u32,
        alias: Option<&str>,
        catalog: &dyn CatalogLookup,
        expanded_views: &[u32],
    ) -> Result<crate::include::nodes::parsenodes::SelectStatement, ParseError> {
        crate::backend::rewrite::load_view_return_select(
            relation_oid,
            alias,
            catalog,
            expanded_views,
        )
    }

    fn classify_view_dml_rules(
        &self,
        relation_oid: u32,
        event: pgrust_analyze::ViewDmlEvent,
        catalog: &dyn CatalogLookup,
    ) -> pgrust_analyze::ViewRuleEventClassification {
        crate::backend::rewrite::classify_view_dml_rules(relation_oid, event, catalog)
    }

    fn resolve_auto_updatable_view_target(
        &self,
        relation_oid: u32,
        relation_desc: &crate::include::nodes::primnodes::RelationDesc,
        event: pgrust_analyze::ViewDmlEvent,
        catalog: &dyn CatalogLookup,
        expanded_views: &[u32],
    ) -> Result<pgrust_analyze::ResolvedAutoViewTarget, pgrust_analyze::ViewDmlRewriteError> {
        crate::backend::rewrite::resolve_auto_updatable_view_target(
            relation_oid,
            relation_desc,
            event,
            catalog,
            expanded_views,
        )
    }

    fn relation_has_row_security(&self, relation_oid: u32, catalog: &dyn CatalogLookup) -> bool {
        crate::backend::rewrite::relation_has_row_security(relation_oid, catalog)
    }

    fn apply_query_row_security(
        &self,
        query: &mut crate::include::nodes::parsenodes::Query,
        catalog: &dyn CatalogLookup,
    ) -> Result<(), ParseError> {
        crate::backend::rewrite::apply_query_row_security(query, catalog)
    }

    fn build_target_relation_row_security(
        &self,
        relation_name: &str,
        relation_oid: u32,
        desc: &crate::include::nodes::primnodes::RelationDesc,
        command: crate::include::catalog::PolicyCommand,
        include_select_visibility: bool,
        include_select_check: bool,
        catalog: &dyn CatalogLookup,
    ) -> Result<pgrust_analyze::TargetRlsState, ParseError> {
        crate::backend::rewrite::build_target_relation_row_security(
            relation_name,
            relation_oid,
            desc,
            command,
            include_select_visibility,
            include_select_check,
            catalog,
        )
    }

    fn build_target_relation_row_security_for_user(
        &self,
        relation_name: &str,
        relation_oid: u32,
        desc: &crate::include::nodes::primnodes::RelationDesc,
        command: crate::include::catalog::PolicyCommand,
        include_select_visibility: bool,
        include_select_check: bool,
        user_oid: u32,
        catalog: &dyn CatalogLookup,
    ) -> Result<pgrust_analyze::TargetRlsState, ParseError> {
        crate::backend::rewrite::build_target_relation_row_security_for_user(
            relation_name,
            relation_oid,
            desc,
            command,
            include_select_visibility,
            include_select_check,
            user_oid,
            catalog,
        )
    }

    fn current_timestamp_value(
        &self,
        precision: Option<i32>,
        with_time_zone: bool,
    ) -> crate::include::nodes::datum::Value {
        crate::backend::executor::current_timestamp_value(precision, with_time_zone)
    }

    fn eval_to_char_function(
        &self,
        values: &[crate::include::nodes::datum::Value],
    ) -> Result<crate::include::nodes::datum::Value, ParseError> {
        crate::backend::executor::eval_to_char_function(
            values,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        )
        .map_err(parse_error_from_exec)
    }

    fn format_trigger_definition(
        &self,
        row: &crate::include::catalog::PgTriggerRow,
        _relation_name: Option<&str>,
        catalog: &dyn CatalogLookup,
    ) -> Option<pgrust_analyze::FormattedTriggerDefinition> {
        let formatted =
            crate::backend::utils::trigger::format_trigger_definition(catalog, row, true)?;
        Some(pgrust_analyze::FormattedTriggerDefinition {
            definition: formatted.definition,
            event_manipulations: formatted.event_manipulations,
            action_condition: formatted.action_condition,
            action_statement: formatted.action_statement,
            action_orientation: formatted.action_orientation,
            action_timing: formatted.action_timing,
            action_reference_old_table: formatted.action_reference_old_table,
            action_reference_new_table: formatted.action_reference_new_table,
        })
    }
}

pub fn pg_plan_query(
    stmt: &crate::include::nodes::parsenodes::SelectStatement,
    catalog: &dyn CatalogLookup,
) -> Result<crate::include::nodes::plannodes::PlannedStmt, ParseError> {
    with_root_analyze_services(|| pgrust_analyze::pg_plan_query(stmt, catalog))
}

pub fn pg_plan_query_with_config(
    stmt: &crate::include::nodes::parsenodes::SelectStatement,
    catalog: &dyn CatalogLookup,
    config: crate::include::nodes::pathnodes::PlannerConfig,
) -> Result<crate::include::nodes::plannodes::PlannedStmt, ParseError> {
    with_root_analyze_services(|| pgrust_analyze::pg_plan_query_with_config(stmt, catalog, config))
}

pub fn pg_plan_query_with_outer(
    stmt: &crate::include::nodes::parsenodes::SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_columns: &[(String, SqlType)],
) -> Result<crate::include::nodes::plannodes::PlannedStmt, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::pg_plan_query_with_outer(stmt, catalog, outer_columns)
    })
}

pub fn pg_plan_query_with_sql_function_args(
    stmt: &crate::include::nodes::parsenodes::SelectStatement,
    catalog: &dyn CatalogLookup,
    input_args: &[(Option<String>, SqlType)],
) -> Result<crate::include::nodes::plannodes::PlannedStmt, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::pg_plan_query_with_sql_function_args(stmt, catalog, input_args)
    })
}

pub fn pg_plan_query_with_outer_scopes(
    stmt: &crate::include::nodes::parsenodes::SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[pgrust_analyze::BoundScope],
) -> Result<crate::include::nodes::plannodes::PlannedStmt, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::pg_plan_query_with_outer_scopes(stmt, catalog, outer_scopes)
    })
}

pub fn pg_plan_query_with_outer_scopes_and_ctes(
    stmt: &crate::include::nodes::parsenodes::SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[pgrust_analyze::BoundScope],
    outer_ctes: &[pgrust_analyze::BoundCte],
) -> Result<crate::include::nodes::plannodes::PlannedStmt, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::pg_plan_query_with_outer_scopes_and_ctes(
            stmt,
            catalog,
            outer_scopes,
            outer_ctes,
        )
    })
}

pub fn pg_plan_query_with_outer_scopes_and_ctes_config(
    stmt: &crate::include::nodes::parsenodes::SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[pgrust_analyze::BoundScope],
    outer_ctes: &[pgrust_analyze::BoundCte],
    config: crate::include::nodes::pathnodes::PlannerConfig,
) -> Result<crate::include::nodes::plannodes::PlannedStmt, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::pg_plan_query_with_outer_scopes_and_ctes_config(
            stmt,
            catalog,
            outer_scopes,
            outer_ctes,
            config,
        )
    })
}

pub fn analyze_select_query_with_outer(
    stmt: &crate::include::nodes::parsenodes::SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[pgrust_analyze::BoundScope],
    grouped_outer: Option<pgrust_analyze::GroupedOuterScope>,
    visible_agg_scope: Option<&pgrust_analyze::VisibleAggregateScope>,
    outer_ctes: &[pgrust_analyze::BoundCte],
    expanded_views: &[u32],
) -> Result<
    (
        crate::include::nodes::parsenodes::Query,
        pgrust_analyze::BoundScope,
    ),
    ParseError,
> {
    with_root_analyze_services(|| {
        pgrust_analyze::analyze_select_query_with_outer(
            stmt,
            catalog,
            outer_scopes,
            grouped_outer,
            visible_agg_scope,
            outer_ctes,
            expanded_views,
        )
    })
}

pub fn build_plan(
    stmt: &crate::include::nodes::parsenodes::SelectStatement,
    catalog: &dyn CatalogLookup,
) -> Result<crate::include::nodes::plannodes::Plan, ParseError> {
    with_root_analyze_services(|| pgrust_analyze::build_plan(stmt, catalog))
}

pub fn pg_plan_values_query(
    stmt: &crate::include::nodes::parsenodes::ValuesStatement,
    catalog: &dyn CatalogLookup,
) -> Result<crate::include::nodes::plannodes::PlannedStmt, ParseError> {
    with_root_analyze_services(|| pgrust_analyze::pg_plan_values_query(stmt, catalog))
}

pub fn pg_plan_values_query_with_config(
    stmt: &crate::include::nodes::parsenodes::ValuesStatement,
    catalog: &dyn CatalogLookup,
    config: crate::include::nodes::pathnodes::PlannerConfig,
) -> Result<crate::include::nodes::plannodes::PlannedStmt, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::pg_plan_values_query_with_config(stmt, catalog, config)
    })
}

pub fn pg_plan_values_query_with_outer(
    stmt: &crate::include::nodes::parsenodes::ValuesStatement,
    catalog: &dyn CatalogLookup,
    outer_columns: &[(String, SqlType)],
) -> Result<crate::include::nodes::plannodes::PlannedStmt, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::pg_plan_values_query_with_outer(stmt, catalog, outer_columns)
    })
}

pub fn pg_plan_values_query_with_sql_function_args(
    stmt: &crate::include::nodes::parsenodes::ValuesStatement,
    catalog: &dyn CatalogLookup,
    input_args: &[(Option<String>, SqlType)],
) -> Result<crate::include::nodes::plannodes::PlannedStmt, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::pg_plan_values_query_with_sql_function_args(stmt, catalog, input_args)
    })
}

pub fn pg_plan_values_query_with_outer_scopes(
    stmt: &crate::include::nodes::parsenodes::ValuesStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[pgrust_analyze::BoundScope],
) -> Result<crate::include::nodes::plannodes::PlannedStmt, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::pg_plan_values_query_with_outer_scopes(stmt, catalog, outer_scopes)
    })
}

pub fn pg_plan_values_query_with_outer_scopes_and_ctes(
    stmt: &crate::include::nodes::parsenodes::ValuesStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[pgrust_analyze::BoundScope],
    outer_ctes: &[pgrust_analyze::BoundCte],
) -> Result<crate::include::nodes::plannodes::PlannedStmt, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::pg_plan_values_query_with_outer_scopes_and_ctes(
            stmt,
            catalog,
            outer_scopes,
            outer_ctes,
        )
    })
}

pub fn pg_plan_values_query_with_outer_scopes_and_ctes_config(
    stmt: &crate::include::nodes::parsenodes::ValuesStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[pgrust_analyze::BoundScope],
    outer_ctes: &[pgrust_analyze::BoundCte],
    config: crate::include::nodes::pathnodes::PlannerConfig,
) -> Result<crate::include::nodes::plannodes::PlannedStmt, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::pg_plan_values_query_with_outer_scopes_and_ctes_config(
            stmt,
            catalog,
            outer_scopes,
            outer_ctes,
            config,
        )
    })
}

pub fn bind_insert(
    stmt: &crate::include::nodes::parsenodes::InsertStatement,
    catalog: &dyn CatalogLookup,
) -> Result<pgrust_analyze::BoundInsertStatement, ParseError> {
    with_root_analyze_services(|| pgrust_analyze::bind_insert(stmt, catalog))
}

pub fn bind_insert_with_outer_scopes(
    stmt: &crate::include::nodes::parsenodes::InsertStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[pgrust_analyze::BoundScope],
) -> Result<pgrust_analyze::BoundInsertStatement, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::bind_insert_with_outer_scopes(stmt, catalog, outer_scopes)
    })
}

pub fn bind_insert_with_outer_scopes_and_ctes(
    stmt: &crate::include::nodes::parsenodes::InsertStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[pgrust_analyze::BoundScope],
    outer_ctes: &[pgrust_analyze::BoundCte],
) -> Result<pgrust_analyze::BoundInsertStatement, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::bind_insert_with_outer_scopes_and_ctes(
            stmt,
            catalog,
            outer_scopes,
            outer_ctes,
        )
    })
}

pub fn bind_update(
    stmt: &crate::include::nodes::parsenodes::UpdateStatement,
    catalog: &dyn CatalogLookup,
) -> Result<pgrust_analyze::BoundUpdateStatement, ParseError> {
    with_root_analyze_services(|| pgrust_analyze::bind_update(stmt, catalog))
}

pub fn bind_update_with_outer_scopes(
    stmt: &crate::include::nodes::parsenodes::UpdateStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[pgrust_analyze::BoundScope],
) -> Result<pgrust_analyze::BoundUpdateStatement, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::bind_update_with_outer_scopes(stmt, catalog, outer_scopes)
    })
}

pub fn bind_update_with_outer_scopes_and_ctes(
    stmt: &crate::include::nodes::parsenodes::UpdateStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[pgrust_analyze::BoundScope],
    outer_ctes: &[pgrust_analyze::BoundCte],
) -> Result<pgrust_analyze::BoundUpdateStatement, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::bind_update_with_outer_scopes_and_ctes(
            stmt,
            catalog,
            outer_scopes,
            outer_ctes,
        )
    })
}

pub fn bind_delete(
    stmt: &crate::include::nodes::parsenodes::DeleteStatement,
    catalog: &dyn CatalogLookup,
) -> Result<pgrust_analyze::BoundDeleteStatement, ParseError> {
    with_root_analyze_services(|| pgrust_analyze::bind_delete(stmt, catalog))
}

pub fn bind_delete_with_outer_scopes(
    stmt: &crate::include::nodes::parsenodes::DeleteStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[pgrust_analyze::BoundScope],
) -> Result<pgrust_analyze::BoundDeleteStatement, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::bind_delete_with_outer_scopes(stmt, catalog, outer_scopes)
    })
}

pub fn bind_delete_with_outer_scopes_and_ctes(
    stmt: &crate::include::nodes::parsenodes::DeleteStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[pgrust_analyze::BoundScope],
    outer_ctes: &[pgrust_analyze::BoundCte],
) -> Result<pgrust_analyze::BoundDeleteStatement, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::bind_delete_with_outer_scopes_and_ctes(
            stmt,
            catalog,
            outer_scopes,
            outer_ctes,
        )
    })
}

pub fn plan_merge(
    stmt: &crate::include::nodes::parsenodes::MergeStatement,
    catalog: &dyn CatalogLookup,
) -> Result<pgrust_analyze::BoundMergeStatement, ParseError> {
    with_root_analyze_services(|| pgrust_analyze::plan_merge(stmt, catalog))
}

pub fn plan_merge_with_outer_ctes(
    stmt: &crate::include::nodes::parsenodes::MergeStatement,
    catalog: &dyn CatalogLookup,
    outer_ctes: &[pgrust_analyze::BoundCte],
) -> Result<pgrust_analyze::BoundMergeStatement, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::plan_merge_with_outer_ctes(stmt, catalog, outer_ctes)
    })
}

pub fn plan_merge_with_outer_scopes_and_ctes(
    stmt: &crate::include::nodes::parsenodes::MergeStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[pgrust_analyze::BoundScope],
    outer_ctes: &[pgrust_analyze::BoundCte],
) -> Result<pgrust_analyze::BoundMergeStatement, ParseError> {
    with_root_analyze_services(|| {
        pgrust_analyze::plan_merge_with_outer_scopes_and_ctes(
            stmt,
            catalog,
            outer_scopes,
            outer_ctes,
        )
    })
}
