use crate::backend::catalog::pg_constraint::derived_pg_constraint_rows;
use crate::backend::parser::analyze::bound_index_relation_from_relcache_entry;
use crate::backend::parser::{BoundRelation, CatalogLookup};
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::relcache::RelCache;
use crate::backend::utils::cache::system_views::{
    build_pg_indexes_rows, build_pg_locks_rows, build_pg_matviews_rows, build_pg_policies_rows,
    build_pg_rules_rows, build_pg_stat_io_rows, build_pg_stat_user_functions_rows,
    build_pg_stat_user_tables_rows, build_pg_statio_user_tables_rows, build_pg_stats_rows,
    build_pg_views_rows,
};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, PgAggregateRow, PgAmprocRow, PgAuthIdRow, PgAuthMembersRow, PgCastRow,
    PgClassRow, PgCollationRow, PgConstraintRow, PgDatabaseRow, PgDependRow, PgEnumRow,
    PgForeignDataWrapperRow, PgInheritsRow, PgLanguageRow, PgNamespaceRow, PgOpclassRow,
    PgOperatorRow, PgPartitionedTableRow, PgPolicyRow, PgProcRow, PgRangeRow, PgRewriteRow,
    PgStatisticExtDataRow, PgStatisticExtRow, PgStatisticRow, PgTriggerRow, PgTsConfigRow,
    PgTsDictRow, PgTypeRow, bootstrap_pg_aggregate_rows, bootstrap_pg_amproc_rows,
    bootstrap_pg_cast_rows, bootstrap_pg_collation_rows, bootstrap_pg_database_rows,
    bootstrap_pg_enum_rows, bootstrap_pg_language_rows, bootstrap_pg_namespace_rows,
    bootstrap_pg_opclass_rows, bootstrap_pg_operator_rows, bootstrap_pg_proc_rows,
    bootstrap_pg_ts_config_rows, bootstrap_pg_ts_dict_rows, builtin_range_rows, builtin_type_rows,
    synthetic_range_proc_rows_by_name,
};
use crate::pgrust::database::DatabaseStatsStore;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub struct VisibleCatalog {
    relcache: RelCache,
    catcache: Option<CatCache>,
    search_path: Vec<String>,
    enum_rows: Vec<PgEnumRow>,
    uncommitted_enum_label_oids: BTreeSet<u32>,
    domain_checks: BTreeMap<u32, (String, Vec<u32>)>,
    dynamic_type_rows: Vec<PgTypeRow>,
}

impl VisibleCatalog {
    pub fn new(relcache: RelCache, catcache: Option<CatCache>) -> Self {
        Self::with_search_path(relcache, catcache, Vec::new())
    }

    pub fn with_search_path(
        relcache: RelCache,
        catcache: Option<CatCache>,
        search_path: Vec<String>,
    ) -> Self {
        Self {
            relcache,
            catcache,
            search_path,
            enum_rows: Vec::new(),
            uncommitted_enum_label_oids: BTreeSet::new(),
            domain_checks: BTreeMap::new(),
            dynamic_type_rows: Vec::new(),
        }
    }

    pub fn relcache(&self) -> &RelCache {
        &self.relcache
    }

    pub fn with_enum_rows(mut self, enum_rows: Vec<PgEnumRow>) -> Self {
        self.enum_rows = enum_rows;
        self
    }

    pub fn with_uncommitted_enum_label_oids(mut self, label_oids: Vec<u32>) -> Self {
        self.uncommitted_enum_label_oids = label_oids.into_iter().collect();
        self
    }

    pub fn with_domain_checks(mut self, checks: BTreeMap<u32, (String, Vec<u32>)>) -> Self {
        self.domain_checks = checks;
        self
    }

    pub fn with_dynamic_type_rows(mut self, rows: Vec<PgTypeRow>) -> Self {
        self.dynamic_type_rows = rows;
        self
    }

    pub fn constraint_rows_for_relation(&self, relation_oid: u32) -> Vec<PgConstraintRow> {
        if let Some(catcache) = &self.catcache {
            return catcache.constraint_rows_for_relation(relation_oid);
        }
        let Some((name, entry)) = self
            .relcache
            .entries()
            .find(|(_, entry)| entry.relation_oid == relation_oid)
        else {
            return Vec::new();
        };
        let relname = name.rsplit('.').next().unwrap_or(name);
        derived_pg_constraint_rows(relation_oid, relname, entry.namespace_oid, &entry.desc)
    }

    pub fn constraint_rows(&self) -> Vec<PgConstraintRow> {
        if let Some(catcache) = &self.catcache {
            return catcache.constraint_rows();
        }
        self.relcache
            .entries()
            .flat_map(|(name, entry)| {
                let relname = name.rsplit('.').next().unwrap_or(name);
                derived_pg_constraint_rows(
                    entry.relation_oid,
                    relname,
                    entry.namespace_oid,
                    &entry.desc,
                )
            })
            .collect()
    }

    pub fn trigger_rows_for_relation(&self, relation_oid: u32) -> Vec<PgTriggerRow> {
        self.catcache
            .as_ref()
            .map(|catcache| catcache.trigger_rows_for_relation(relation_oid))
            .unwrap_or_default()
    }

    pub fn depend_rows(&self) -> Vec<PgDependRow> {
        self.catcache
            .as_ref()
            .map(CatCache::depend_rows)
            .unwrap_or_default()
    }

    pub fn policy_rows_for_relation(&self, relation_oid: u32) -> Vec<PgPolicyRow> {
        self.catcache
            .as_ref()
            .map(|catcache| catcache.policy_rows_for_relation(relation_oid))
            .unwrap_or_default()
    }

    pub fn has_index_on_relation(&self, relation_oid: u32) -> bool {
        self.catcache
            .as_ref()
            .map(|catcache| {
                catcache
                    .index_rows()
                    .into_iter()
                    .any(|row| row.indrelid == relation_oid)
            })
            .unwrap_or(false)
    }

    pub fn access_method_name_for_relation(&self, relation_oid: u32) -> Option<String> {
        if let Some(catcache) = &self.catcache {
            let relam = catcache.class_by_oid(relation_oid)?.relam;
            return catcache
                .am_rows()
                .into_iter()
                .find(|row| row.oid == relam)
                .map(|row| row.amname);
        }

        match self.relcache.get_by_oid(relation_oid)?.relkind {
            'r' => Some("heap".to_string()),
            'i' => Some("btree".to_string()),
            _ => None,
        }
    }

    pub fn authid_rows(&self) -> Vec<PgAuthIdRow> {
        self.catcache
            .as_ref()
            .map(|catcache| catcache.authid_rows())
            .unwrap_or_default()
    }

    pub fn proc_rows(&self) -> Vec<PgProcRow> {
        self.catcache
            .as_ref()
            .map(|catcache| catcache.proc_rows())
            .unwrap_or_else(crate::include::catalog::bootstrap_pg_proc_rows)
    }

    pub fn amproc_rows(&self) -> Vec<PgAmprocRow> {
        self.catcache
            .as_ref()
            .map(CatCache::amproc_rows)
            .unwrap_or_else(bootstrap_pg_amproc_rows)
    }

    pub fn database_row_by_oid(&self, oid: u32) -> Option<PgDatabaseRow> {
        self.catcache
            .as_ref()
            .and_then(|catcache| {
                catcache
                    .database_rows()
                    .into_iter()
                    .find(|row| row.oid == oid)
            })
            .or_else(|| {
                bootstrap_pg_database_rows()
                    .into_iter()
                    .find(|row| row.oid == oid)
            })
    }

    pub fn foreign_data_wrapper_row_by_oid(&self, oid: u32) -> Option<PgForeignDataWrapperRow> {
        self.catcache.as_ref().and_then(|catcache| {
            catcache
                .foreign_data_wrapper_rows()
                .into_iter()
                .find(|row| row.oid == oid)
        })
    }

    pub fn auth_members_rows(&self) -> Vec<PgAuthMembersRow> {
        self.catcache
            .as_ref()
            .map(|catcache| catcache.auth_members_rows())
            .unwrap_or_default()
    }

    pub fn statistic_ext_rows(&self) -> Vec<PgStatisticExtRow> {
        self.catcache
            .as_ref()
            .map(|catcache| catcache.statistic_ext_rows())
            .unwrap_or_default()
    }

    pub fn statistic_ext_data_rows(&self) -> Vec<PgStatisticExtDataRow> {
        self.catcache
            .as_ref()
            .map(|catcache| catcache.statistic_ext_data_rows())
            .unwrap_or_default()
    }

    pub fn role_name_by_oid(&self, role_oid: u32) -> Option<String> {
        self.catcache.as_ref().and_then(|catcache| {
            catcache
                .authid_rows()
                .into_iter()
                .find(|row| row.oid == role_oid)
                .map(|row| row.rolname)
        })
    }

    pub fn database_oid_by_name(&self, name: &str) -> Option<u32> {
        self.catcache.as_ref().and_then(|catcache| {
            catcache
                .database_rows()
                .into_iter()
                .find(|row| row.datname.eq_ignore_ascii_case(name))
                .map(|row| row.oid)
        })
    }
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

impl CatalogLookup for VisibleCatalog {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation> {
        self.relcache
            .get_by_name(name)
            .map(|entry| bound_relation_from_relcache_entry(&self.relcache, entry))
    }

    fn lookup_relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.relcache
            .get_by_oid(relation_oid)
            .map(|entry| bound_relation_from_relcache_entry(&self.relcache, entry))
    }

    fn constraint_rows_for_relation(&self, relation_oid: u32) -> Vec<PgConstraintRow> {
        VisibleCatalog::constraint_rows_for_relation(self, relation_oid)
    }

    fn constraint_rows(&self) -> Vec<PgConstraintRow> {
        VisibleCatalog::constraint_rows(self)
    }

    fn relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        self.relcache
            .get_by_oid(relation_oid)
            .map(|entry| bound_relation_from_relcache_entry(&self.relcache, entry))
    }

    fn index_relations_for_heap(
        &self,
        relation_oid: u32,
    ) -> Vec<crate::backend::parser::BoundIndexRelation> {
        self.relcache
            .relation_get_index_list(relation_oid)
            .into_iter()
            .filter_map(|index_oid| {
                let entry = self.relcache.get_by_oid(index_oid)?;
                let name = self
                    .relcache
                    .relation_name_by_oid(index_oid)
                    .unwrap_or_else(|| index_oid.to_string());
                bound_index_relation_from_relcache_entry(name, entry, self)
            })
            .collect()
    }

    fn index_row_by_oid(&self, index_oid: u32) -> Option<crate::include::catalog::PgIndexRow> {
        self.relcache.index_row_by_oid(index_oid)
    }

    fn current_user_oid(&self) -> u32 {
        BOOTSTRAP_SUPERUSER_OID
    }

    fn search_path(&self) -> Vec<String> {
        self.search_path.clone()
    }

    fn session_user_oid(&self) -> u32 {
        BOOTSTRAP_SUPERUSER_OID
    }

    fn authid_rows(&self) -> Vec<PgAuthIdRow> {
        VisibleCatalog::authid_rows(self)
    }

    fn auth_members_rows(&self) -> Vec<PgAuthMembersRow> {
        VisibleCatalog::auth_members_rows(self)
    }

    fn namespace_row_by_oid(&self, oid: u32) -> Option<PgNamespaceRow> {
        self.catcache
            .as_ref()
            .and_then(|catcache| catcache.namespace_by_oid(oid).cloned())
            .or_else(|| {
                bootstrap_pg_namespace_rows()
                    .into_iter()
                    .find(|row| row.oid == oid)
            })
    }

    fn namespace_rows(&self) -> Vec<PgNamespaceRow> {
        self.catcache
            .as_ref()
            .map(|catcache| catcache.namespace_rows())
            .unwrap_or_else(|| bootstrap_pg_namespace_rows().to_vec())
    }

    fn proc_rows_by_name(&self, name: &str) -> Vec<PgProcRow> {
        if let Some(catcache) = self.catcache.as_ref() {
            let mut rows = catcache
                .proc_rows_by_name(name)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            rows.extend(synthetic_range_proc_rows_by_name(
                name,
                &self.type_rows(),
                &self.range_rows(),
            ));
            dedup_proc_rows(&mut rows);
            return rows;
        }

        let normalized = normalize_name(name);
        let mut rows: Vec<_> = bootstrap_pg_proc_rows()
            .into_iter()
            .filter(|row| row.proname.eq_ignore_ascii_case(normalized))
            .collect();
        rows.extend(synthetic_range_proc_rows_by_name(
            name,
            &self.type_rows(),
            &self.range_rows(),
        ));
        dedup_proc_rows(&mut rows);
        rows
    }

    fn proc_row_by_oid(&self, oid: u32) -> Option<PgProcRow> {
        self.catcache
            .as_ref()
            .and_then(|catcache| catcache.proc_by_oid(oid).cloned())
            .or_else(|| {
                bootstrap_pg_proc_rows()
                    .into_iter()
                    .find(|row| row.oid == oid)
            })
            .or_else(|| {
                crate::include::catalog::synthetic_range_proc_row_by_oid(
                    oid,
                    &self.type_rows(),
                    &self.range_rows(),
                )
            })
    }

    fn opclass_rows(&self) -> Vec<PgOpclassRow> {
        self.catcache
            .as_ref()
            .map(CatCache::opclass_rows)
            .unwrap_or_else(bootstrap_pg_opclass_rows)
    }

    fn collation_rows(&self) -> Vec<PgCollationRow> {
        self.catcache
            .as_ref()
            .map(CatCache::collation_rows)
            .unwrap_or_else(|| bootstrap_pg_collation_rows().to_vec())
    }

    fn aggregate_by_fnoid(&self, aggfnoid: u32) -> Option<PgAggregateRow> {
        self.catcache
            .as_ref()
            .and_then(|catcache| catcache.aggregate_by_fnoid(aggfnoid).cloned())
            .or_else(|| {
                bootstrap_pg_aggregate_rows()
                    .into_iter()
                    .find(|row| row.aggfnoid == aggfnoid)
            })
    }

    fn operator_by_name_left_right(
        &self,
        name: &str,
        left_type_oid: u32,
        right_type_oid: u32,
    ) -> Option<PgOperatorRow> {
        if let Some(catcache) = &self.catcache {
            return catcache
                .operator_by_name_left_right(name, left_type_oid, right_type_oid)
                .cloned();
        }
        let normalized = normalize_name(name);
        bootstrap_pg_operator_rows().into_iter().find(|row| {
            row.oprname.eq_ignore_ascii_case(normalized)
                && row.oprleft == left_type_oid
                && row.oprright == right_type_oid
        })
    }

    fn operator_by_oid(&self, oid: u32) -> Option<PgOperatorRow> {
        self.catcache
            .as_ref()
            .and_then(|catcache| {
                catcache
                    .operator_rows()
                    .into_iter()
                    .find(|row| row.oid == oid)
            })
            .or_else(|| {
                bootstrap_pg_operator_rows()
                    .into_iter()
                    .find(|row| row.oid == oid)
            })
    }

    fn operator_rows(&self) -> Vec<PgOperatorRow> {
        self.catcache
            .as_ref()
            .map(CatCache::operator_rows)
            .unwrap_or_else(bootstrap_pg_operator_rows)
    }

    fn ts_config_rows(&self) -> Vec<PgTsConfigRow> {
        self.catcache
            .as_ref()
            .map(CatCache::ts_config_rows)
            .unwrap_or_else(|| bootstrap_pg_ts_config_rows().to_vec())
    }

    fn ts_dict_rows(&self) -> Vec<PgTsDictRow> {
        self.catcache
            .as_ref()
            .map(CatCache::ts_dict_rows)
            .unwrap_or_else(|| bootstrap_pg_ts_dict_rows().to_vec())
    }

    fn cast_by_source_target(
        &self,
        source_type_oid: u32,
        target_type_oid: u32,
    ) -> Option<PgCastRow> {
        if let Some(catcache) = &self.catcache {
            return catcache
                .cast_by_source_target(source_type_oid, target_type_oid)
                .cloned();
        }
        bootstrap_pg_cast_rows()
            .into_iter()
            .find(|row| row.castsource == source_type_oid && row.casttarget == target_type_oid)
    }

    fn cast_rows(&self) -> Vec<PgCastRow> {
        self.catcache
            .as_ref()
            .map(CatCache::cast_rows)
            .unwrap_or_else(bootstrap_pg_cast_rows)
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        let mut rows = self
            .catcache
            .as_ref()
            .map(CatCache::type_rows)
            .unwrap_or_else(builtin_type_rows);
        for composite in composite_type_rows_from_relcache(&self.relcache) {
            if rows.iter().all(|existing| existing.oid != composite.oid) {
                rows.push(composite);
            }
        }
        for dynamic in &self.dynamic_type_rows {
            if rows.iter().all(|existing| existing.oid != dynamic.oid) {
                rows.push(dynamic.clone());
            }
        }
        rows
    }

    fn domain_check_by_type_oid(&self, oid: u32) -> Option<String> {
        self.domain_checks.get(&oid).map(|(name, _)| name.clone())
    }

    fn range_rows(&self) -> Vec<PgRangeRow> {
        builtin_range_rows()
    }

    fn enum_label_oid(&self, type_oid: u32, label: &str) -> Option<u32> {
        self.enum_rows
            .iter()
            .find(|row| row.enumtypid == type_oid && row.enumlabel == label)
            .map(|row| row.oid)
    }

    fn enum_label(&self, type_oid: u32, label_oid: u32) -> Option<String> {
        self.enum_rows
            .iter()
            .find(|row| row.enumtypid == type_oid && row.oid == label_oid)
            .map(|row| row.enumlabel.clone())
    }

    fn enum_label_by_oid(&self, label_oid: u32) -> Option<String> {
        self.enum_rows
            .iter()
            .find(|row| row.oid == label_oid)
            .map(|row| row.enumlabel.clone())
    }

    fn enum_rows(&self) -> Vec<PgEnumRow> {
        if self.enum_rows.is_empty() {
            return bootstrap_pg_enum_rows().to_vec();
        }
        self.enum_rows.clone()
    }

    fn enum_label_is_committed(&self, _type_oid: u32, label_oid: u32) -> bool {
        !self.uncommitted_enum_label_oids.contains(&label_oid)
    }

    fn domain_allowed_enum_label_oids(&self, domain_oid: u32) -> Option<Vec<u32>> {
        self.domain_checks
            .get(&domain_oid)
            .map(|(_, allowed)| allowed.clone())
    }

    fn domain_check_name(&self, domain_oid: u32) -> Option<String> {
        self.domain_checks
            .get(&domain_oid)
            .map(|(name, _)| name.clone())
    }

    fn language_rows(&self) -> Vec<PgLanguageRow> {
        self.catcache
            .as_ref()
            .map(CatCache::language_rows)
            .unwrap_or_else(|| bootstrap_pg_language_rows().to_vec())
    }

    fn language_row_by_oid(&self, oid: u32) -> Option<PgLanguageRow> {
        self.language_rows().into_iter().find(|row| row.oid == oid)
    }

    fn language_row_by_name(&self, name: &str) -> Option<PgLanguageRow> {
        let normalized = normalize_name(name);
        self.language_rows()
            .into_iter()
            .find(|row| row.lanname.eq_ignore_ascii_case(normalized))
    }

    fn rewrite_rows_for_relation(&self, relation_oid: u32) -> Vec<PgRewriteRow> {
        self.catcache
            .as_ref()
            .map(|catcache| catcache.rewrite_rows_for_relation(relation_oid))
            .unwrap_or_default()
    }

    fn trigger_rows_for_relation(&self, relation_oid: u32) -> Vec<PgTriggerRow> {
        VisibleCatalog::trigger_rows_for_relation(self, relation_oid)
    }

    fn policy_rows_for_relation(&self, relation_oid: u32) -> Vec<PgPolicyRow> {
        VisibleCatalog::policy_rows_for_relation(self, relation_oid)
    }

    fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow> {
        self.catcache
            .as_ref()
            .and_then(|catcache| catcache.class_by_oid(relation_oid).cloned())
    }

    fn partitioned_table_row(&self, relation_oid: u32) -> Option<PgPartitionedTableRow> {
        self.catcache
            .as_ref()
            .and_then(|catcache| catcache.partitioned_table_row(relation_oid).cloned())
    }

    fn partitioned_table_rows(&self) -> Vec<PgPartitionedTableRow> {
        self.catcache
            .as_ref()
            .map(CatCache::partitioned_table_rows)
            .unwrap_or_default()
    }

    fn inheritance_parents(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
        self.catcache
            .as_ref()
            .map(|catcache| {
                catcache
                    .inherit_rows()
                    .into_iter()
                    .filter(|row| row.inhrelid == relation_oid)
                    .collect()
            })
            .unwrap_or_default()
    }

    fn inheritance_children(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
        self.catcache
            .as_ref()
            .map(|catcache| {
                catcache
                    .inherit_rows()
                    .into_iter()
                    .filter(|row| row.inhparent == relation_oid)
                    .collect()
            })
            .unwrap_or_default()
    }

    fn statistic_rows_for_relation(&self, relation_oid: u32) -> Vec<PgStatisticRow> {
        self.catcache
            .as_ref()
            .map(|catcache| {
                catcache
                    .statistic_rows()
                    .into_iter()
                    .filter(|row| row.starelid == relation_oid)
                    .collect()
            })
            .unwrap_or_default()
    }

    fn statistic_ext_rows(&self) -> Vec<PgStatisticExtRow> {
        VisibleCatalog::statistic_ext_rows(self)
    }

    fn statistic_ext_data_rows(&self) -> Vec<PgStatisticExtDataRow> {
        VisibleCatalog::statistic_ext_data_rows(self)
    }

    fn pg_views_rows(&self) -> Vec<Vec<crate::backend::executor::Value>> {
        let Some(catcache) = &self.catcache else {
            return Vec::new();
        };
        build_pg_views_rows(
            catcache.namespace_rows(),
            catcache.authid_rows(),
            catcache.class_rows(),
            catcache.rewrite_rows(),
        )
    }

    fn pg_matviews_rows(&self) -> Vec<Vec<crate::backend::executor::Value>> {
        let Some(catcache) = &self.catcache else {
            return Vec::new();
        };
        build_pg_matviews_rows(
            catcache.namespace_rows(),
            catcache.authid_rows(),
            catcache.class_rows(),
            catcache.index_rows(),
            catcache.rewrite_rows(),
        )
    }

    fn pg_indexes_rows(&self) -> Vec<Vec<crate::backend::executor::Value>> {
        let Some(catcache) = &self.catcache else {
            return Vec::new();
        };
        build_pg_indexes_rows(
            catcache.namespace_rows(),
            catcache.class_rows(),
            catcache.attribute_rows(),
            catcache.index_rows(),
            catcache.am_rows(),
        )
    }

    fn pg_policies_rows(&self) -> Vec<Vec<crate::backend::executor::Value>> {
        let Some(catcache) = &self.catcache else {
            return Vec::new();
        };
        build_pg_policies_rows(
            catcache.namespace_rows(),
            catcache.authid_rows(),
            catcache.class_rows(),
            catcache.policy_rows(),
        )
    }

    fn pg_rules_rows(&self) -> Vec<Vec<crate::backend::executor::Value>> {
        let Some(catcache) = &self.catcache else {
            return Vec::new();
        };
        build_pg_rules_rows(
            catcache.namespace_rows(),
            catcache.class_rows(),
            catcache.rewrite_rows(),
        )
    }

    fn pg_stats_rows(&self) -> Vec<Vec<crate::backend::executor::Value>> {
        let Some(catcache) = &self.catcache else {
            return Vec::new();
        };
        build_pg_stats_rows(
            catcache.namespace_rows(),
            catcache.class_rows(),
            catcache.attribute_rows(),
            catcache.statistic_rows(),
        )
    }

    fn pg_stat_activity_rows(&self) -> Vec<Vec<crate::backend::executor::Value>> {
        Vec::new()
    }

    fn pg_stat_user_tables_rows(&self) -> Vec<Vec<crate::backend::executor::Value>> {
        let Some(catcache) = &self.catcache else {
            return Vec::new();
        };
        let stats = DatabaseStatsStore::with_default_io_rows();
        build_pg_stat_user_tables_rows(
            catcache.namespace_rows(),
            catcache.class_rows(),
            catcache.index_rows(),
            &stats,
        )
    }

    fn pg_statio_user_tables_rows(&self) -> Vec<Vec<crate::backend::executor::Value>> {
        let Some(catcache) = &self.catcache else {
            return Vec::new();
        };
        let stats = DatabaseStatsStore::with_default_io_rows();
        build_pg_statio_user_tables_rows(
            catcache.namespace_rows(),
            catcache.class_rows(),
            catcache.index_rows(),
            &stats,
        )
    }

    fn pg_stat_user_functions_rows(&self) -> Vec<Vec<crate::backend::executor::Value>> {
        let Some(catcache) = &self.catcache else {
            return Vec::new();
        };
        let stats = DatabaseStatsStore::with_default_io_rows();
        build_pg_stat_user_functions_rows(catcache.namespace_rows(), catcache.proc_rows(), &stats)
    }

    fn pg_stat_io_rows(&self) -> Vec<Vec<crate::backend::executor::Value>> {
        build_pg_stat_io_rows(&DatabaseStatsStore::with_default_io_rows())
    }

    fn pg_locks_rows(&self) -> Vec<Vec<crate::backend::executor::Value>> {
        build_pg_locks_rows(Vec::new())
    }

    fn materialize_visible_catalog(&self) -> Option<VisibleCatalog> {
        Some(self.clone())
    }
}

fn normalize_name(name: &str) -> &str {
    name.strip_prefix("pg_catalog.").unwrap_or(name)
}

fn bound_relation_from_relcache_entry(
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
        relpersistence: entry.relpersistence,
        relkind: entry.relkind,
        relispopulated: entry.relispopulated,
        relispartition: entry.relispartition,
        relpartbound: entry.relpartbound.clone(),
        desc: entry.desc.clone(),
        partitioned_table: entry.partitioned_table.clone(),
    }
}

fn composite_type_rows_from_relcache(relcache: &RelCache) -> Vec<PgTypeRow> {
    relcache
        .entries()
        .flat_map(|(name, entry)| {
            let relname = name.rsplit('.').next().unwrap_or(name);
            let mut rows = Vec::new();
            if entry.row_type_oid != 0 {
                rows.push(crate::include::catalog::composite_type_row(
                    relname,
                    entry.row_type_oid,
                    entry.namespace_oid,
                    entry.relation_oid,
                    entry.array_type_oid,
                ));
            }
            if entry.array_type_oid != 0 {
                rows.push(crate::include::catalog::composite_array_type_row(
                    relname,
                    entry.array_type_oid,
                    entry.namespace_oid,
                    entry.row_type_oid,
                    entry.relation_oid,
                ));
            }
            rows
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::Catalog;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::parser::{CatalogLookup, SqlType, SqlTypeKind};
    use crate::include::catalog::TEXT_TYPE_OID;
    use crate::include::nodes::primnodes::RelationDesc;

    #[test]
    fn visible_catalog_prefers_supplied_catcache_metadata() {
        let base = CatCache::from_catalog(&Catalog::default());
        let filtered = CatCache::from_rows(
            base.namespace_rows(),
            base.class_rows(),
            base.attribute_rows(),
            base.attrdef_rows(),
            base.depend_rows(),
            base.inherit_rows(),
            base.index_rows(),
            base.rewrite_rows(),
            base.trigger_rows(),
            base.policy_rows(),
            base.publication_rows(),
            base.publication_rel_rows(),
            base.publication_namespace_rows(),
            base.statistic_ext_rows(),
            base.statistic_ext_data_rows(),
            base.am_rows(),
            base.amop_rows(),
            base.amproc_rows(),
            base.authid_rows(),
            base.auth_members_rows(),
            base.language_rows(),
            base.ts_parser_rows(),
            base.ts_template_rows(),
            base.ts_dict_rows(),
            base.ts_config_rows(),
            base.ts_config_map_rows(),
            base.constraint_rows(),
            base.operator_rows(),
            base.opclass_rows(),
            base.opfamily_rows(),
            base.partitioned_table_rows(),
            base.proc_rows()
                .into_iter()
                .filter(|row| row.proname != "lower")
                .collect(),
            base.aggregate_rows(),
            base.cast_rows(),
            base.collation_rows(),
            base.foreign_data_wrapper_rows(),
            base.database_rows(),
            base.tablespace_rows(),
            base.statistic_rows(),
            base.type_rows(),
        );
        let visible = VisibleCatalog::new(RelCache::default(), Some(filtered));

        let lower_rows = visible.proc_rows_by_name("lower");
        assert!(!lower_rows.is_empty());
        assert!(lower_rows.iter().all(|row| row.prosrc == "range_lower"));
    }

    #[test]
    fn visible_catalog_type_oid_prefers_builtin_scalar_types_over_composites() {
        let visible = VisibleCatalog::new(
            RelCache::default(),
            Some(CatCache::from_catalog(&Catalog::default())),
        );

        assert_eq!(
            visible.type_oid_for_sql_type(SqlType::new(SqlTypeKind::Text)),
            Some(TEXT_TYPE_OID)
        );
    }

    #[test]
    fn visible_catalog_type_oid_preserves_named_composite_identity() {
        let mut catalog = Catalog::default();
        let entry = catalog
            .create_table(
                "widgets",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("label", SqlType::new(SqlTypeKind::Text), false),
                    ],
                },
            )
            .unwrap();
        let visible = VisibleCatalog::new(
            RelCache::from_catalog(&catalog),
            Some(CatCache::from_catalog(&catalog)),
        );

        assert_eq!(
            visible.type_oid_for_sql_type(SqlType::named_composite(
                entry.row_type_oid,
                entry.relation_oid,
            )),
            Some(entry.row_type_oid)
        );
    }
}
