use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use crate::{
    BoundIndexRelation, BoundRelation, CatalogLookup,
    bound_index_relation_from_relcache_entry_with_heap_and_cache,
};
use pgrust_catalog_data::{
    PgAttributeRow, PgAuthIdRow, PgAuthMembersRow, PgClassRow, PgConstraintRow, PgDatabaseRow,
    PgEventTriggerRow, PgForeignDataWrapperRow, PgForeignServerRow, PgForeignTableRow, PgIndexRow,
    PgInheritsRow, PgLanguageRow, PgNamespaceRow, PgOperatorRow, PgPartitionedTableRow,
    PgTablespaceRow, PgTypeRow, PgUserMappingRow, bootstrap_pg_language_rows,
    bootstrap_pg_namespace_rows, bootstrap_pg_operator_rows, builtin_type_rows,
    composite_array_type_row_with_owner, composite_type_row_with_owner,
};
use pgrust_catalog_store::catcache::{CatCache, sql_type_oid};
use pgrust_catalog_store::{Catalog, RelCache};
use pgrust_nodes::SqlType;
use pgrust_nodes::pathnodes::PlannerIndexExprCacheEntry;
use pgrust_nodes::primnodes::ToastRelationRef;
use pgrust_nodes::relcache::RelCacheEntry;

fn catalog_catcache(catalog: &Catalog) -> CatCache {
    CatCache::from_catalog(catalog)
}

fn catalog_relcache(catalog: &Catalog) -> RelCache {
    RelCache::from_catalog(catalog)
}

impl CatalogLookup for Catalog {
    fn lookup_any_relation(&self, name: &str) -> Option<BoundRelation> {
        CatalogLookup::lookup_any_relation(&catalog_relcache(self), name)
    }

    fn lookup_relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        CatalogLookup::lookup_relation_by_oid(&catalog_relcache(self), relation_oid)
    }

    fn relation_by_oid(&self, relation_oid: u32) -> Option<BoundRelation> {
        CatalogLookup::relation_by_oid(&catalog_relcache(self), relation_oid)
    }

    fn index_relations_for_heap(&self, relation_oid: u32) -> Vec<BoundIndexRelation> {
        CatalogLookup::index_relations_for_heap(&catalog_relcache(self), relation_oid)
    }

    fn index_relations_for_heap_with_cache(
        &self,
        relation_oid: u32,
        index_expr_cache: &RefCell<BTreeMap<u32, PlannerIndexExprCacheEntry>>,
    ) -> Vec<BoundIndexRelation> {
        CatalogLookup::index_relations_for_heap_with_cache(
            &catalog_relcache(self),
            relation_oid,
            index_expr_cache,
        )
    }

    fn index_row_by_oid(&self, index_oid: u32) -> Option<PgIndexRow> {
        CatalogLookup::index_row_by_oid(&catalog_relcache(self), index_oid)
    }

    fn type_rows(&self) -> Vec<PgTypeRow> {
        catalog_catcache(self).type_rows()
    }

    fn type_by_oid(&self, oid: u32) -> Option<PgTypeRow> {
        catalog_catcache(self).type_by_oid(oid).cloned()
    }

    fn type_by_name(&self, name: &str) -> Option<PgTypeRow> {
        catalog_catcache(self).type_by_name(name).cloned()
    }

    fn type_oid_for_sql_type(&self, sql_type: SqlType) -> Option<u32> {
        Some(sql_type_oid(sql_type))
    }

    fn namespace_row_by_oid(&self, oid: u32) -> Option<PgNamespaceRow> {
        catalog_catcache(self).namespace_by_oid(oid).cloned()
    }

    fn namespace_rows(&self) -> Vec<PgNamespaceRow> {
        catalog_catcache(self).namespace_rows()
    }

    fn language_rows(&self) -> Vec<PgLanguageRow> {
        catalog_catcache(self).language_rows()
    }

    fn language_row_by_oid(&self, oid: u32) -> Option<PgLanguageRow> {
        self.language_rows().into_iter().find(|row| row.oid == oid)
    }

    fn language_row_by_name(&self, name: &str) -> Option<PgLanguageRow> {
        self.language_rows()
            .into_iter()
            .find(|row| row.lanname.eq_ignore_ascii_case(name))
    }

    fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow> {
        catalog_catcache(self).class_by_oid(relation_oid).cloned()
    }

    fn class_rows(&self) -> Vec<PgClassRow> {
        catalog_catcache(self).class_rows()
    }

    fn attribute_rows(&self) -> Vec<PgAttributeRow> {
        catalog_catcache(self).attribute_rows()
    }

    fn attribute_rows_for_relation(&self, relation_oid: u32) -> Vec<PgAttributeRow> {
        catalog_catcache(self)
            .attributes_by_relid(relation_oid)
            .unwrap_or_default()
            .to_vec()
    }

    fn constraint_rows_for_relation(&self, relation_oid: u32) -> Vec<PgConstraintRow> {
        catalog_catcache(self).constraint_rows_for_relation(relation_oid)
    }

    fn constraint_rows(&self) -> Vec<PgConstraintRow> {
        catalog_catcache(self).constraint_rows()
    }

    fn authid_rows(&self) -> Vec<PgAuthIdRow> {
        self.authid_rows().to_vec()
    }

    fn auth_members_rows(&self) -> Vec<PgAuthMembersRow> {
        self.auth_members_rows().to_vec()
    }

    fn database_rows(&self) -> Vec<PgDatabaseRow> {
        self.database_rows().to_vec()
    }

    fn tablespace_rows(&self) -> Vec<PgTablespaceRow> {
        self.tablespace_rows().to_vec()
    }

    fn rewrite_rows_for_relation(
        &self,
        relation_oid: u32,
    ) -> Vec<pgrust_catalog_data::PgRewriteRow> {
        catalog_catcache(self).rewrite_rows_for_relation(relation_oid)
    }

    fn rewrite_rows(&self) -> Vec<pgrust_catalog_data::PgRewriteRow> {
        catalog_catcache(self).rewrite_rows()
    }

    fn rewrite_row_by_oid(&self, rewrite_oid: u32) -> Option<pgrust_catalog_data::PgRewriteRow> {
        catalog_catcache(self)
            .rewrite_rows()
            .into_iter()
            .find(|row| row.oid == rewrite_oid)
    }

    fn event_trigger_rows(&self) -> Vec<PgEventTriggerRow> {
        catalog_catcache(self).event_trigger_rows()
    }

    fn partitioned_table_row(&self, relation_oid: u32) -> Option<PgPartitionedTableRow> {
        catalog_catcache(self)
            .partitioned_table_row(relation_oid)
            .cloned()
    }

    fn partitioned_table_rows(&self) -> Vec<PgPartitionedTableRow> {
        catalog_catcache(self).partitioned_table_rows()
    }

    fn inheritance_parents(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
        catalog_catcache(self)
            .inherit_rows()
            .into_iter()
            .filter(|row| row.inhrelid == relation_oid)
            .collect()
    }

    fn inheritance_children(&self, relation_oid: u32) -> Vec<PgInheritsRow> {
        catalog_catcache(self)
            .inherit_rows()
            .into_iter()
            .filter(|row| row.inhparent == relation_oid)
            .collect()
    }

    fn inheritance_rows(&self) -> Vec<PgInheritsRow> {
        catalog_catcache(self).inherit_rows()
    }

    fn operator_by_oid(&self, oid: u32) -> Option<PgOperatorRow> {
        catalog_catcache(self)
            .operator_rows()
            .into_iter()
            .find(|row| row.oid == oid)
    }

    fn foreign_data_wrapper_rows(&self) -> Vec<PgForeignDataWrapperRow> {
        catalog_catcache(self).foreign_data_wrapper_rows()
    }

    fn foreign_server_rows(&self) -> Vec<PgForeignServerRow> {
        catalog_catcache(self).foreign_server_rows()
    }

    fn foreign_table_rows(&self) -> Vec<PgForeignTableRow> {
        catalog_catcache(self).foreign_table_rows()
    }

    fn user_mapping_rows(&self) -> Vec<PgUserMappingRow> {
        catalog_catcache(self).user_mapping_rows()
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

fn relcache_bound_relation(relcache: &RelCache, entry: &RelCacheEntry) -> BoundRelation {
    BoundRelation {
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        toast: (entry.reltoastrelid != 0)
            .then(|| relcache.get_by_oid(entry.reltoastrelid))
            .flatten()
            .map(|toast| ToastRelationRef {
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
            rows.push(composite_type_row_with_owner(
                relname,
                entry.row_type_oid,
                entry.namespace_oid,
                entry.owner_oid,
                entry.relation_oid,
                entry.array_type_oid,
            ));
        }
        if entry.array_type_oid != 0 && seen.insert(entry.array_type_oid) {
            rows.push(composite_array_type_row_with_owner(
                relname,
                entry.array_type_oid,
                entry.namespace_oid,
                entry.owner_oid,
                entry.row_type_oid,
                entry.relation_oid,
            ));
        }
    }
    rows
}
