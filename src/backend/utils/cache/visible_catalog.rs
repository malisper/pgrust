use crate::backend::catalog::pg_constraint::derived_pg_constraint_rows;
use crate::backend::parser::{BoundRelation, CatalogLookup, SqlType};
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::relcache::RelCache;
use crate::backend::utils::cache::system_views::{build_pg_stats_rows, build_pg_views_rows};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, PgCastRow, PgClassRow, PgConstraintRow, PgOperatorRow, PgProcRow,
    PgRewriteRow, PgStatisticRow, PgTypeRow, bootstrap_pg_cast_rows, bootstrap_pg_operator_rows,
    bootstrap_pg_proc_rows, builtin_type_rows,
};

#[derive(Debug, Clone)]
pub struct VisibleCatalog {
    relcache: RelCache,
    catcache: Option<CatCache>,
}

impl VisibleCatalog {
    pub fn new(relcache: RelCache, catcache: Option<CatCache>) -> Self {
        Self { relcache, catcache }
    }

    pub fn relcache(&self) -> &RelCache {
        &self.relcache
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

    fn proc_rows_by_name(&self, name: &str) -> Vec<PgProcRow> {
        self.catcache
            .as_ref()
            .map(|catcache| {
                catcache
                    .proc_rows_by_name(name)
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| {
                let normalized = normalize_name(name);
                bootstrap_pg_proc_rows()
                    .into_iter()
                    .filter(|row| row.proname.eq_ignore_ascii_case(normalized))
                    .collect()
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
        rows
    }

    fn rewrite_rows_for_relation(&self, relation_oid: u32) -> Vec<PgRewriteRow> {
        self.catcache
            .as_ref()
            .map(|catcache| catcache.rewrite_rows_for_relation(relation_oid))
            .unwrap_or_default()
    }

    fn class_row_by_oid(&self, relation_oid: u32) -> Option<PgClassRow> {
        self.catcache
            .as_ref()
            .and_then(|catcache| catcache.class_by_oid(relation_oid).cloned())
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

    fn pg_views_rows(&self) -> Vec<Vec<crate::backend::executor::Value>> {
        let Some(catcache) = &self.catcache else {
            return Vec::new();
        };
        build_pg_views_rows(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::Catalog;
    use crate::backend::parser::{CatalogLookup, SqlType, SqlTypeKind};
    use crate::include::catalog::TEXT_TYPE_OID;

    #[test]
    fn visible_catalog_prefers_supplied_catcache_metadata() {
        let base = CatCache::from_catalog(&Catalog::default());
        let filtered = CatCache::from_rows(
            base.namespace_rows(),
            base.class_rows(),
            base.attribute_rows(),
            base.attrdef_rows(),
            base.depend_rows(),
            base.index_rows(),
            base.rewrite_rows(),
            base.am_rows(),
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
            base.proc_rows()
                .into_iter()
                .filter(|row| row.proname != "lower")
                .collect(),
            base.cast_rows(),
            base.collation_rows(),
            base.database_rows(),
            base.tablespace_rows(),
            base.statistic_rows(),
            base.type_rows(),
        );
        let visible = VisibleCatalog::new(RelCache::default(), Some(filtered));

        assert!(visible.proc_rows_by_name("lower").is_empty());
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
}
