use crate::backend::catalog::pg_constraint::derived_pg_constraint_rows;
use crate::backend::parser::{BoundRelation, CatalogLookup};
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::relcache::RelCache;
use crate::include::catalog::{
    PgCastRow, PgConstraintRow, PgOperatorRow, PgProcRow, PgTypeRow, bootstrap_pg_cast_rows,
    bootstrap_pg_operator_rows, bootstrap_pg_proc_rows, builtin_type_rows,
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
    fn lookup_relation(&self, name: &str) -> Option<BoundRelation> {
        self.relcache.get_by_name(name).and_then(|entry| {
            (entry.relkind == 'r').then(|| BoundRelation {
                rel: entry.rel,
                relation_oid: entry.relation_oid,
                desc: entry.desc.clone(),
            })
        })
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

    fn cast_by_source_target(&self, source_type_oid: u32, target_type_oid: u32) -> Option<PgCastRow> {
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
        self.catcache
            .as_ref()
            .map(CatCache::type_rows)
            .unwrap_or_else(builtin_type_rows)
    }
}

fn normalize_name(name: &str) -> &str {
    name.strip_prefix("pg_catalog.").unwrap_or(name)
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
            base.am_rows(),
            base.authid_rows(),
            base.auth_members_rows(),
            base.language_rows(),
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
