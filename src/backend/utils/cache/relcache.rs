use std::collections::BTreeMap;

use crate::backend::catalog::catalog::{Catalog, CatalogEntry};
use crate::backend::utils::cache::catcache::normalize_catalog_name;

#[derive(Debug, Clone, Default)]
pub struct RelCache {
    by_name: BTreeMap<String, CatalogEntry>,
    by_oid: BTreeMap<u32, CatalogEntry>,
}

impl RelCache {
    pub fn from_catalog(catalog: &Catalog) -> Self {
        let mut cache = Self::default();
        for (name, entry) in catalog.entries() {
            cache.by_name
                .insert(normalize_catalog_name(name).to_ascii_lowercase(), entry.clone());
            cache.by_oid.insert(entry.relation_oid, entry.clone());
        }
        cache
    }

    pub fn get_by_name(&self, name: &str) -> Option<&CatalogEntry> {
        self.by_name
            .get(&normalize_catalog_name(name).to_ascii_lowercase())
    }

    pub fn get_by_oid(&self, oid: u32) -> Option<&CatalogEntry> {
        self.by_oid.get(&oid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::executor::RelationDesc;
    use crate::backend::parser::{SqlType, SqlTypeKind};

    #[test]
    fn relcache_indexes_relations_by_name_and_oid() {
        let mut catalog = Catalog::default();
        let entry = catalog
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let cache = RelCache::from_catalog(&catalog);
        assert_eq!(
            cache.get_by_name("people").map(|entry| entry.rel.rel_number),
            Some(entry.rel.rel_number)
        );
        assert_eq!(
            cache.get_by_oid(entry.relation_oid).map(|entry| entry.rel.rel_number),
            Some(entry.rel.rel_number)
        );
    }
}
