use std::collections::BTreeMap;
use std::path::Path;

use crate::backend::catalog::catalog::{Catalog, CatalogEntry, column_desc};
use crate::backend::catalog::CatalogError;
use crate::backend::executor::RelationDesc;
use crate::backend::storage::smgr::RelFileLocator;
use crate::backend::utils::cache::catcache::{CatCache, normalize_catalog_name};
use crate::backend::parser::SqlType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelCacheEntry {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub namespace_oid: u32,
    pub row_type_oid: u32,
    pub relkind: char,
    pub desc: RelationDesc,
}

#[derive(Debug, Clone, Default)]
pub struct RelCache {
    by_name: BTreeMap<String, RelCacheEntry>,
    by_oid: BTreeMap<u32, RelCacheEntry>,
}

impl RelCache {
    pub fn from_catalog(catalog: &Catalog) -> Self {
        let mut cache = Self::default();
        for (name, entry) in catalog.entries() {
            cache.by_name
                .insert(normalize_catalog_name(name).to_ascii_lowercase(), from_catalog_entry(entry));
            cache.by_oid.insert(entry.relation_oid, from_catalog_entry(entry));
        }
        cache
    }

    pub fn from_physical(base_dir: &Path) -> Result<Self, CatalogError> {
        let catcache = CatCache::from_physical(base_dir)?;
        Self::from_catcache(&catcache)
    }

    pub fn from_catcache(catcache: &CatCache) -> Result<Self, CatalogError> {
        let mut cache = Self::default();
        for class in catcache.class_rows() {
            let attrs = catcache
                .attributes_by_relid(class.oid)
                .ok_or(CatalogError::Corrupt("missing pg_attribute rows for relation"))?;
            let columns = attrs
                .iter()
                .map(|attr| {
                    let sql_type = catcache
                        .type_by_oid(attr.atttypid)
                        .map(|ty| ty.sql_type)
                        .ok_or(CatalogError::Corrupt("unknown atttypid"))?;
                    Ok(column_desc(
                        attr.attname.clone(),
                        SqlType {
                            typmod: attr.atttypmod,
                            ..sql_type
                        },
                        !attr.attnotnull,
                    ))
                })
                .collect::<Result<Vec<_>, CatalogError>>()?;
            let entry = RelCacheEntry {
                rel: RelFileLocator {
                    spc_oid: 0,
                    db_oid: 1,
                    rel_number: class.relfilenode,
                },
                relation_oid: class.oid,
                namespace_oid: class.relnamespace,
                row_type_oid: class.reltype,
                relkind: class.relkind,
                desc: RelationDesc { columns },
            };
            let relname = class.relname.to_ascii_lowercase();
            cache.by_name.insert(relname.clone(), entry.clone());
            if let Some(namespace) = catcache.namespace_by_oid(class.relnamespace) {
                let qualified = format!("{}.{}", namespace.nspname.to_ascii_lowercase(), relname);
                cache.by_name.insert(qualified, entry.clone());
            }
            cache.by_oid.insert(class.oid, entry);
        }
        Ok(cache)
    }

    pub fn get_by_name(&self, name: &str) -> Option<&RelCacheEntry> {
        self.by_name
            .get(&normalize_catalog_name(name).to_ascii_lowercase())
    }

    pub fn get_by_oid(&self, oid: u32) -> Option<&RelCacheEntry> {
        self.by_oid.get(&oid)
    }

    pub fn insert(&mut self, name: impl Into<String>, entry: RelCacheEntry) {
        self.by_name
            .insert(normalize_catalog_name(&name.into()).to_ascii_lowercase(), entry.clone());
        self.by_oid.insert(entry.relation_oid, entry);
    }

    pub fn entries(&self) -> impl Iterator<Item = (&str, &RelCacheEntry)> {
        self.by_name.iter().map(|(name, entry)| (name.as_str(), entry))
    }
}

fn from_catalog_entry(entry: &CatalogEntry) -> RelCacheEntry {
    RelCacheEntry {
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        namespace_oid: entry.namespace_oid,
        row_type_oid: entry.row_type_oid,
        relkind: entry.relkind,
        desc: entry.desc.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::CatalogStore;
    use crate::backend::catalog::catalog::column_desc;
    use crate::backend::executor::RelationDesc;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pgrust_{prefix}_{nanos}"));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        path
    }

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

    #[test]
    fn relcache_loads_relations_from_physical_catalogs() {
        let base = temp_dir("relcache_from_physical");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let cache = RelCache::from_physical(&base).unwrap();
        assert_eq!(
            cache.get_by_name("people").map(|rel| rel.rel.rel_number),
            Some(entry.rel.rel_number)
        );
        assert_eq!(
            cache.get_by_oid(entry.relation_oid).map(|rel| rel.desc.columns.len()),
            Some(1)
        );
    }
}
