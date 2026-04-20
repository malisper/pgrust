use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::backend::catalog::CatalogError;
use crate::backend::catalog::bootstrap::bootstrap_catalog_rel;
use crate::backend::catalog::catalog::{Catalog, CatalogEntry, column_desc};
use crate::backend::executor::RelationDesc;
use crate::backend::parser::SqlType;
use crate::backend::storage::smgr::RelFileLocator;
use crate::backend::utils::cache::catcache::{CatCache, normalize_catalog_name};
use crate::include::catalog::{
    CONSTRAINT_NOTNULL, CONSTRAINT_PRIMARY, PG_CATALOG_NAMESPACE_OID, PG_CONSTRAINT_RELATION_OID,
    bootstrap_catalog_kinds, relam_for_relkind, system_catalog_index_by_oid,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexRelCacheEntry {
    pub indexrelid: u32,
    pub indrelid: u32,
    pub indnatts: i16,
    pub indnkeyatts: i16,
    pub indisunique: bool,
    pub indnullsnotdistinct: bool,
    pub indisprimary: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub indisclustered: bool,
    pub indisvalid: bool,
    pub indcheckxmin: bool,
    pub indisready: bool,
    pub indislive: bool,
    pub indisreplident: bool,
    pub am_oid: u32,
    pub am_handler_oid: Option<u32>,
    pub indkey: Vec<i16>,
    pub indclass: Vec<u32>,
    pub indcollation: Vec<u32>,
    pub indoption: Vec<i16>,
    pub opfamily_oids: Vec<u32>,
    pub opcintype_oids: Vec<u32>,
    pub indexprs: Option<String>,
    pub indpred: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelCacheEntry {
    pub rel: RelFileLocator,
    pub relation_oid: u32,
    pub namespace_oid: u32,
    pub owner_oid: u32,
    pub row_type_oid: u32,
    pub array_type_oid: u32,
    pub reltoastrelid: u32,
    pub relpersistence: char,
    pub relkind: char,
    pub relhastriggers: bool,
    pub relrowsecurity: bool,
    pub relforcerowsecurity: bool,
    pub desc: RelationDesc,
    pub index: Option<IndexRelCacheEntry>,
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
            cache.by_name.insert(
                normalize_catalog_name(name).to_ascii_lowercase(),
                from_catalog_entry(entry),
            );
            cache
                .by_oid
                .insert(entry.relation_oid, from_catalog_entry(entry));
        }
        cache
    }

    pub fn from_physical(base_dir: &Path) -> Result<Self, CatalogError> {
        let catcache = CatCache::from_physical(base_dir)?;
        Self::from_catcache(&catcache)
    }

    pub fn from_catcache(catcache: &CatCache) -> Result<Self, CatalogError> {
        Self::from_catcache_in_db(catcache, 1)
    }

    pub fn from_catcache_in_db(
        catcache: &CatCache,
        current_db_oid: u32,
    ) -> Result<Self, CatalogError> {
        let mut cache = Self::default();
        let not_null_constraints = catcache
            .constraint_rows()
            .into_iter()
            .filter(|row| row.contype == CONSTRAINT_NOTNULL)
            .filter_map(|row| {
                let attnum = *row.conkey.as_ref()?.first()?;
                Some(((row.conrelid, attnum), row))
            })
            .collect::<BTreeMap<_, _>>();
        let primary_constraint_oids = catcache
            .constraint_rows()
            .into_iter()
            .filter(|row| row.contype == CONSTRAINT_PRIMARY)
            .map(|row| row.oid)
            .collect::<BTreeSet<_>>();
        let pk_owned_not_null = catcache
            .depend_rows()
            .into_iter()
            .filter(|row| {
                row.classid == PG_CONSTRAINT_RELATION_OID
                    && row.refclassid == PG_CONSTRAINT_RELATION_OID
                    && primary_constraint_oids.contains(&row.refobjid)
            })
            .map(|row| row.objid)
            .collect::<BTreeSet<_>>();
        for class in catcache.class_rows() {
            let attrs = catcache.attributes_by_relid(class.oid).unwrap_or(&[]);
            let columns = match attrs
                .iter()
                .map(|attr| {
                    let sql_type = catcache
                        .type_by_oid(attr.atttypid)
                        .map(|ty| ty.sql_type)
                        .ok_or(CatalogError::Corrupt("unknown atttypid"))?;
                    let mut desc = column_desc(
                        attr.attname.clone(),
                        SqlType {
                            typmod: attr.atttypmod,
                            ..sql_type
                        },
                        !attr.attnotnull,
                    );
                    desc.storage.attlen = attr.attlen;
                    desc.storage.attalign = attr.attalign;
                    desc.storage.attstorage = attr.attstorage;
                    desc.storage.attcompression = attr.attcompression;
                    desc.attstattarget = attr.attstattarget;
                    desc.attinhcount = attr.attinhcount;
                    desc.attislocal = attr.attislocal;
                    desc.dropped = attr.attisdropped;
                    if let Some(constraint) = not_null_constraints.get(&(class.oid, attr.attnum)) {
                        desc.not_null_constraint_oid = Some(constraint.oid);
                        desc.not_null_constraint_name = Some(constraint.conname.clone());
                        desc.not_null_constraint_validated = constraint.convalidated;
                        desc.not_null_primary_key_owned =
                            pk_owned_not_null.contains(&constraint.oid);
                    }
                    if let Some(attrdef) = catcache.attrdef_by_relid_attnum(class.oid, attr.attnum)
                    {
                        desc.attrdef_oid = Some(attrdef.oid);
                        desc.default_expr = Some(attrdef.adbin.clone());
                        desc.default_sequence_oid =
                            crate::pgrust::database::default_sequence_oid_from_default_expr(
                                &attrdef.adbin,
                            );
                        desc.missing_default_value = if desc.default_sequence_oid.is_some() {
                            None
                        } else {
                            crate::backend::parser::derive_literal_default_value(
                                &attrdef.adbin,
                                desc.sql_type,
                            )
                            .ok()
                        };
                    }
                    Ok(desc)
                })
                .collect::<Result<Vec<_>, CatalogError>>()
            {
                Ok(columns) => columns,
                // :HACK: RelCache currently rebuilds eagerly from every relation in the
                // catalog. Skip non-system relations with dangling type refs so one broken
                // user relation cannot make the entire catalog unreadable. The PG-like end
                // state is to open relcache entries lazily and surface corruption per
                // relation instead of failing the whole cache rebuild.
                Err(CatalogError::Corrupt("unknown atttypid"))
                    if class.relnamespace != PG_CATALOG_NAMESPACE_OID =>
                {
                    continue;
                }
                Err(err) => return Err(err),
            };
            let entry = RelCacheEntry {
                rel: relation_locator_for_class_row(class.oid, class.relfilenode, current_db_oid),
                relation_oid: class.oid,
                namespace_oid: class.relnamespace,
                owner_oid: class.relowner,
                row_type_oid: class.reltype,
                array_type_oid: catcache
                    .type_by_oid(class.reltype)
                    .map(|row| row.typarray)
                    .unwrap_or(0),
                reltoastrelid: class.reltoastrelid,
                relpersistence: class.relpersistence,
                relkind: class.relkind,
                relhastriggers: class.relhastriggers,
                relrowsecurity: class.relrowsecurity,
                relforcerowsecurity: class.relforcerowsecurity,
                desc: RelationDesc { columns },
                index: class.relkind.eq(&'i').then(|| {
                    let Some(index) = catcache
                        .index_rows()
                        .into_iter()
                        .find(|row| row.indexrelid == class.oid)
                    else {
                        return IndexRelCacheEntry {
                            indexrelid: class.oid,
                            indrelid: 0,
                            indnatts: 0,
                            indnkeyatts: 0,
                            indisunique: false,
                            indnullsnotdistinct: false,
                            indisprimary: false,
                            indisexclusion: false,
                            indimmediate: false,
                            indisclustered: false,
                            indisvalid: false,
                            indcheckxmin: false,
                            indisready: false,
                            indislive: false,
                            indisreplident: false,
                            am_oid: class.relam,
                            am_handler_oid: catcache
                                .am_rows()
                                .into_iter()
                                .find(|am| am.oid == class.relam)
                                .map(|am| am.amhandler),
                            indkey: Vec::new(),
                            indclass: Vec::new(),
                            indcollation: Vec::new(),
                            indoption: Vec::new(),
                            opfamily_oids: Vec::new(),
                            opcintype_oids: Vec::new(),
                            indexprs: None,
                            indpred: None,
                        };
                    };
                    let indclass = index.indclass.clone();
                    let opclass_rows = catcache.opclass_rows();
                    let resolved_opclasses = indclass
                        .iter()
                        .filter_map(|oid| opclass_rows.iter().find(|row| row.oid == *oid))
                        .collect::<Vec<_>>();
                    IndexRelCacheEntry {
                        indexrelid: class.oid,
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
                        am_oid: class.relam,
                        am_handler_oid: catcache
                            .am_rows()
                            .into_iter()
                            .find(|am| am.oid == class.relam)
                            .map(|am| am.amhandler),
                        indkey: index.indkey.clone(),
                        indclass,
                        indcollation: index.indcollation.clone(),
                        indoption: index.indoption.clone(),
                        opfamily_oids: resolved_opclasses.iter().map(|row| row.opcfamily).collect(),
                        opcintype_oids: resolved_opclasses
                            .iter()
                            .map(|row| row.opcintype)
                            .collect(),
                        indexprs: index.indexprs.clone(),
                        indpred: index.indpred.clone(),
                    }
                }),
            };
            let relname = class.relname.to_ascii_lowercase();
            if class.relpersistence != 't' {
                cache.by_name.insert(relname.clone(), entry.clone());
            }
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

    pub fn with_search_path(&self, search_path: &[String]) -> Self {
        let mut cache = Self {
            by_name: BTreeMap::new(),
            by_oid: self.by_oid.clone(),
        };

        for (name, entry) in &self.by_name {
            if name.contains('.') {
                cache.by_name.insert(name.clone(), entry.clone());
            }
        }

        for schema_name in search_path.iter().rev() {
            let prefix = format!("{}.", schema_name.to_ascii_lowercase());
            for (name, entry) in &self.by_name {
                if !name.starts_with(&prefix) {
                    continue;
                }
                if let Some((_, unqualified)) = name.rsplit_once('.') {
                    cache.by_name.insert(unqualified.to_string(), entry.clone());
                }
            }
        }

        // :HACK: `get_by_name()` still normalizes `pg_catalog.foo` to `foo`,
        // so keep catalog aliases visible even when rebuilding unqualified
        // names from the current search path.
        for (name, entry) in &self.by_name {
            if !name.contains('.') && entry.namespace_oid == PG_CATALOG_NAMESPACE_OID {
                cache.by_name.insert(name.clone(), entry.clone());
            }
        }

        cache
    }

    pub fn insert(&mut self, name: impl Into<String>, entry: RelCacheEntry) {
        self.by_name.insert(
            normalize_catalog_name(&name.into()).to_ascii_lowercase(),
            entry.clone(),
        );
        self.by_oid.insert(entry.relation_oid, entry);
    }

    pub fn entries(&self) -> impl Iterator<Item = (&str, &RelCacheEntry)> {
        self.by_name
            .iter()
            .map(|(name, entry)| (name.as_str(), entry))
    }
}

fn relation_locator_for_class_row(
    relation_oid: u32,
    relfilenode: u32,
    current_db_oid: u32,
) -> RelFileLocator {
    if let Some(kind) = bootstrap_catalog_kinds()
        .into_iter()
        .find(|kind| kind.relation_oid() == relation_oid)
    {
        return bootstrap_catalog_rel(kind, current_db_oid);
    }
    if let Some(descriptor) = system_catalog_index_by_oid(relation_oid) {
        let heap_rel = bootstrap_catalog_rel(descriptor.heap_kind, current_db_oid);
        return RelFileLocator {
            spc_oid: heap_rel.spc_oid,
            db_oid: heap_rel.db_oid,
            rel_number: relfilenode,
        };
    }
    RelFileLocator {
        spc_oid: 0,
        db_oid: current_db_oid,
        rel_number: relfilenode,
    }
}

fn from_catalog_entry(entry: &CatalogEntry) -> RelCacheEntry {
    RelCacheEntry {
        rel: entry.rel,
        relation_oid: entry.relation_oid,
        namespace_oid: entry.namespace_oid,
        owner_oid: entry.owner_oid,
        row_type_oid: entry.row_type_oid,
        array_type_oid: entry.array_type_oid,
        reltoastrelid: entry.reltoastrelid,
        relpersistence: entry.relpersistence,
        relkind: entry.relkind,
        relhastriggers: entry.relhastriggers,
        relrowsecurity: entry.relrowsecurity,
        relforcerowsecurity: entry.relforcerowsecurity,
        desc: entry.desc.clone(),
        index: entry.index_meta.as_ref().map(|index| IndexRelCacheEntry {
            indexrelid: entry.relation_oid,
            indrelid: index.indrelid,
            indnatts: index.indkey.len() as i16,
            indnkeyatts: index.indkey.len() as i16,
            indisunique: index.indisunique,
            indnullsnotdistinct: false,
            indisprimary: index.indisprimary,
            indisexclusion: false,
            indimmediate: true,
            indisclustered: false,
            indisvalid: index.indisvalid,
            indcheckxmin: false,
            indisready: index.indisready,
            indislive: index.indislive,
            indisreplident: false,
            am_oid: relam_for_relkind(entry.relkind),
            am_handler_oid: None,
            indkey: index.indkey.clone(),
            indclass: index.indclass.clone(),
            indcollation: index.indcollation.clone(),
            indoption: index.indoption.clone(),
            opfamily_oids: Vec::new(),
            opcintype_oids: Vec::new(),
            indexprs: index.indexprs.clone(),
            indpred: index.indpred.clone(),
        }),
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
            cache
                .get_by_name("people")
                .map(|entry| entry.rel.rel_number),
            Some(entry.rel.rel_number)
        );
        assert_eq!(
            cache
                .get_by_oid(entry.relation_oid)
                .map(|entry| entry.rel.rel_number),
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
            cache
                .get_by_oid(entry.relation_oid)
                .map(|rel| rel.desc.columns.len()),
            Some(1)
        );
    }

    #[test]
    fn relcache_loads_zero_column_relations_from_physical_catalogs() {
        let base = temp_dir("relcache_zero_column");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "zerocol",
                RelationDesc {
                    columns: Vec::new(),
                },
            )
            .unwrap();

        let cache = RelCache::from_physical(&base).unwrap();
        assert_eq!(
            cache
                .get_by_oid(entry.relation_oid)
                .map(|rel| rel.desc.columns.len()),
            Some(0)
        );
        assert!(cache.get_by_name("zerocol").is_some());
    }

    #[test]
    fn relcache_skips_user_relations_with_dangling_type_oids() {
        let base = temp_dir("relcache_dangling_type");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let mut rows = crate::backend::catalog::rows::physical_catalog_rows_from_catcache(
            &store.catcache().unwrap(),
        );
        rows.attributes
            .iter_mut()
            .find(|row| row.attrelid == entry.relation_oid && row.attname == "id")
            .unwrap()
            .atttypid = 999_999;
        let broken = crate::backend::utils::cache::catcache::CatCache::from_rows(
            rows.namespaces,
            rows.classes,
            rows.attributes,
            rows.attrdefs,
            rows.depends,
            rows.inherits,
            rows.indexes,
            rows.rewrites,
            rows.triggers,
            rows.policies,
            rows.ams,
            rows.amops,
            rows.amprocs,
            rows.authids,
            rows.auth_members,
            rows.languages,
            rows.ts_parsers,
            rows.ts_templates,
            rows.ts_dicts,
            rows.ts_configs,
            rows.ts_config_maps,
            rows.constraints,
            rows.operators,
            rows.opclasses,
            rows.opfamilies,
            rows.procs,
            rows.casts,
            rows.collations,
            rows.databases,
            rows.tablespaces,
            rows.statistics,
            rows.types,
        );

        let cache = RelCache::from_catcache_in_db(&broken, 1).unwrap();
        assert!(cache.get_by_name("people").is_none());
        assert!(cache.get_by_name("pg_namespace").is_some());
    }
}
