use crate::backend::catalog::catalog::Catalog;
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::relcache::{RelCache, RelCacheEntry};
use crate::include::catalog::{PgAttributeRow, PgClassRow, PgNamespaceRow};

pub fn relation_lookup_by_name<'a>(
    relcache: &'a RelCache,
    name: &str,
) -> Option<&'a RelCacheEntry> {
    relcache.get_by_name(name)
}

pub fn relation_lookup_by_oid<'a>(
    relcache: &'a RelCache,
    oid: u32,
) -> Option<&'a RelCacheEntry> {
    relcache.get_by_oid(oid)
}

pub fn namespace_lookup_by_name<'a>(catcache: &'a CatCache, name: &str) -> Option<&'a PgNamespaceRow> {
    catcache.namespace_by_name(name)
}

pub fn pg_class_lookup_by_name<'a>(catcache: &'a CatCache, name: &str) -> Option<&'a PgClassRow> {
    catcache.class_by_name(name)
}

pub fn pg_class_lookup_by_oid(catcache: &CatCache, oid: u32) -> Option<&PgClassRow> {
    catcache.class_by_oid(oid)
}

pub fn pg_attribute_rows(catcache: &CatCache, relid: u32) -> Option<&[PgAttributeRow]> {
    catcache.attributes_by_relid(relid)
}

pub fn caches_for_catalog(catalog: &Catalog) -> (RelCache, CatCache) {
    (RelCache::from_catalog(catalog), CatCache::from_catalog(catalog))
}
