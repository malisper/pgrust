//! Seam wiring: install every one of the 18 outward `catcache-seams`
//! declarations. Thin marshal + delegate only — no logic lives here.

use backend_utils_cache_catcache_seams as seams;

use crate::{graph_machinery, init_meta, inval_support, list_path, search_path};

/// Install every outward catcache seam (called once from `seams-init`).
pub fn init_seams() {
    // graph machinery
    seams::init_cat_cache::set(|id, reloid, indexoid, nkeys, key, nbuckets| {
        graph_machinery::InitCatCache(id, reloid, indexoid, nkeys, &key, nbuckets)
    });
    seams::cat_cache_invalidate::set(graph_machinery::CatCacheInvalidate);
    seams::reset_catalog_caches_ext::set(graph_machinery::ResetCatalogCachesExt);
    seams::catalog_cache_flush_catalog::set(graph_machinery::CatalogCacheFlushCatalog);
    // syscache.c's SysCacheInvalidate arm maps to the per-cache invalidation;
    // it carries callback errors, so it returns PgResult.
    seams::syscache_invalidate::set(syscache_invalidate);

    // init + metadata
    seams::init_cat_cache_phase2::set(init_meta::InitCatCachePhase2);
    seams::cache_nkeys::set(init_meta::cache_nkeys);
    seams::cache_relisshared::set(init_meta::cache_relisshared);
    seams::cache_tupdesc_is_valid::set(init_meta::cache_tupdesc_is_valid);
    seams::with_cache_tupdesc::set(init_meta::with_cache_tupdesc);

    // single-tuple search path
    seams::search_cat_cache::set(search_path::search_cat_cache);
    seams::search_cat_cache_1::set(search_path::search_cat_cache_1);
    seams::search_cat_cache_2::set(search_path::search_cat_cache_2);
    seams::search_cat_cache_3::set(search_path::search_cat_cache_3);
    seams::search_cat_cache_4::set(search_path::search_cat_cache_4);
    seams::get_cat_cache_hash_value::set(search_path::get_cat_cache_hash_value);

    // list search path
    seams::search_cat_cache_list::set(list_path::search_cat_cache_list);

    // inval support
    seams::prepare_to_invalidate_cache_tuple::set(inval_support::prepare_to_invalidate_cache_tuple);

    // The genam/heaptuple/relcache-facing substrate seams the miss path calls
    // (`SearchCatCacheMiss`'s catalog scan, the cached-tuple `heap_copytuple`,
    // and the by-reference search-key reconstitution). Declared in `search_path`
    // and installed here — the catcache crate depends on genam-seams + the
    // relcache seams + the real heaptuple crate, none of which depend back on
    // it, so the scan path closes without a dependency cycle.
    search_path::install_substrate_seams();
}

/// `SysCacheInvalidate(cacheId, hashValue)` — the catcache arm of
/// `LocalExecuteInvalidationMessage` (syscache.c → catcache.c). Marshals the
/// infallible `CatCacheInvalidate` into the callback-error-carrying seam shape.
fn syscache_invalidate(cache_id: i32, hash_value: u32) -> types_error::PgResult<()> {
    graph_machinery::CatCacheInvalidate(cache_id, hash_value);
    Ok(())
}
