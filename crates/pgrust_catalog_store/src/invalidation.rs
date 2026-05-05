use std::collections::BTreeSet;

use pgrust_catalog_data::BootstrapCatalogKind;

use crate::store::CatalogMutationEffect;
use crate::syscache::SysCacheInvalidationKey;

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct CatalogInvalidation {
    pub touched_catalogs: BTreeSet<BootstrapCatalogKind>,
    pub syscache_keys: BTreeSet<SysCacheInvalidationKey>,
    pub syscache_flush_catalogs: BTreeSet<BootstrapCatalogKind>,
    pub relation_oids: BTreeSet<u32>,
    pub namespace_oids: BTreeSet<u32>,
    pub type_oids: BTreeSet<u32>,
    pub full_reset: bool,
}

impl CatalogInvalidation {
    pub fn is_empty(&self) -> bool {
        !self.full_reset
            && self.touched_catalogs.is_empty()
            && self.syscache_keys.is_empty()
            && self.syscache_flush_catalogs.is_empty()
            && self.relation_oids.is_empty()
            && self.namespace_oids.is_empty()
            && self.type_oids.is_empty()
    }
}

pub fn catalog_invalidation_from_effect(effect: &CatalogMutationEffect) -> CatalogInvalidation {
    let syscache_keys = effect
        .syscache_keys
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let syscache_flush_catalogs = if syscache_keys.is_empty() {
        effect.touched_catalogs.iter().copied().collect()
    } else {
        BTreeSet::new()
    };
    CatalogInvalidation {
        touched_catalogs: effect.touched_catalogs.iter().copied().collect(),
        syscache_keys,
        syscache_flush_catalogs,
        relation_oids: effect.relation_oids.iter().copied().collect(),
        namespace_oids: effect.namespace_oids.iter().copied().collect(),
        type_oids: effect.type_oids.iter().copied().collect(),
        full_reset: effect.full_reset,
    }
}
