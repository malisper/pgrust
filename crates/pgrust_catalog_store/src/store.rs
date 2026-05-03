use std::collections::HashMap;

use crate::Catalog;
use crate::toasting::ToastCatalogChanges;
use pgrust_catalog_data::PgTypeRow;
use pgrust_catalog_data::{BootstrapCatalogKind, CatalogScope};
use pgrust_core::RelFileLocator;
use pgrust_nodes::Query;

#[derive(Debug, Clone, PartialEq)]
pub struct CatalogStoreSnapshot {
    pub catalog: Catalog,
    pub catalog_materialized: bool,
    pub scope: CatalogScope,
    pub extra_type_rows: Vec<PgTypeRow>,
    pub stored_view_queries: HashMap<u32, Query>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct CatalogMutationEffect {
    pub touched_catalogs: Vec<BootstrapCatalogKind>,
    pub created_rels: Vec<RelFileLocator>,
    pub dropped_rels: Vec<RelFileLocator>,
    pub relation_oids: Vec<u32>,
    pub namespace_oids: Vec<u32>,
    pub type_oids: Vec<u32>,
    pub full_reset: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableResult {
    pub entry: crate::CatalogEntry,
    pub toast: Option<ToastCatalogChanges>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleOwnerDependency {
    Auto,
    Internal,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuleDependencies {
    pub relation_oids: Vec<u32>,
    pub column_refs: Vec<(u32, i16)>,
    pub constraint_oids: Vec<u32>,
    pub proc_oids: Vec<u32>,
    pub type_oids: Vec<u32>,
}

impl RuleDependencies {
    pub fn from_relation_oids(relation_oids: &[u32]) -> Self {
        Self {
            relation_oids: relation_oids.to_vec(),
            column_refs: Vec::new(),
            constraint_oids: Vec::new(),
            proc_oids: Vec::new(),
            type_oids: Vec::new(),
        }
    }
}
