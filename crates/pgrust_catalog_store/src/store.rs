use std::collections::HashMap;
use std::path::PathBuf;

use crate::rows::PhysicalCatalogRows;
use crate::syscache::SysCacheInvalidationKey;
use crate::toasting::ToastCatalogChanges;
use crate::{Catalog, CatalogEntry};
use pgrust_catalog_data::PgTypeRow;
use pgrust_catalog_data::{BootstrapCatalogKind, CatalogScope};
use pgrust_core::RelFileLocator;
use pgrust_nodes::Query;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogStoreMode {
    Durable {
        base_dir: PathBuf,
        control_path: PathBuf,
    },
    Ephemeral,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogControl {
    pub next_oid: u32,
    pub next_rel_number: u32,
    pub bootstrap_complete: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CatalogStoreCore {
    pub mode: CatalogStoreMode,
    pub scope: CatalogScope,
    pub oid_control_path: Option<PathBuf>,
    pub catalog: Catalog,
    pub catalog_materialized: bool,
    pub control: CatalogControl,
    pub extra_type_rows: Vec<PgTypeRow>,
    pub stored_view_queries: HashMap<u32, Query>,
}

pub trait CatalogReadRuntime {
    type Error;

    fn check_catalog_interrupts(&self) -> Result<(), Self::Error>;
}

pub trait CatalogWriteRuntime: CatalogReadRuntime {
    fn insert_catalog_rows(
        &self,
        rows: &PhysicalCatalogRows,
        db_oid: u32,
        kinds: &[BootstrapCatalogKind],
    ) -> Result<(), Self::Error>;

    fn delete_catalog_rows(
        &self,
        rows: &PhysicalCatalogRows,
        db_oid: u32,
        kinds: &[BootstrapCatalogKind],
    ) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct CatalogStoreSnapshot {
    pub catalog: Catalog,
    pub catalog_materialized: bool,
    pub control: CatalogControl,
    pub scope: CatalogScope,
    pub extra_type_rows: Vec<PgTypeRow>,
    pub stored_view_queries: HashMap<u32, Query>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct CatalogMutationEffect {
    pub touched_catalogs: Vec<BootstrapCatalogKind>,
    pub syscache_keys: Vec<SysCacheInvalidationKey>,
    pub created_rels: Vec<RelFileLocator>,
    pub dropped_rels: Vec<RelFileLocator>,
    pub relation_oids: Vec<u32>,
    pub namespace_oids: Vec<u32>,
    pub type_oids: Vec<u32>,
    pub full_reset: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableResult {
    pub entry: CatalogEntry,
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
