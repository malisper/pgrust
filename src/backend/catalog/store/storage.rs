use std::fs;
use std::path::{Path, PathBuf};

use crate::BufferPool;
use crate::backend::access::transam::xact::{INVALID_TRANSACTION_ID, Snapshot, TransactionManager};
use crate::backend::catalog::bootstrap::bootstrap_catalog_entry;
use crate::backend::catalog::catalog::{Catalog, CatalogError};
use crate::backend::catalog::indexing::{
    insert_bootstrap_system_indexes, rebuild_system_catalog_indexes,
};
use crate::backend::catalog::loader::{
    load_catalog_from_physical, load_catalog_from_visible_physical, load_catalog_from_visible_pool,
    load_physical_catalog_rows_visible, load_physical_catalog_rows_visible_in_pool,
};
use crate::backend::catalog::persistence::sync_catalog_rows_subset;
use crate::backend::catalog::rows::physical_catalog_rows_from_catcache;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::smgr::MdStorageManager;
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::relcache::{RelCache, RelCacheEntry};
use crate::include::catalog::{BootstrapCatalogKind, bootstrap_catalog_kinds};

use super::{
    CONTROL_FILE_MAGIC, CatalogControl, CatalogStore, CatalogStoreMode, CatalogWriteContext,
};

impl CatalogStore {
    pub fn load(base_dir: impl Into<PathBuf>) -> Result<Self, CatalogError> {
        let base_dir = base_dir.into();
        let global_dir = base_dir.join("global");
        let control_path = global_dir.join("pg_control");
        if let Some(parent) = control_path.parent() {
            fs::create_dir_all(parent).map_err(|e| CatalogError::Io(e.to_string()))?;
        }

        let (mut catalog, control) = if control_path.exists() {
            let control = load_control_file(&control_path)?;
            let mut catalog = load_catalog_from_visible_physical_startup(&base_dir)?;
            insert_missing_bootstrap_relations(&mut catalog);
            insert_bootstrap_system_indexes(&mut catalog);
            catalog.next_oid = catalog.next_oid.max(control.next_oid);
            catalog.next_rel_number = catalog.next_rel_number.max(control.next_rel_number);
            (catalog, control)
        } else {
            let catalog = Catalog::default();
            let control = CatalogControl {
                next_oid: catalog.next_oid,
                next_rel_number: catalog.next_rel_number,
                bootstrap_complete: true,
            };
            persist_control_file(&control_path, &control)?;
            (catalog, control)
        };

        catalog.next_oid = catalog.next_oid.max(control.next_oid);
        catalog.next_rel_number = catalog.next_rel_number.max(control.next_rel_number);
        persist_control_file(
            &control_path,
            &CatalogControl {
                next_rel_number: catalog.next_rel_number,
                next_oid: catalog.next_oid.max(control.next_oid),
                bootstrap_complete: control.bootstrap_complete,
            },
        )?;
        sync_physical_catalogs(&base_dir, &catalog)?;

        Ok(Self {
            mode: CatalogStoreMode::Durable {
                base_dir,
                control_path,
            },
            catalog,
            control: control.clone(),
        })
    }

    pub fn new_ephemeral() -> Self {
        let catalog = Catalog::default();
        let control = CatalogControl {
            next_oid: catalog.next_oid,
            next_rel_number: catalog.next_rel_number,
            bootstrap_complete: true,
        };
        Self {
            mode: CatalogStoreMode::Ephemeral,
            catalog,
            control,
        }
    }

    pub fn catalog_snapshot(&self) -> Result<Catalog, CatalogError> {
        self.catalog_snapshot_with_control()
    }

    pub fn relcache(&self) -> Result<RelCache, CatalogError> {
        Ok(RelCache::from_catalog(&self.catalog))
    }

    pub fn catcache(&self) -> Result<CatCache, CatalogError> {
        Ok(CatCache::from_catalog(&self.catalog))
    }

    pub fn catcache_with_snapshot(
        &self,
        pool: &BufferPool<SmgrStorageBackend>,
        txns: &TransactionManager,
        snapshot: &Snapshot,
        client_id: crate::ClientId,
    ) -> Result<CatCache, CatalogError> {
        let rows = match &self.mode {
            CatalogStoreMode::Durable { base_dir, .. } => {
                load_physical_catalog_rows_visible(base_dir, pool, txns, snapshot, client_id)?
            }
            CatalogStoreMode::Ephemeral => {
                load_physical_catalog_rows_visible_in_pool(pool, txns, snapshot, client_id)?
            }
        };
        Ok(CatCache::from_rows(
            rows.namespaces,
            rows.classes,
            rows.attributes,
            rows.attrdefs,
            rows.depends,
            rows.inherits,
            rows.indexes,
            rows.rewrites,
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
        ))
    }

    pub fn relation(&self, name: &str) -> Result<Option<RelCacheEntry>, CatalogError> {
        Ok(self.relcache()?.get_by_name(name).cloned())
    }

    pub fn relcache_with_snapshot(
        &self,
        pool: &BufferPool<SmgrStorageBackend>,
        txns: &TransactionManager,
        snapshot: &Snapshot,
        client_id: crate::ClientId,
    ) -> Result<RelCache, CatalogError> {
        let catcache = self.catcache_with_snapshot(pool, txns, snapshot, client_id)?;
        RelCache::from_catcache(&catcache)
    }

    pub fn visible_table_names(&self) -> Result<Vec<String>, CatalogError> {
        let mut names = self
            .relcache()?
            .entries()
            .filter(|(_, entry)| {
                entry.relkind == 'r'
                    && entry.namespace_oid != crate::include::catalog::PG_CATALOG_NAMESPACE_OID
            })
            .map(|(name, _)| name.to_string())
            .filter(|name| !name.contains('.'))
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        Ok(names)
    }

    pub fn base_dir(&self) -> &Path {
        match &self.mode {
            CatalogStoreMode::Durable { base_dir, .. } => base_dir,
            CatalogStoreMode::Ephemeral => Path::new(""),
        }
    }

    pub(super) fn persist_catalog_kinds(
        &self,
        catalog: &Catalog,
        kinds: &[BootstrapCatalogKind],
    ) -> Result<(), CatalogError> {
        match &self.mode {
            CatalogStoreMode::Durable { base_dir, .. } => {
                self.persist_control_state(catalog)?;
                sync_physical_catalogs_kinds(base_dir, catalog, kinds)
            }
            CatalogStoreMode::Ephemeral => Ok(()),
        }
    }

    pub(super) fn persist_control_state(&self, catalog: &Catalog) -> Result<(), CatalogError> {
        match &self.mode {
            CatalogStoreMode::Durable { control_path, .. } => persist_control_file(
                control_path,
                &CatalogControl {
                    next_oid: catalog.next_oid,
                    next_rel_number: catalog.next_rel_number,
                    bootstrap_complete: true,
                },
            ),
            CatalogStoreMode::Ephemeral => Ok(()),
        }
    }

    pub(super) fn catalog_snapshot_with_control(&self) -> Result<Catalog, CatalogError> {
        match &self.mode {
            CatalogStoreMode::Durable {
                base_dir,
                control_path,
            } => {
                let mut catalog = load_catalog_from_physical(base_dir)?;
                if control_path.exists() {
                    let control = load_control_file(control_path)?;
                    catalog.next_oid = catalog.next_oid.max(control.next_oid);
                    catalog.next_rel_number = catalog.next_rel_number.max(control.next_rel_number);
                }
                Ok(catalog)
            }
            CatalogStoreMode::Ephemeral => Ok(self.catalog.clone()),
        }
    }

    pub(super) fn catalog_snapshot_with_control_for_snapshot(
        &self,
        ctx: &CatalogWriteContext,
    ) -> Result<Catalog, CatalogError> {
        let snapshot = ctx
            .txns
            .read()
            .snapshot_for_command(ctx.xid, ctx.cid)
            .map_err(|e| CatalogError::Io(format!("catalog snapshot failed: {e:?}")))?;
        let txns = ctx.txns.read();
        let mut catalog = match &self.mode {
            CatalogStoreMode::Durable {
                base_dir,
                control_path,
            } => {
                let mut catalog = load_catalog_from_visible_physical(
                    base_dir,
                    &ctx.pool,
                    &txns,
                    &snapshot,
                    ctx.client_id,
                )?;
                if control_path.exists() {
                    let control = load_control_file(control_path)?;
                    catalog.next_oid = catalog.next_oid.max(control.next_oid);
                    catalog.next_rel_number = catalog.next_rel_number.max(control.next_rel_number);
                }
                catalog
            }
            CatalogStoreMode::Ephemeral => {
                let mut catalog =
                    load_catalog_from_visible_pool(&ctx.pool, &txns, &snapshot, ctx.client_id)?;
                catalog.next_oid = catalog.next_oid.max(self.control.next_oid);
                catalog.next_rel_number = catalog.next_rel_number.max(self.control.next_rel_number);
                catalog
            }
        };
        if matches!(self.mode, CatalogStoreMode::Ephemeral) {
            catalog.next_oid = catalog.next_oid.max(self.control.next_oid);
            catalog.next_rel_number = catalog.next_rel_number.max(self.control.next_rel_number);
        }
        Ok(catalog)
    }
}

#[cfg(test)]
pub(crate) fn sync_catalog_heaps_for_tests(
    base_dir: &Path,
    catalog: &Catalog,
) -> Result<(), CatalogError> {
    let catcache = CatCache::from_catalog(catalog);
    let rows = physical_catalog_rows_from_catcache(&catcache);
    crate::backend::catalog::persistence::sync_catalog_rows(base_dir, &rows, 1)
}

fn insert_missing_bootstrap_relations(catalog: &mut Catalog) {
    for kind in bootstrap_catalog_kinds() {
        if catalog.get_by_oid(kind.relation_oid()).is_none() {
            catalog.insert(kind.relation_name(), bootstrap_catalog_entry(kind));
        }
    }
}

fn load_catalog_from_visible_physical_startup(base_dir: &Path) -> Result<Catalog, CatalogError> {
    let txns = TransactionManager::new_durable(base_dir.to_path_buf())
        .map_err(|e| CatalogError::Io(format!("transaction status load failed: {e:?}")))?;
    let snapshot = txns
        .snapshot(INVALID_TRANSACTION_ID)
        .map_err(|e| CatalogError::Io(format!("startup catalog snapshot failed: {e:?}")))?;
    let pool = BufferPool::new(SmgrStorageBackend::new(MdStorageManager::new(base_dir)), 64);
    load_catalog_from_visible_physical(base_dir, &pool, &txns, &snapshot, 0)
}

fn sync_physical_catalogs(base_dir: &Path, catalog: &Catalog) -> Result<(), CatalogError> {
    sync_physical_catalogs_kinds(base_dir, catalog, &bootstrap_catalog_kinds())
}

fn sync_physical_catalogs_kinds(
    base_dir: &Path,
    catalog: &Catalog,
    kinds: &[BootstrapCatalogKind],
) -> Result<(), CatalogError> {
    let catcache = CatCache::from_catalog(catalog);
    let mut rows = physical_catalog_rows_from_catcache(&catcache);
    if kinds.contains(&BootstrapCatalogKind::PgDescription) {
        let pool = std::sync::Arc::new(BufferPool::new(
            SmgrStorageBackend::new(MdStorageManager::new(base_dir)),
            64,
        ));
        let txns = TransactionManager::new_durable(base_dir.to_path_buf()).unwrap_or_default();
        if let Ok(snapshot) = txns.snapshot(INVALID_TRANSACTION_ID) {
            if let Ok(existing_rows) =
                load_physical_catalog_rows_visible(base_dir, &pool, &txns, &snapshot, 0)
            {
                rows.descriptions = existing_rows.descriptions;
            }
        }
    }
    sync_catalog_rows_subset(base_dir, &rows, 1, kinds)?;
    rebuild_system_catalog_indexes(base_dir)
}

fn persist_control_file(path: &Path, control: &CatalogControl) -> Result<(), CatalogError> {
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&CONTROL_FILE_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&control.next_oid.to_le_bytes());
    bytes.extend_from_slice(&control.next_rel_number.to_le_bytes());
    bytes.extend_from_slice(&(u32::from(control.bootstrap_complete)).to_le_bytes());
    fs::write(path, bytes).map_err(|e| CatalogError::Io(e.to_string()))
}

fn load_control_file(path: &Path) -> Result<CatalogControl, CatalogError> {
    let bytes = fs::read(path).map_err(|e| CatalogError::Io(e.to_string()))?;
    if bytes.len() != 16 {
        return Err(CatalogError::Corrupt("invalid control file size"));
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if magic != CONTROL_FILE_MAGIC {
        return Err(CatalogError::Corrupt("invalid control magic"));
    }
    let next_oid = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    let next_rel_number = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let bootstrap_complete = match u32::from_le_bytes(bytes[12..16].try_into().unwrap()) {
        0 => false,
        1 => true,
        _ => return Err(CatalogError::Corrupt("invalid bootstrap flag")),
    };

    Ok(CatalogControl {
        next_oid,
        next_rel_number,
        bootstrap_complete,
    })
}
