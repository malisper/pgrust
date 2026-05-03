use std::fs;
use std::path::{Path, PathBuf};

use crate::BufferPool;
use crate::backend::access::transam::xact::{INVALID_TRANSACTION_ID, Snapshot, TransactionManager};
use crate::backend::catalog::catalog::{Catalog, CatalogError};
use crate::backend::catalog::indexing::system_catalog_index_entry_for_db;
use crate::backend::catalog::loader::{
    catalog_from_physical_rows_scoped, load_physical_catalog_rows_visible_in_pool,
    load_physical_catalog_rows_visible_scoped, load_visible_catalog_kind_in_pool_scoped,
};
use crate::backend::catalog::persistence::{
    apply_catalog_row_changes_subset_incremental, sync_catalog_rows_subset,
};
use crate::backend::catalog::rowcodec::pg_event_trigger_row_from_values;
use crate::backend::catalog::rows::{
    PhysicalCatalogRows, add_builtin_description_rows, physical_catalog_rows_from_catcache,
};
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::smgr::{ForkNumber, MdStorageManager, StorageManager};
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::cache::relcache::{RelCache, RelCacheEntry};
use crate::include::catalog::{
    BootstrapCatalogKind, CatalogScope, PgEventTriggerRow, PgTypeRow, bootstrap_catalog_kinds,
    system_catalog_indexes,
};

use super::relcache_init::{
    invalidate_relcache_init_file, load_relcache_init_file, persist_relcache_init_file,
    relcache_init_needs_invalidation,
};
use super::{
    CONTROL_FILE_MAGIC, CatalogControl, CatalogStore, CatalogStoreCore, CatalogStoreMode,
    CatalogWriteContext,
};

fn scope_db_oid(scope: CatalogScope) -> u32 {
    match scope {
        CatalogScope::Shared => 0,
        CatalogScope::Database(db_oid) => db_oid,
    }
}

fn scope_kinds(scope: CatalogScope) -> Vec<BootstrapCatalogKind> {
    bootstrap_catalog_kinds()
        .into_iter()
        .filter(|kind| match (scope, kind.scope()) {
            (CatalogScope::Shared, CatalogScope::Shared) => true,
            (CatalogScope::Database(_), CatalogScope::Database(_)) => true,
            _ => false,
        })
        .collect()
}

fn visible_kinds(scope: CatalogScope) -> Vec<BootstrapCatalogKind> {
    match scope {
        CatalogScope::Shared => scope_kinds(scope),
        CatalogScope::Database(_) => bootstrap_catalog_kinds().into_iter().collect(),
    }
}

fn add_extra_type_rows(rows: &mut PhysicalCatalogRows, extra_type_rows: &[PgTypeRow]) {
    for row in extra_type_rows {
        if rows.types.iter().all(|existing| existing.oid != row.oid) {
            rows.types.push(row.clone());
        }
    }
}

fn control_path_for_scope(base_dir: &Path, scope: CatalogScope) -> PathBuf {
    match scope {
        CatalogScope::Shared => base_dir.join("global").join("pg_catalog_control"),
        CatalogScope::Database(db_oid) => base_dir
            .join("base")
            .join(db_oid.to_string())
            .join("pg_db_control"),
    }
}

fn oid_control_path_for_scope(base_dir: &Path, scope: CatalogScope) -> Option<PathBuf> {
    match scope {
        CatalogScope::Shared => None,
        CatalogScope::Database(_) => Some(base_dir.join("global").join("pg_catalog_control")),
    }
}

impl CatalogStore {
    pub(crate) fn scope_db_oid(&self) -> u32 {
        scope_db_oid(self.scope)
    }

    pub fn next_oid(&self) -> u32 {
        if self.catalog_materialized {
            self.catalog.next_oid().max(self.control.next_oid)
        } else {
            self.control.next_oid
        }
    }

    pub fn load(base_dir: impl Into<PathBuf>) -> Result<Self, CatalogError> {
        let base_dir = base_dir.into();
        Self::load_shared(base_dir.clone())?;
        Self::load_database(base_dir, 1)
    }

    pub fn load_shared(base_dir: impl Into<PathBuf>) -> Result<Self, CatalogError> {
        Self::load_with_scope(base_dir.into(), CatalogScope::Shared)
    }

    pub fn load_database(base_dir: impl Into<PathBuf>, db_oid: u32) -> Result<Self, CatalogError> {
        Self::load_with_scope(base_dir.into(), CatalogScope::Database(db_oid))
    }

    fn load_with_scope(base_dir: PathBuf, scope: CatalogScope) -> Result<Self, CatalogError> {
        let control_path = control_path_for_scope(&base_dir, scope);
        let oid_control_path = oid_control_path_for_scope(&base_dir, scope);
        if let Some(parent) = control_path.parent() {
            fs::create_dir_all(parent).map_err(|e| CatalogError::Io(e.to_string()))?;
        }
        if let Some(oid_control_path) = &oid_control_path
            && let Some(parent) = oid_control_path.parent()
        {
            fs::create_dir_all(parent).map_err(|e| CatalogError::Io(e.to_string()))?;
        }
        let kinds = scope_kinds(scope);

        let (mut catalog, control, needs_bootstrap_sync, catalog_materialized) =
            if control_path.exists() {
                let control = load_control_file(&control_path)?;
                let mut catalog = Catalog::default();
                catalog.next_rel_number = catalog.next_rel_number.max(control.next_rel_number);
                (catalog, control, false, false)
            } else {
                let catalog = Catalog::default();
                let control = CatalogControl {
                    next_oid: catalog.next_oid,
                    next_rel_number: catalog.next_rel_number,
                    bootstrap_complete: true,
                };
                persist_control_file(&control_path, &control)?;
                (catalog, control, true, true)
            };
        if !needs_bootstrap_sync {
            validate_storage_relfiles_exist(&base_dir, scope)?;
        }

        let oid_next = load_effective_next_oid(&control_path, oid_control_path.as_deref())?;
        catalog.next_oid = catalog.next_oid.max(oid_next);
        catalog.next_rel_number = catalog.next_rel_number.max(control.next_rel_number);
        let effective_control = CatalogControl {
            next_rel_number: catalog.next_rel_number,
            next_oid: catalog.next_oid.max(oid_next),
            bootstrap_complete: control.bootstrap_complete,
        };
        if needs_bootstrap_sync {
            persist_scope_control_file(
                &control_path,
                oid_control_path.as_deref(),
                scope,
                &effective_control,
            )?;
        }
        if needs_bootstrap_sync {
            sync_physical_catalogs_scoped(&base_dir, &catalog, scope, &kinds)?;
        }

        Ok(Self {
            core: CatalogStoreCore {
                mode: CatalogStoreMode::Durable {
                    base_dir,
                    control_path,
                },
                scope,
                oid_control_path,
                catalog,
                catalog_materialized,
                control: effective_control,
                extra_type_rows: Vec::new(),
                stored_view_queries: Default::default(),
            },
        })
    }

    pub fn new_ephemeral() -> Self {
        Self::new_ephemeral_scope(CatalogScope::Database(1))
    }

    pub fn new_ephemeral_scope(scope: CatalogScope) -> Self {
        let catalog = Catalog::default();
        let control = CatalogControl {
            next_oid: catalog.next_oid,
            next_rel_number: catalog.next_rel_number,
            bootstrap_complete: true,
        };
        Self {
            core: CatalogStoreCore {
                mode: CatalogStoreMode::Ephemeral,
                scope,
                oid_control_path: None,
                catalog,
                catalog_materialized: true,
                control,
                extra_type_rows: Vec::new(),
                stored_view_queries: Default::default(),
            },
        }
    }

    pub fn catalog_snapshot(&self) -> Result<Catalog, CatalogError> {
        self.catalog_snapshot_with_control()
    }

    pub fn relcache(&self) -> Result<RelCache, CatalogError> {
        match &self.mode {
            CatalogStoreMode::Durable { base_dir, .. } => {
                if let Some(relcache) = load_relcache_init_file(base_dir, self.scope) {
                    return Ok(relcache);
                }
                // :HACK: full relcache builds exist for broad compatibility.
                // Backend query paths should use RelationIdGetRelation so one
                // relation descriptor is built from keyed syscache probes.
                let relcache =
                    RelCache::from_catcache_in_db(&self.catcache()?, self.scope_db_oid())?;
                persist_relcache_init_file(base_dir, self.scope, &relcache);
                Ok(relcache)
            }
            CatalogStoreMode::Ephemeral => Ok(RelCache::from_catcache_in_db(
                &self.catcache()?,
                self.scope_db_oid(),
            )?),
        }
    }

    pub fn catcache(&self) -> Result<CatCache, CatalogError> {
        match &self.mode {
            CatalogStoreMode::Durable { base_dir, .. } => {
                // :HACK: compatibility API for callers that still need the full
                // catalog at once. Normal backend lookups should go through
                // syscache and per-relation relcache instead.
                let rows = load_visible_physical_rows_startup_scoped(base_dir, self.scope)?;
                Ok(CatCache::from_rows(
                    rows.namespaces,
                    rows.classes,
                    rows.attributes,
                    rows.attrdefs,
                    rows.depends,
                    rows.inherits,
                    rows.indexes,
                    rows.rewrites,
                    rows.sequences,
                    rows.triggers,
                    rows.event_triggers,
                    rows.policies,
                    rows.publications,
                    rows.publication_rels,
                    rows.publication_namespaces,
                    rows.statistics_ext,
                    rows.statistics_ext_data,
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
                    rows.partitioned_tables,
                    rows.procs,
                    rows.aggregates,
                    rows.casts,
                    rows.conversions,
                    rows.collations,
                    rows.foreign_data_wrappers,
                    rows.foreign_servers,
                    rows.foreign_tables,
                    rows.user_mappings,
                    rows.databases,
                    rows.tablespaces,
                    rows.statistics,
                    rows.types,
                ))
            }
            CatalogStoreMode::Ephemeral => Ok(CatCache::from_catalog(&self.catalog)),
        }
    }

    pub fn catcache_with_snapshot(
        &self,
        pool: &BufferPool<SmgrStorageBackend>,
        txns: &TransactionManager,
        snapshot: &Snapshot,
        client_id: crate::ClientId,
    ) -> Result<CatCache, CatalogError> {
        let mut rows = match &self.mode {
            CatalogStoreMode::Durable { base_dir, .. } => {
                load_physical_catalog_rows_visible_scoped(
                    base_dir,
                    pool,
                    txns,
                    snapshot,
                    client_id,
                    scope_db_oid(self.scope),
                    &visible_kinds(self.scope),
                )?
            }
            CatalogStoreMode::Ephemeral => {
                load_physical_catalog_rows_visible_in_pool(pool, txns, snapshot, client_id)?
            }
        };
        add_extra_type_rows(&mut rows, &self.extra_type_rows);
        Ok(CatCache::from_rows(
            rows.namespaces,
            rows.classes,
            rows.attributes,
            rows.attrdefs,
            rows.depends,
            rows.inherits,
            rows.indexes,
            rows.rewrites,
            rows.sequences,
            rows.triggers,
            rows.event_triggers,
            rows.policies,
            rows.publications,
            rows.publication_rels,
            rows.publication_namespaces,
            rows.statistics_ext,
            rows.statistics_ext_data,
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
            rows.partitioned_tables,
            rows.procs,
            rows.aggregates,
            rows.casts,
            rows.conversions,
            rows.collations,
            rows.foreign_data_wrappers,
            rows.foreign_servers,
            rows.foreign_tables,
            rows.user_mappings,
            rows.databases,
            rows.tablespaces,
            rows.statistics,
            rows.types,
        ))
    }

    pub(crate) fn event_trigger_rows_with_snapshot(
        &self,
        pool: &BufferPool<SmgrStorageBackend>,
        txns: &TransactionManager,
        snapshot: &Snapshot,
        client_id: crate::ClientId,
    ) -> Result<Vec<PgEventTriggerRow>, CatalogError> {
        match &self.mode {
            CatalogStoreMode::Durable { base_dir, .. } => {
                Ok(load_physical_catalog_rows_visible_scoped(
                    base_dir,
                    pool,
                    txns,
                    snapshot,
                    client_id,
                    scope_db_oid(self.scope),
                    &[BootstrapCatalogKind::PgEventTrigger],
                )?
                .event_triggers)
            }
            CatalogStoreMode::Ephemeral => load_visible_catalog_kind_in_pool_scoped(
                pool,
                txns,
                snapshot,
                client_id,
                BootstrapCatalogKind::PgEventTrigger,
                scope_db_oid(self.scope),
            )?
            .into_iter()
            .map(pg_event_trigger_row_from_values)
            .collect::<Result<Vec<_>, _>>(),
        }
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
        RelCache::from_catcache_in_db(&catcache, self.scope_db_oid())
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

    pub(super) fn persist_catalog_row_changes(
        &self,
        catalog: &Catalog,
        rows_to_delete: &PhysicalCatalogRows,
        rows_to_insert: &PhysicalCatalogRows,
        kinds: &[BootstrapCatalogKind],
    ) -> Result<(), CatalogError> {
        self.persist_catalog_row_changes_with_control(
            catalog.next_oid,
            catalog.next_rel_number,
            rows_to_delete,
            rows_to_insert,
            kinds,
        )
    }

    pub(super) fn persist_catalog_row_changes_with_control(
        &self,
        next_oid: u32,
        next_rel_number: u32,
        rows_to_delete: &PhysicalCatalogRows,
        rows_to_insert: &PhysicalCatalogRows,
        kinds: &[BootstrapCatalogKind],
    ) -> Result<(), CatalogError> {
        match &self.mode {
            CatalogStoreMode::Durable { base_dir, .. } => {
                self.persist_control_values(next_oid, next_rel_number)?;
                apply_catalog_row_changes_subset_incremental(
                    base_dir,
                    rows_to_delete,
                    rows_to_insert,
                    scope_db_oid(self.scope),
                    kinds,
                )
            }
            CatalogStoreMode::Ephemeral => Ok(()),
        }
    }

    pub(super) fn persist_control_state(&self, catalog: &Catalog) -> Result<(), CatalogError> {
        self.persist_control_values(catalog.next_oid, catalog.next_rel_number)
    }

    pub(super) fn invalidate_relcache_init_for_kinds(&self, kinds: &[BootstrapCatalogKind]) {
        if !relcache_init_needs_invalidation(kinds) {
            return;
        }
        if let CatalogStoreMode::Durable { base_dir, .. } = &self.mode {
            invalidate_relcache_init_file(base_dir, self.scope);
        }
    }

    fn persist_control_values_internal(
        &self,
        next_oid: u32,
        next_rel_number: u32,
        invalidate_relcache: bool,
    ) -> Result<(), CatalogError> {
        match &self.mode {
            CatalogStoreMode::Durable {
                base_dir,
                control_path,
            } => {
                if invalidate_relcache {
                    invalidate_relcache_init_file(base_dir, self.scope);
                }
                persist_scope_control_file(
                    control_path,
                    self.oid_control_path.as_deref(),
                    self.scope,
                    &CatalogControl {
                        next_oid,
                        next_rel_number,
                        bootstrap_complete: true,
                    },
                )
            }
            CatalogStoreMode::Ephemeral => Ok(()),
        }
    }

    pub(super) fn persist_control_values(
        &self,
        next_oid: u32,
        next_rel_number: u32,
    ) -> Result<(), CatalogError> {
        self.persist_control_values_internal(next_oid, next_rel_number, true)
    }

    pub(super) fn persist_control_values_without_relcache_invalidation(
        &self,
        next_oid: u32,
        next_rel_number: u32,
    ) -> Result<(), CatalogError> {
        self.persist_control_values_internal(next_oid, next_rel_number, false)
    }

    pub(super) fn control_state(&self) -> Result<CatalogControl, CatalogError> {
        match &self.mode {
            CatalogStoreMode::Durable { control_path, .. } => {
                let mut control = load_control_file(control_path)?;
                control.next_oid = control.next_oid.max(load_effective_next_oid(
                    control_path,
                    self.oid_control_path.as_deref(),
                )?);
                Ok(control)
            }
            CatalogStoreMode::Ephemeral => Ok(self.control.clone()),
        }
    }

    pub(crate) fn set_extra_type_rows(&mut self, rows: Vec<crate::include::catalog::PgTypeRow>) {
        self.extra_type_rows = rows;
    }

    pub(crate) fn extra_type_rows(&self) -> &[crate::include::catalog::PgTypeRow] {
        &self.extra_type_rows
    }

    pub(super) fn ensure_catalog_materialized(&mut self) -> Result<(), CatalogError> {
        if self.catalog_materialized {
            return Ok(());
        }
        // :HACK: compatibility write paths still expect a full in-memory
        // Catalog. PostgreSQL routes normal reads through syscache/relcache; we
        // only materialize here for legacy broad Catalog mutation helpers.
        self.catalog = self.catalog_snapshot_with_control()?;
        self.catalog_materialized = true;
        Ok(())
    }

    pub fn allocate_oid_block(&mut self, count: u32, floor: u32) -> Result<u32, CatalogError> {
        let mut control = self.control_state()?;
        let oid = control.next_oid.max(floor);
        control.next_oid = oid.saturating_add(count);
        self.persist_control_values_without_relcache_invalidation(
            control.next_oid,
            control.next_rel_number,
        )?;
        self.control = control;
        Ok(oid)
    }

    pub(crate) fn allocate_next_oid(&mut self, requested_oid: u32) -> Result<u32, CatalogError> {
        let mut control = self.control_state()?;
        let oid = if requested_oid == 0 {
            control.next_oid
        } else {
            requested_oid
        };
        control.next_oid = control.next_oid.max(oid.saturating_add(1));
        self.persist_control_values_without_relcache_invalidation(
            control.next_oid,
            control.next_rel_number,
        )?;
        self.control = control;
        Ok(oid)
    }

    pub(super) fn catalog_snapshot_with_control(&self) -> Result<Catalog, CatalogError> {
        match &self.mode {
            CatalogStoreMode::Durable {
                base_dir,
                control_path,
            } => {
                let mut rows = load_visible_physical_rows_startup_scoped(base_dir, self.scope)?;
                add_extra_type_rows(&mut rows, &self.extra_type_rows);
                let mut catalog =
                    catalog_from_physical_rows_scoped(base_dir, rows, self.scope_db_oid())?;
                let control = load_control_file(control_path)?;
                catalog.next_oid = catalog.next_oid.max(load_effective_next_oid(
                    control_path,
                    self.oid_control_path.as_deref(),
                )?);
                catalog.next_rel_number = catalog.next_rel_number.max(control.next_rel_number);
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
                let mut rows = load_physical_catalog_rows_visible_scoped(
                    base_dir,
                    &ctx.pool,
                    &txns,
                    &snapshot,
                    ctx.client_id,
                    self.scope_db_oid(),
                    &visible_kinds(self.scope),
                )?;
                add_extra_type_rows(&mut rows, &self.extra_type_rows);
                let mut catalog =
                    catalog_from_physical_rows_scoped(base_dir, rows, self.scope_db_oid())?;
                let control = load_control_file(control_path)?;
                catalog.next_oid = catalog.next_oid.max(load_effective_next_oid(
                    control_path,
                    self.oid_control_path.as_deref(),
                )?);
                catalog.next_rel_number = catalog.next_rel_number.max(control.next_rel_number);
                catalog
            }
            CatalogStoreMode::Ephemeral => {
                let mut rows = load_physical_catalog_rows_visible_in_pool(
                    &ctx.pool,
                    &txns,
                    &snapshot,
                    ctx.client_id,
                )?;
                add_extra_type_rows(&mut rows, &self.extra_type_rows);
                let mut catalog = catalog_from_physical_rows_scoped(Path::new(""), rows, 1)?;
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

fn validate_storage_relfiles_exist(
    base_dir: &Path,
    scope: CatalogScope,
) -> Result<(), CatalogError> {
    let mut smgr = MdStorageManager::new(base_dir);

    let db_oid = scope_db_oid(scope);
    for kind in scope_kinds(scope) {
        let rel = crate::backend::catalog::bootstrap::bootstrap_catalog_rel(kind, db_oid);
        if !smgr.exists(rel, ForkNumber::Main) {
            return Err(CatalogError::Corrupt("missing physical relation relfile"));
        }
    }

    if matches!(scope, CatalogScope::Database(_)) {
        for descriptor in system_catalog_indexes() {
            let entry = system_catalog_index_entry_for_db(*descriptor, db_oid);
            if !smgr.exists(entry.rel, ForkNumber::Main) {
                return Err(CatalogError::Corrupt("missing physical relation relfile"));
            }
        }
    }

    Ok(())
}

fn load_visible_physical_rows_startup_scoped(
    base_dir: &Path,
    scope: CatalogScope,
) -> Result<PhysicalCatalogRows, CatalogError> {
    let txns = TransactionManager::new_durable(base_dir.to_path_buf())
        .map_err(|e| CatalogError::Io(format!("transaction status load failed: {e:?}")))?;
    let snapshot = txns
        .snapshot(INVALID_TRANSACTION_ID)
        .map_err(|e| CatalogError::Io(format!("startup catalog snapshot failed: {e:?}")))?;
    let pool = BufferPool::new(SmgrStorageBackend::new(MdStorageManager::new(base_dir)), 64);
    load_physical_catalog_rows_visible_scoped(
        base_dir,
        &pool,
        &txns,
        &snapshot,
        0,
        scope_db_oid(scope),
        &visible_kinds(scope),
    )
}

fn sync_physical_catalogs_scoped(
    base_dir: &Path,
    catalog: &Catalog,
    scope: CatalogScope,
    kinds: &[BootstrapCatalogKind],
) -> Result<(), CatalogError> {
    let db_oid = scope_db_oid(scope);
    let catcache = CatCache::from_catalog(catalog);
    let mut rows = physical_catalog_rows_from_catcache(&catcache);
    if kinds.contains(&BootstrapCatalogKind::PgDescription) {
        let pool = std::sync::Arc::new(BufferPool::new(
            SmgrStorageBackend::new(MdStorageManager::new(base_dir)),
            64,
        ));
        let txns = TransactionManager::new_durable(base_dir.to_path_buf()).unwrap_or_default();
        if let Ok(snapshot) = txns.snapshot(INVALID_TRANSACTION_ID) {
            if let Ok(existing_rows) = load_physical_catalog_rows_visible_scoped(
                base_dir, &pool, &txns, &snapshot, 0, db_oid, kinds,
            ) {
                rows.descriptions = existing_rows.descriptions;
            }
        }
        add_builtin_description_rows(&mut rows.descriptions, &catcache);
    }
    sync_catalog_rows_subset(base_dir, &rows, db_oid, kinds)
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

fn load_effective_next_oid(
    control_path: &Path,
    oid_control_path: Option<&Path>,
) -> Result<u32, CatalogError> {
    let control = load_control_file(control_path)?;
    if let Some(path) = oid_control_path {
        if path.exists() {
            return Ok(load_control_file(path)?.next_oid);
        }
    }
    Ok(control.next_oid)
}

fn persist_scope_control_file(
    control_path: &Path,
    oid_control_path: Option<&Path>,
    scope: CatalogScope,
    control: &CatalogControl,
) -> Result<(), CatalogError> {
    match scope {
        CatalogScope::Shared => persist_control_file(control_path, control),
        CatalogScope::Database(_) => {
            let local_control = CatalogControl {
                next_oid: load_effective_next_oid(control_path, oid_control_path)?,
                next_rel_number: control.next_rel_number,
                bootstrap_complete: control.bootstrap_complete,
            };
            persist_control_file(control_path, &local_control)?;
            if let Some(path) = oid_control_path {
                let mut oid_control = if path.exists() {
                    load_control_file(path)?
                } else {
                    CatalogControl {
                        next_oid: control.next_oid,
                        next_rel_number: control.next_rel_number,
                        bootstrap_complete: control.bootstrap_complete,
                    }
                };
                oid_control.next_oid = control.next_oid;
                persist_control_file(path, &oid_control)?;
            }
            Ok(())
        }
    }
}
