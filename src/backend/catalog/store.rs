use std::path::PathBuf;

use parking_lot::RwLock;

use crate::BufferPool;
use crate::backend::access::transam::xact::{CommandId, TransactionId, TransactionManager};
use crate::backend::catalog::catalog::{Catalog, CatalogEntry, CatalogError};
use crate::backend::catalog::toasting::ToastCatalogChanges;
use crate::backend::storage::buffer::storage_backend::SmgrStorageBackend;
use crate::backend::storage::lmgr::TransactionWaiter;
use crate::backend::storage::smgr::RelFileLocator;
use crate::backend::utils::misc::interrupts::{InterruptState, check_for_interrupts};
use crate::include::catalog::{BootstrapCatalogKind, CatalogScope};

// Mirror PostgreSQL's catalog split: durable control/storage lives in `storage`,
// while relation DDL and catalog row mutation paths live in `heap`.
#[path = "store/heap.rs"]
mod heap;
#[path = "store/relcache_init.rs"]
mod relcache_init;
#[path = "store/roles.rs"]
mod roles;
#[path = "store/storage.rs"]
mod storage;
#[cfg(test)]
pub(crate) use storage::sync_catalog_heaps_for_tests;

const CONTROL_FILE_MAGIC: u32 = 0x5052_4743;
pub(crate) const DEFAULT_FIRST_REL_NUMBER: u32 = 16000;
pub(crate) const DEFAULT_FIRST_USER_OID: u32 = 16_384;

#[derive(Debug, Clone, PartialEq, Eq)]
enum CatalogStoreMode {
    Durable {
        base_dir: PathBuf,
        control_path: PathBuf,
    },
    Ephemeral,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CatalogStore {
    mode: CatalogStoreMode,
    scope: CatalogScope,
    oid_control_path: Option<PathBuf>,
    catalog: Catalog,
    control: CatalogControl,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
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
    pub entry: CatalogEntry,
    pub toast: Option<ToastCatalogChanges>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleOwnerDependency {
    Auto,
    Internal,
}

pub struct CatalogWriteContext {
    pub pool: std::sync::Arc<BufferPool<SmgrStorageBackend>>,
    pub txns: std::sync::Arc<RwLock<TransactionManager>>,
    pub xid: TransactionId,
    pub cid: CommandId,
    pub client_id: crate::ClientId,
    pub waiter: Option<std::sync::Arc<TransactionWaiter>>,
    pub interrupts: std::sync::Arc<InterruptState>,
}

impl CatalogWriteContext {
    pub fn check_for_interrupts(&self) -> Result<(), CatalogError> {
        check_for_interrupts(&self.interrupts).map_err(CatalogError::Interrupted)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogControl {
    next_oid: u32,
    next_rel_number: u32,
    bootstrap_complete: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
    use crate::backend::catalog::bootstrap::bootstrap_catalog_rel;
    use crate::backend::catalog::column_desc;
    use crate::backend::catalog::loader::load_physical_catalog_rows;
    use crate::backend::catalog::rows::physical_catalog_rows_for_catalog_entry;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::backend::storage::smgr::{
        BLCKSZ, ForkNumber, MdStorageManager, StorageManager, segment_path,
    };
    use crate::include::access::nbtree::{BTP_DELETED, bt_page_data_items, bt_page_get_opaque};
    use crate::include::catalog::{
        BOOTSTRAP_SUPERUSER_NAME, BOOTSTRAP_SUPERUSER_OID, BTREE_AM_OID, C_COLLATION_OID,
        CURRENT_DATABASE_NAME, CatalogScope, DEFAULT_COLLATION_OID, DEFAULT_TABLESPACE_OID,
        DEPENDENCY_AUTO, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL, HEAP_TABLE_AM_OID, INT4_TYPE_OID,
        INT8_TYPE_OID, JSON_TYPE_OID, OID_TYPE_OID, PG_ATTRDEF_RELATION_OID, PG_CLASS_RELATION_OID,
        PG_CONSTRAINT_RELATION_OID, PG_LANGUAGE_INTERNAL_OID, PG_NAMESPACE_RELATION_OID,
        PG_TOAST_NAMESPACE_OID, PG_TYPE_RELATION_OID, POSIX_COLLATION_OID, PUBLIC_NAMESPACE_OID,
        TEXT_TYPE_OID, VARCHAR_TYPE_OID, system_catalog_indexes,
    };
    use crate::include::nodes::primnodes::RelationDesc;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("pgrust_catalog_{label}_{nanos}"))
    }

    fn durable_write_context(
        base: &PathBuf,
    ) -> (
        Arc<BufferPool<SmgrStorageBackend>>,
        Arc<RwLock<TransactionManager>>,
        CatalogWriteContext,
    ) {
        let mut smgr = MdStorageManager::new(base);
        for kind in crate::backend::catalog::bootstrap::bootstrap_catalog_kinds() {
            smgr.open(bootstrap_catalog_rel(kind, 1)).unwrap();
        }
        let pool = Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 16));
        let txns = Arc::new(RwLock::new(
            TransactionManager::new_durable(base.clone()).unwrap(),
        ));
        let xid = txns.write().begin();
        let ctx = CatalogWriteContext {
            pool: Arc::clone(&pool),
            txns: Arc::clone(&txns),
            xid,
            cid: 0,
            client_id: 0,
            waiter: None,
            interrupts: Arc::new(InterruptState::new()),
        };
        (pool, txns, ctx)
    }

    fn commit_catalog_write(txns: &Arc<RwLock<TransactionManager>>, xid: TransactionId) {
        txns.write().commit(xid).unwrap();
    }

    #[cfg(unix)]
    fn system_index_path(base: &PathBuf, db_oid: u32, relname: &str) -> PathBuf {
        let descriptor = system_catalog_indexes()
            .iter()
            .find(|descriptor| descriptor.relation_name == relname)
            .unwrap();
        let heap_rel = bootstrap_catalog_rel(descriptor.heap_kind, db_oid);
        segment_path(
            base,
            RelFileLocator {
                spc_oid: heap_rel.spc_oid,
                db_oid: heap_rel.db_oid,
                rel_number: descriptor.relation_oid,
            },
            ForkNumber::Main,
            0,
        )
    }

    #[cfg(unix)]
    fn system_index_rel(db_oid: u32, relname: &str) -> RelFileLocator {
        let descriptor = system_catalog_indexes()
            .iter()
            .find(|descriptor| descriptor.relation_name == relname)
            .unwrap();
        let heap_rel = bootstrap_catalog_rel(descriptor.heap_kind, db_oid);
        RelFileLocator {
            spc_oid: heap_rel.spc_oid,
            db_oid: heap_rel.db_oid,
            rel_number: descriptor.relation_oid,
        }
    }

    fn vacuum_relation_via_command(
        base: &PathBuf,
        relation_name: &str,
        txns: Option<Arc<RwLock<TransactionManager>>>,
    ) {
        // Use the database-visible catalog view so shared catalogs like pg_authid
        // resolve through the normal pg_catalog lookup path during VACUUM.
        let store = CatalogStore::load(base).unwrap();
        let relcache = store.relcache().unwrap();

        let mut smgr = MdStorageManager::new(base);
        for kind in crate::backend::catalog::bootstrap::bootstrap_catalog_kinds() {
            smgr.open(bootstrap_catalog_rel(kind, 1)).unwrap();
        }
        let pool = Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 16));
        // :HACK: Tests that keep a durable TransactionManager alive after
        // commit must reuse it here; reopening from disk can miss unflushed
        // status bits that the live server would still have in memory.
        let txns = txns.unwrap_or_else(|| {
            Arc::new(RwLock::new(
                TransactionManager::new_durable(base.clone()).unwrap(),
            ))
        });
        let snapshot = txns.read().snapshot(INVALID_TRANSACTION_ID).unwrap();
        let mut ctx = crate::backend::executor::ExecutorContext {
            pool,
            txns,
            txn_waiter: None,
            sequences: Some(Arc::new(crate::pgrust::database::SequenceRuntime::new_ephemeral())),
            large_objects: Some(Arc::new(
                crate::pgrust::database::LargeObjectRuntime::new_ephemeral(),
            )),
            checkpoint_stats:
                crate::backend::utils::misc::checkpoint::CheckpointStatsSnapshot::default(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            interrupts: Arc::new(InterruptState::new()),
            stats: Arc::new(RwLock::new(
                crate::pgrust::database::DatabaseStatsStore::with_default_io_rows(),
            )),
            session_stats: Arc::new(RwLock::new(
                crate::pgrust::database::SessionStatsState::default(),
            )),
            snapshot,
            client_id: 0,
            current_user_oid: BOOTSTRAP_SUPERUSER_OID,
            next_command_id: 0,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: Vec::new(),
            timed: false,
            allow_side_effects: true,
            catalog: None,
            compiled_functions: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
            deferred_foreign_keys: None,
        };
        crate::backend::commands::tablecmds::execute_vacuum(
            crate::backend::parser::VacuumStatement {
                targets: vec![crate::backend::parser::MaintenanceTarget {
                    table_name: relation_name.to_string(),
                    columns: Vec::new(),
                    only: false,
                }],
                analyze: false,
                full: false,
                verbose: false,
                skip_locked: false,
                buffer_usage_limit: None,
            },
            &relcache,
            &mut ctx,
        )
        .unwrap();
    }

    #[cfg(unix)]
    fn count_leaf_btree_items(base: &PathBuf, rel: RelFileLocator) -> usize {
        let mut smgr = MdStorageManager::new(base);
        let nblocks = smgr.nblocks(rel, ForkNumber::Main).unwrap();
        let mut page = [0u8; BLCKSZ];
        let mut count = 0usize;
        for block in 1..nblocks {
            smgr.read_block(rel, ForkNumber::Main, block, &mut page)
                .unwrap();
            let opaque = bt_page_get_opaque(&page).unwrap();
            if !opaque.is_leaf() || opaque.btpo_flags & BTP_DELETED != 0 {
                continue;
            }
            count += bt_page_data_items(&page).unwrap().len();
        }
        count
    }

    #[test]
    fn catalog_store_roundtrips() {
        let base = temp_dir("roundtrip");
        let mut store = CatalogStore::load(&base).unwrap();
        assert!(store.catalog_snapshot().unwrap().get("pg_class").is_some());
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("name", SqlType::new(SqlTypeKind::Text), false),
                        column_desc("note", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();
        assert_eq!(entry.rel.rel_number, DEFAULT_FIRST_REL_NUMBER);
        assert!(entry.relation_oid >= DEFAULT_FIRST_USER_OID);

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        let reopened_entry = reopened_catalog.get("people").unwrap();
        assert_eq!(reopened_entry.rel.rel_number, DEFAULT_FIRST_REL_NUMBER);
        assert_eq!(reopened_entry.desc.columns.len(), 3);
    }

    #[test]
    fn catalog_store_relcache_init_file_recovers_from_corruption() {
        let base = temp_dir("relcache_init_corrupt");
        let store = CatalogStore::load(&base).unwrap();
        let init_path =
            super::relcache_init::relcache_init_path_for_scope(&base, CatalogScope::Database(1));
        assert!(store.relcache().unwrap().get_by_name("pg_class").is_some());
        assert!(init_path.exists(), "relcache init file should be written");

        fs::write(&init_path, b"corrupt").unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        assert!(
            reopened
                .relcache()
                .unwrap()
                .get_by_name("pg_class")
                .is_some()
        );
        let rewritten = fs::read_to_string(&init_path).unwrap();
        assert!(
            rewritten.contains("\"magic\""),
            "corrupt relcache init file should be regenerated"
        );
    }

    #[test]
    fn catalog_store_relcache_init_file_is_invalidated_on_catalog_write() {
        let base = temp_dir("relcache_init_invalidate");
        let mut store = CatalogStore::load(&base).unwrap();
        let init_path =
            super::relcache_init::relcache_init_path_for_scope(&base, CatalogScope::Database(1));
        store.relcache().unwrap();
        assert!(init_path.exists(), "relcache init file should be written");

        store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();
        assert!(
            !init_path.exists(),
            "catalog writes should invalidate relcache init files"
        );

        let relcache = store.relcache().unwrap();
        assert!(relcache.get_by_name("people").is_some());
        assert!(
            init_path.exists(),
            "relcache init file should be regenerated on next relcache build"
        );
    }

    #[test]
    fn catalog_store_comment_write_preserves_relcache_init_file() {
        let base = temp_dir("relcache_init_comment");
        let mut store = CatalogStore::load(&base).unwrap();
        let created = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();
        let init_path =
            super::relcache_init::relcache_init_path_for_scope(&base, CatalogScope::Database(1));
        store.relcache().unwrap();
        assert!(init_path.exists(), "relcache init file should be written");

        let (pool, txns, ctx) = durable_write_context(&base);
        let effect = store
            .comment_relation_mvcc(created.relation_oid, Some("hello"), &ctx)
            .unwrap();
        let _ = (effect, pool);
        commit_catalog_write(&txns, ctx.xid);

        assert!(
            init_path.exists(),
            "comment-only writes should not invalidate relcache init files"
        );
    }

    #[test]
    fn catalog_store_shared_tablespace_write_preserves_relcache_init_file() {
        let base = temp_dir("relcache_init_tablespace");
        let mut store = CatalogStore::load_shared(&base).unwrap();
        let init_path =
            super::relcache_init::relcache_init_path_for_scope(&base, CatalogScope::Shared);
        store.relcache().unwrap();
        assert!(init_path.exists(), "shared relcache init file should be written");

        let (pool, txns, ctx) = durable_write_context(&base);
        let (_, effect) = store
            .create_tablespace_mvcc("tblspc", BOOTSTRAP_SUPERUSER_OID, &ctx)
            .unwrap();
        let _ = (effect, pool);
        commit_catalog_write(&txns, ctx.xid);

        assert!(
            init_path.exists(),
            "tablespace rows should not invalidate shared relcache init files"
        );
    }

    #[test]
    fn catalog_store_persists_column_defaults() {
        let base = temp_dir("defaults_roundtrip");
        let mut store = CatalogStore::load(&base).unwrap();
        let mut desc = RelationDesc {
            columns: vec![
                column_desc("b1", SqlType::with_bit_len(SqlTypeKind::Bit, 4), false),
                column_desc("b2", SqlType::with_bit_len(SqlTypeKind::VarBit, 5), true),
            ],
        };
        desc.columns[0].default_expr = Some("'1001'".into());
        desc.columns[1].default_expr = Some("B'0101'".into());
        store.create_table("bit_defaults", desc).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let relcache = reopened.relcache().unwrap();
        let entry = relcache.get_by_name("bit_defaults").unwrap();
        assert_eq!(
            entry.desc.columns[0].default_expr.as_deref(),
            Some("'1001'")
        );
        assert_eq!(
            entry.desc.columns[1].default_expr.as_deref(),
            Some("B'0101'")
        );
    }

    #[test]
    fn catalog_store_persists_pg_attrdef_rows() {
        let base = temp_dir("attrdef_rows");
        let mut store = CatalogStore::load(&base).unwrap();
        let mut desc = RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("note", SqlType::new(SqlTypeKind::Text), true),
            ],
        };
        desc.columns[1].default_expr = Some("'hello'".into());
        let entry = store.create_table("notes", desc).unwrap();

        let rows = load_physical_catalog_rows(&base).unwrap();
        let attrdef = rows
            .attrdefs
            .iter()
            .find(|row| row.adrelid == entry.relation_oid && row.adnum == 2)
            .unwrap();
        assert_eq!(attrdef.adbin, "'hello'");
        assert!(attrdef.oid >= DEFAULT_FIRST_USER_OID);
    }

    #[test]
    fn catalog_store_persists_pg_depend_rows() {
        let base = temp_dir("depend_rows");
        let mut store = CatalogStore::load(&base).unwrap();
        let mut desc = RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("note", SqlType::new(SqlTypeKind::Text), true),
            ],
        };
        desc.columns[1].default_expr = Some("'hello'".into());
        let entry = store.create_table("notes", desc).unwrap();
        let attrdef_oid = entry.desc.columns[1].attrdef_oid.unwrap();
        let constraint_oid = entry.desc.columns[0].not_null_constraint_oid.unwrap();

        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_CLASS_RELATION_OID
                && row.objid == entry.relation_oid
                && row.objsubid == 0
                && row.refclassid == PG_NAMESPACE_RELATION_OID
                && row.refobjid == PUBLIC_NAMESPACE_OID
                && row.refobjsubid == 0
                && row.deptype == DEPENDENCY_NORMAL
        }));
        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_TYPE_RELATION_OID
                && row.objid == entry.row_type_oid
                && row.objsubid == 0
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == entry.relation_oid
                && row.refobjsubid == 0
                && row.deptype == DEPENDENCY_INTERNAL
        }));
        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_ATTRDEF_RELATION_OID
                && row.objid == attrdef_oid
                && row.objsubid == 0
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == entry.relation_oid
                && row.refobjsubid == 2
                && row.deptype == DEPENDENCY_AUTO
        }));
        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_CONSTRAINT_RELATION_OID
                && row.objid == constraint_oid
                && row.objsubid == 0
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == entry.relation_oid
                && row.refobjsubid == 1
                && row.deptype == DEPENDENCY_AUTO
        }));

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.depend_rows().iter().any(|row| {
            row.classid == PG_CONSTRAINT_RELATION_OID
                && row.objid == constraint_oid
                && row.objsubid == 0
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == entry.relation_oid
                && row.refobjsubid == 1
                && row.deptype == DEPENDENCY_AUTO
        }));
    }

    #[test]
    fn catalog_store_persists_pg_index_rows() {
        let base = temp_dir("index_rows");
        let mut store = CatalogStore::load(&base).unwrap();
        let table = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("name", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();
        let index = store
            .create_index(
                "people_name_idx",
                "people",
                true,
                &["id".into(), "name".into()],
            )
            .unwrap();

        let rows = load_physical_catalog_rows(&base).unwrap();
        let index_row = rows
            .indexes
            .iter()
            .find(|row| row.indexrelid == index.relation_oid)
            .unwrap();
        assert_eq!(index_row.indrelid, table.relation_oid);
        assert_eq!(index_row.indnatts, 2);
        assert_eq!(index_row.indnkeyatts, 2);
        assert!(index_row.indisunique);
        assert_eq!(index_row.indkey, vec![1, 2]);

        let class_row = rows
            .classes
            .iter()
            .find(|row| row.oid == index.relation_oid)
            .unwrap();
        assert_eq!(class_row.relkind, 'i');
        assert_eq!(class_row.relam, BTREE_AM_OID);
        assert_eq!(class_row.relpersistence, 'p');
        assert_eq!(class_row.relnamespace, PUBLIC_NAMESPACE_OID);
        assert_eq!(class_row.reltype, 0);

        let table_row = rows
            .classes
            .iter()
            .find(|row| row.oid == table.relation_oid)
            .unwrap();
        assert_eq!(table_row.relam, HEAP_TABLE_AM_OID);
        assert_eq!(table_row.relpersistence, 'p');

        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_CLASS_RELATION_OID
                && row.objid == index.relation_oid
                && row.objsubid == 0
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == table.relation_oid
                && row.refobjsubid == 1
                && row.deptype == DEPENDENCY_AUTO
        }));
        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_CLASS_RELATION_OID
                && row.objid == index.relation_oid
                && row.objsubid == 0
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == table.relation_oid
                && row.refobjsubid == 2
                && row.deptype == DEPENDENCY_AUTO
        }));

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        let reopened_index = reopened_catalog.get("people_name_idx").unwrap();
        assert_eq!(reopened_index.relkind, 'i');
        assert_eq!(
            reopened_index.index_meta.as_ref().map(|meta| (
                meta.indrelid,
                meta.indkey.clone(),
                meta.indisunique
            )),
            Some((table.relation_oid, vec![1, 2], true))
        );
    }

    #[test]
    fn catalog_store_creates_toast_table_and_index() {
        let base = temp_dir("toast_create");
        let mut store = CatalogStore::load(&base).unwrap();
        let table = store
            .create_table(
                "docs",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("payload", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();

        assert_ne!(table.reltoastrelid, 0);

        let rows = load_physical_catalog_rows(&base).unwrap();
        let table_row = rows
            .classes
            .iter()
            .find(|row| row.oid == table.relation_oid)
            .unwrap();
        assert_eq!(table_row.reltoastrelid, table.reltoastrelid);

        let toast_row = rows
            .classes
            .iter()
            .find(|row| row.oid == table.reltoastrelid)
            .unwrap();
        assert_eq!(toast_row.relkind, 't');
        assert_eq!(toast_row.relnamespace, PG_TOAST_NAMESPACE_OID);

        let toast_index = rows
            .indexes
            .iter()
            .find(|row| row.indrelid == toast_row.oid)
            .unwrap();
        assert!(toast_index.indisunique);
        assert_eq!(toast_index.indkey, vec![1, 2]);

        assert!(rows.depends.iter().any(|row| {
            row.classid == PG_CLASS_RELATION_OID
                && row.objid == toast_row.oid
                && row.refclassid == PG_CLASS_RELATION_OID
                && row.refobjid == table.relation_oid
                && row.deptype == DEPENDENCY_INTERNAL
        }));
    }

    #[test]
    fn catalog_store_persists_pg_am_rows() {
        let base = temp_dir("am_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.ams.iter().any(|row| {
            row.oid == HEAP_TABLE_AM_OID
                && row.amname == "heap"
                && row.amhandler == 3
                && row.amtype == 't'
        }));
        assert!(rows.ams.iter().any(|row| {
            row.oid == BTREE_AM_OID
                && row.amname == "btree"
                && row.amhandler == 330
                && row.amtype == 'i'
        }));
    }

    #[test]
    fn catalog_store_persists_pg_authid_rows() {
        let base = temp_dir("authid_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.authids.iter().any(|row| {
            row.oid == BOOTSTRAP_SUPERUSER_OID
                && row.rolname == BOOTSTRAP_SUPERUSER_NAME
                && row.rolsuper
                && row.rolcreatedb
                && row.rolcanlogin
                && row.rolconnlimit == -1
        }));
    }

    #[test]
    fn catalog_store_persists_pg_auth_members_rows() {
        let base = temp_dir("auth_members_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.auth_members.is_empty());
    }

    #[test]
    fn catalog_store_persists_pg_language_rows() {
        let base = temp_dir("language_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.languages.iter().any(|row| {
            row.oid == PG_LANGUAGE_INTERNAL_OID
                && row.lanname == "internal"
                && row.lanowner == BOOTSTRAP_SUPERUSER_OID
        }));
        assert!(
            rows.languages
                .iter()
                .any(|row| row.lanname == "sql" && row.lanpltrusted)
        );
    }

    #[test]
    fn catalog_store_persists_created_role_rows() {
        let base = temp_dir("create_role_rows");
        let mut store = CatalogStore::load(&base).unwrap();
        let created = store
            .create_role_direct(
                "app_user",
                &crate::backend::catalog::roles::RoleAttributes {
                    rolcanlogin: true,
                    ..crate::backend::catalog::roles::RoleAttributes::default()
                },
            )
            .unwrap();
        let reopened = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(reopened.base_dir()).unwrap();
        assert!(
            rows.authids
                .iter()
                .any(|row| row.oid == created.oid && row.rolname == "app_user" && row.rolcanlogin)
        );
    }

    #[test]
    fn catalog_store_renames_and_drops_role_rows() {
        let base = temp_dir("rename_drop_role_rows");
        let mut store = CatalogStore::load(&base).unwrap();
        store
            .create_role_direct(
                "app_user",
                &crate::backend::catalog::roles::RoleAttributes::default(),
            )
            .unwrap();
        let renamed = store.rename_role_direct("app_user", "app_owner").unwrap();
        assert_eq!(renamed.rolname, "app_owner");
        let dropped = store.drop_role_direct("app_owner").unwrap();
        assert_eq!(dropped.rolname, "app_owner");

        let reopened = CatalogStore::load(&base).unwrap();
        let rows = reopened.catcache().unwrap().authid_rows();
        assert!(
            !rows
                .iter()
                .any(|row| row.rolname == "app_user" || row.rolname == "app_owner")
        );
    }

    #[cfg(unix)]
    #[test]
    fn catalog_store_role_mutations_reuse_shared_auth_relfiles() {
        let base = temp_dir("role_relfile_reuse");
        let mut store = CatalogStore::load_shared(&base).unwrap();
        let authid_path = segment_path(
            &base,
            bootstrap_catalog_rel(BootstrapCatalogKind::PgAuthId, 1),
            ForkNumber::Main,
            0,
        );
        let auth_members_path = segment_path(
            &base,
            bootstrap_catalog_rel(BootstrapCatalogKind::PgAuthMembers, 1),
            ForkNumber::Main,
            0,
        );
        let authid_before = fs::metadata(&authid_path).unwrap();
        let auth_members_before = fs::metadata(&auth_members_path).unwrap();

        let parent = store
            .create_role_direct(
                "parent_role",
                &crate::backend::catalog::roles::RoleAttributes::default(),
            )
            .unwrap();
        let member = store
            .create_role_direct(
                "member_role",
                &crate::backend::catalog::roles::RoleAttributes::default(),
            )
            .unwrap();
        store
            .rename_role_direct("member_role", "member_owner")
            .unwrap();
        store
            .grant_role_membership_direct(
                &crate::backend::catalog::role_memberships::NewRoleMembership {
                    roleid: parent.oid,
                    member: member.oid,
                    grantor: BOOTSTRAP_SUPERUSER_OID,
                    admin_option: false,
                    inherit_option: true,
                    set_option: true,
                },
            )
            .unwrap();
        store
            .update_role_membership_options_direct(
                parent.oid,
                member.oid,
                BOOTSTRAP_SUPERUSER_OID,
                true,
                false,
                false,
            )
            .unwrap();
        store
            .revoke_role_membership_direct(parent.oid, member.oid, BOOTSTRAP_SUPERUSER_OID)
            .unwrap();
        store.drop_role_direct("member_owner").unwrap();
        store.drop_role_direct("parent_role").unwrap();

        let authid_after = fs::metadata(&authid_path).unwrap();
        let auth_members_after = fs::metadata(&auth_members_path).unwrap();
        assert_eq!(authid_before.ino(), authid_after.ino());
        assert_eq!(auth_members_before.ino(), auth_members_after.ino());
    }

    #[test]
    fn catalog_store_persists_role_memberships_and_option_updates() {
        let base = temp_dir("auth_membership_mutations");
        let mut store = CatalogStore::load(&base).unwrap();
        let parent = store
            .create_role_direct(
                "parent_role",
                &crate::backend::catalog::roles::RoleAttributes::default(),
            )
            .unwrap();
        let member = store
            .create_role_direct(
                "member_role",
                &crate::backend::catalog::roles::RoleAttributes::default(),
            )
            .unwrap();
        let created = store
            .grant_role_membership_direct(
                &crate::backend::catalog::role_memberships::NewRoleMembership {
                    roleid: parent.oid,
                    member: member.oid,
                    grantor: BOOTSTRAP_SUPERUSER_OID,
                    admin_option: false,
                    inherit_option: true,
                    set_option: true,
                },
            )
            .unwrap();
        let updated = store
            .update_role_membership_options_direct(
                parent.oid,
                member.oid,
                BOOTSTRAP_SUPERUSER_OID,
                true,
                false,
                false,
            )
            .unwrap();
        assert_eq!(created.oid, updated.oid);
        assert!(updated.admin_option);

        let reopened = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(reopened.base_dir()).unwrap();
        assert!(rows.auth_members.iter().any(|row| {
            row.oid == created.oid
                && row.roleid == parent.oid
                && row.member == member.oid
                && row.admin_option
                && !row.inherit_option
                && !row.set_option
        }));
    }

    #[cfg(unix)]
    #[test]
    fn catalog_store_database_row_mutations_reuse_shared_database_relfile() {
        let base = temp_dir("database_relfile_reuse");
        let mut store = CatalogStore::load_shared(&base).unwrap();
        let database_path = segment_path(
            &base,
            bootstrap_catalog_rel(BootstrapCatalogKind::PgDatabase, 1),
            ForkNumber::Main,
            0,
        );
        let before = fs::metadata(&database_path).unwrap();

        let created = store
            .create_database_row_direct(crate::include::catalog::PgDatabaseRow {
                oid: 0,
                datname: "tenant".into(),
                datdba: BOOTSTRAP_SUPERUSER_OID,
                encoding: 6,
                datlocprovider: 'c',
                dattablespace: DEFAULT_TABLESPACE_OID,
                datistemplate: false,
                datallowconn: true,
                datconnlimit: -1,
                datcollate: "C".into(),
                datctype: "C".into(),
                datlocale: None,
                daticurules: None,
                datcollversion: None,
                datacl: None,
            })
            .unwrap();
        assert_eq!(created.datname, "tenant");
        let dropped = store.drop_database_row_direct("tenant").unwrap();
        assert_eq!(dropped.datname, "tenant");

        let after = fs::metadata(&database_path).unwrap();
        assert_eq!(before.ino(), after.ino());
    }

    #[test]
    fn catalog_store_persists_pg_operator_rows() {
        let base = temp_dir("operator_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.operators.iter().any(|row| {
            row.oid == 91
                && row.oprname == "="
                && row.oprleft == crate::include::catalog::BOOL_TYPE_OID
                && row.oprright == crate::include::catalog::BOOL_TYPE_OID
                && row.oprcode == crate::include::catalog::BOOL_CMP_EQ_PROC_OID
                && row.oprcanmerge
                && row.oprcanhash
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 96
                && row.oprname == "="
                && row.oprleft == INT4_TYPE_OID
                && row.oprright == INT4_TYPE_OID
                && row.oprcode == crate::include::catalog::INT4_CMP_EQ_PROC_OID
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 3877
                && row.oprname == "^@"
                && row.oprleft == TEXT_TYPE_OID
                && row.oprright == TEXT_TYPE_OID
                && row.oprcode == crate::include::catalog::TEXT_STARTS_WITH_PROC_OID
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 664
                && row.oprname == "<"
                && row.oprleft == TEXT_TYPE_OID
                && row.oprright == TEXT_TYPE_OID
                && row.oprcode == crate::include::catalog::TEXT_CMP_LT_PROC_OID
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 667
                && row.oprname == ">="
                && row.oprleft == TEXT_TYPE_OID
                && row.oprright == TEXT_TYPE_OID
                && row.oprcode == crate::include::catalog::TEXT_CMP_GE_PROC_OID
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 1784
                && row.oprname == "="
                && row.oprleft == crate::include::catalog::BIT_TYPE_OID
                && row.oprright == crate::include::catalog::BIT_TYPE_OID
                && row.oprcode == crate::include::catalog::BIT_CMP_EQ_PROC_OID
                && row.oprcanmerge
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 1806
                && row.oprname == "<"
                && row.oprleft == crate::include::catalog::VARBIT_TYPE_OID
                && row.oprright == crate::include::catalog::VARBIT_TYPE_OID
                && row.oprcode == crate::include::catalog::VARBIT_CMP_LT_PROC_OID
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 1955
                && row.oprname == "="
                && row.oprleft == crate::include::catalog::BYTEA_TYPE_OID
                && row.oprright == crate::include::catalog::BYTEA_TYPE_OID
                && row.oprcode == crate::include::catalog::BYTEA_CMP_EQ_PROC_OID
                && row.oprcanmerge
                && row.oprcanhash
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 1957
                && row.oprname == "<"
                && row.oprleft == crate::include::catalog::BYTEA_TYPE_OID
                && row.oprright == crate::include::catalog::BYTEA_TYPE_OID
                && row.oprcode == crate::include::catalog::BYTEA_CMP_LT_PROC_OID
        }));
        assert!(rows.operators.iter().any(|row| {
            row.oid == 3240
                && row.oprname == "="
                && row.oprleft == crate::include::catalog::JSONB_TYPE_OID
                && row.oprright == crate::include::catalog::JSONB_TYPE_OID
                && row.oprcode == crate::include::catalog::JSONB_CMP_EQ_PROC_OID
                && row.oprcanmerge
                && row.oprcanhash
        }));
    }

    #[test]
    fn catalog_store_persists_pg_constraint_rows() {
        let base = temp_dir("constraint_rows");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("note", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();
        let constraint_oid = entry.desc.columns[0].not_null_constraint_oid.unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();
        assert!(rows.constraints.iter().any(|row| {
            row.oid == constraint_oid
                && row.conname == "people_id_not_null"
                && row.contype == 'n'
                && row.conrelid == entry.relation_oid
                && row.connamespace == PUBLIC_NAMESPACE_OID
                && row.convalidated
        }));
    }

    #[test]
    fn catalog_store_loads_not_null_constraint_oids_from_pg_constraint() {
        let base = temp_dir("constraint_oid_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let entry = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("note", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();
        let constraint_oid = entry.desc.columns[0].not_null_constraint_oid.unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        let reopened_entry = reopened_catalog.get("people").unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();
        assert_eq!(
            reopened_entry.desc.columns[0].not_null_constraint_oid,
            Some(constraint_oid)
        );
        assert!(reopened_catalog.next_oid() > constraint_oid);
        assert!(rows.constraints.iter().any(|row| {
            row.oid == constraint_oid
                && row.conname == "people_id_not_null"
                && row.contype == 'n'
                && row.conrelid == entry.relation_oid
                && row.connamespace == PUBLIC_NAMESPACE_OID
                && row.convalidated
        }));
    }

    #[test]
    fn physical_catalog_rows_for_entry_use_first_class_constraint_and_depend_rows() {
        let mut catalog = Catalog::default();
        let entry = catalog
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("note", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();
        let constraint_oid = entry.desc.columns[0].not_null_constraint_oid.unwrap();

        let constraint = catalog
            .constraints
            .iter_mut()
            .find(|row| row.oid == constraint_oid)
            .unwrap();
        constraint.conname = "people_id_custom_not_null".into();

        let depend = catalog
            .depends
            .iter_mut()
            .find(|row| row.objid == constraint_oid)
            .unwrap();
        depend.deptype = DEPENDENCY_INTERNAL;

        let rows = physical_catalog_rows_for_catalog_entry(&catalog, "people", &entry);
        assert!(rows.constraints.iter().any(|row| {
            row.oid == constraint_oid && row.conname == "people_id_custom_not_null"
        }));
        assert!(
            rows.constraints
                .iter()
                .all(|row| row.oid != constraint_oid || row.conname != "people_id_not_null")
        );
        assert!(
            rows.depends
                .iter()
                .any(|row| row.objid == constraint_oid && row.deptype == DEPENDENCY_INTERNAL)
        );
    }

    #[test]
    fn catalog_store_persists_pg_proc_rows() {
        let base = temp_dir("proc_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.procs.iter().any(|row| {
            row.proname == "lower"
                && row.pronargs == 1
                && row.prorettype == TEXT_TYPE_OID
                && row.prokind == 'f'
                && row.prosrc == "lower"
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "count"
                && row.pronargs == 1
                && row.prorettype == INT8_TYPE_OID
                && row.prokind == 'a'
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "numeric"
                && row.proargtypes == INT4_TYPE_OID.to_string()
                && row.prorettype == crate::include::catalog::NUMERIC_TYPE_OID
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "biteq"
                && row.proargtypes
                    == format!(
                        "{} {}",
                        crate::include::catalog::BIT_TYPE_OID,
                        crate::include::catalog::BIT_TYPE_OID
                    )
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
                && row.prosrc == "biteq"
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "varbitlt"
                && row.proargtypes
                    == format!(
                        "{} {}",
                        crate::include::catalog::VARBIT_TYPE_OID,
                        crate::include::catalog::VARBIT_TYPE_OID
                    )
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
                && row.prosrc == "varbitlt"
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "byteaeq"
                && row.proargtypes
                    == format!(
                        "{} {}",
                        crate::include::catalog::BYTEA_TYPE_OID,
                        crate::include::catalog::BYTEA_TYPE_OID
                    )
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
                && row.prosrc == "byteaeq"
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "bytealt"
                && row.proargtypes
                    == format!(
                        "{} {}",
                        crate::include::catalog::BYTEA_TYPE_OID,
                        crate::include::catalog::BYTEA_TYPE_OID
                    )
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
                && row.prosrc == "bytealt"
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "jsonb_eq"
                && row.proargtypes
                    == format!(
                        "{} {}",
                        crate::include::catalog::JSONB_TYPE_OID,
                        crate::include::catalog::JSONB_TYPE_OID
                    )
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
                && row.prokind == 'f'
                && row.prosrc == "jsonb_eq"
        }));
        assert!(rows.procs.iter().any(|row| {
            row.proname == "json_array_elements" && row.proretset && row.prorettype == JSON_TYPE_OID
        }));
        assert!(rows.procs.iter().any(|row| {
            row.oid == crate::include::catalog::TEXT_CMP_LT_PROC_OID
                && row.proname == "text_lt"
                && row.proargtypes == format!("{TEXT_TYPE_OID} {TEXT_TYPE_OID}")
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
        }));
        assert!(rows.procs.iter().any(|row| {
            row.oid == crate::include::catalog::TEXT_CMP_GE_PROC_OID
                && row.proname == "text_ge"
                && row.proargtypes == format!("{TEXT_TYPE_OID} {TEXT_TYPE_OID}")
                && row.prorettype == crate::include::catalog::BOOL_TYPE_OID
        }));
    }

    #[test]
    fn catalog_store_persists_pg_collation_rows() {
        let base = temp_dir("collation_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert_eq!(
            rows.collations
                .iter()
                .map(|row| (
                    row.oid,
                    row.collname.as_str(),
                    row.collprovider,
                    row.collowner
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    DEFAULT_COLLATION_OID,
                    "default",
                    'd',
                    BOOTSTRAP_SUPERUSER_OID,
                ),
                (C_COLLATION_OID, "C", 'c', BOOTSTRAP_SUPERUSER_OID),
                (POSIX_COLLATION_OID, "POSIX", 'c', BOOTSTRAP_SUPERUSER_OID),
            ]
        );
    }

    #[test]
    fn catalog_store_persists_pg_cast_rows() {
        let base = temp_dir("cast_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.casts.iter().any(|row| {
            row.castsource == INT4_TYPE_OID
                && row.casttarget == OID_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'i'
                && row.castmethod == 'b'
        }));
        assert!(rows.casts.iter().any(|row| {
            row.castsource == INT4_TYPE_OID
                && row.casttarget == crate::include::catalog::NUMERIC_TYPE_OID
                && row.castfunc != 0
                && row.castcontext == 'i'
                && row.castmethod == 'f'
        }));
        assert!(rows.casts.iter().any(|row| {
            row.castsource == VARCHAR_TYPE_OID
                && row.casttarget == TEXT_TYPE_OID
                && row.castcontext == 'i'
        }));
        assert!(rows.casts.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == crate::include::catalog::JSONB_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.casts.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == crate::include::catalog::JSONPATH_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.casts.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == crate::include::catalog::VARBIT_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.casts.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == crate::include::catalog::INT4_ARRAY_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.casts.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == crate::include::catalog::JSONB_ARRAY_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
    }

    #[test]
    fn catalog_store_persists_pg_database_rows() {
        let base = temp_dir("database_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.databases.iter().any(|row| {
            row.oid == 1
                && row.datname == CURRENT_DATABASE_NAME
                && row.datdba == BOOTSTRAP_SUPERUSER_OID
                && row.dattablespace == DEFAULT_TABLESPACE_OID
                && !row.datistemplate
                && row.datallowconn
        }));
    }

    #[test]
    fn catalog_store_persists_pg_tablespace_rows() {
        let base = temp_dir("tablespace_rows");
        let _store = CatalogStore::load(&base).unwrap();
        let rows = load_physical_catalog_rows(&base).unwrap();

        assert!(rows.tablespaces.iter().any(|row| {
            row.oid == DEFAULT_TABLESPACE_OID
                && row.spcname == "pg_default"
                && row.spcowner == BOOTSTRAP_SUPERUSER_OID
        }));
        assert!(rows.tablespaces.iter().any(|row| {
            row.oid == crate::include::catalog::GLOBAL_TABLESPACE_OID
                && row.spcname == "pg_global"
                && row.spcowner == BOOTSTRAP_SUPERUSER_OID
        }));
    }

    #[test]
    fn catalog_store_drop_table_cascades_indexes() {
        let base = temp_dir("drop_index_cascade");
        let mut store = CatalogStore::load(&base).unwrap();
        let table = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("name", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();
        let index = store
            .create_index("people_name_idx", "people", false, &["name".into()])
            .unwrap();

        let dropped = store.drop_table("people").unwrap();
        assert!(
            dropped
                .iter()
                .any(|entry| entry.relation_oid == index.relation_oid)
        );
        assert!(
            dropped
                .iter()
                .any(|entry| entry.relation_oid == table.relation_oid)
        );
        assert!(dropped.iter().any(|entry| entry.relkind == 't'));
        assert!(dropped.iter().any(|entry| {
            entry.relkind == 'i'
                && entry
                    .index_meta
                    .as_ref()
                    .is_some_and(|meta| meta.indrelid == table.reltoastrelid)
        }));

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("people").is_none());
        assert!(reopened_catalog.get("people_name_idx").is_none());
        let catcache = reopened.catcache().unwrap();
        assert!(!catcache.class_rows().iter().any(|row| row.oid == table.relation_oid));
        assert!(!catcache.class_rows().iter().any(|row| row.oid == index.relation_oid));
        assert!(
            !catcache
                .index_rows()
                .iter()
                .any(|row| row.indexrelid == index.relation_oid)
        );
        assert!(
            !catcache
                .depend_rows()
                .iter()
                .any(|row| row.objid == index.relation_oid)
        );
    }

    #[test]
    fn catalog_store_drop_table_cascades_toast_relations() {
        let base = temp_dir("drop_toast_cascade");
        let mut store = CatalogStore::load(&base).unwrap();
        let table = store
            .create_table(
                "docs",
                RelationDesc {
                    columns: vec![
                        column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                        column_desc("payload", SqlType::new(SqlTypeKind::Text), true),
                    ],
                },
            )
            .unwrap();

        let dropped = store.drop_table("docs").unwrap();
        assert!(
            dropped
                .iter()
                .any(|entry| entry.relation_oid == table.relation_oid)
        );
        assert!(
            dropped
                .iter()
                .any(|entry| entry.relation_oid == table.reltoastrelid && entry.relkind == 't')
        );
        assert!(dropped.iter().any(|entry| {
            entry.relkind == 'i'
                && entry
                    .index_meta
                    .as_ref()
                    .is_some_and(|meta| meta.indrelid == table.reltoastrelid)
        }));
    }

    #[test]
    fn catalog_store_drop_table_removes_constraint_and_depend_rows() {
        let base = temp_dir("drop_constraint_depend_cleanup");
        let mut store = CatalogStore::load(&base).unwrap();
        let mut desc = RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("note", SqlType::new(SqlTypeKind::Text), true),
            ],
        };
        desc.columns[1].default_expr = Some("'hello'".into());
        let entry = store.create_table("notes", desc).unwrap();
        let attrdef_oid = entry.desc.columns[1].attrdef_oid.unwrap();
        let constraint_oid = entry.desc.columns[0].not_null_constraint_oid.unwrap();

        let dropped = store.drop_table("notes").unwrap();
        assert!(
            dropped
                .iter()
                .any(|dropped| dropped.relation_oid == entry.relation_oid)
        );
        assert!(dropped.iter().any(|dropped| dropped.relkind == 't'));

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        assert!(reopened_catalog.get("notes").is_none());
        assert!(
            reopened_catalog
                .constraint_rows()
                .iter()
                .all(|row| row.conrelid != entry.relation_oid)
        );
        assert!(reopened_catalog.depend_rows().iter().all(|row| {
            row.objid != entry.relation_oid
                && row.refobjid != entry.relation_oid
                && row.objid != attrdef_oid
                && row.objid != constraint_oid
        }));

        let catcache = reopened.catcache().unwrap();
        assert!(
            catcache
                .constraint_rows()
                .iter()
                .all(|row| row.conrelid != entry.relation_oid)
        );
        assert!(catcache.depend_rows().iter().all(|row| {
            row.objid != entry.relation_oid
                && row.refobjid != entry.relation_oid
                && row.objid != attrdef_oid
                && row.objid != constraint_oid
        }));
    }

    #[cfg(unix)]
    #[test]
    fn catalog_store_create_table_appends_to_touched_catalog_relations() {
        let base = temp_dir("selective_catalog_sync_create_table");
        let mut store = CatalogStore::load(&base).unwrap();
        let proc_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgProc.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        let class_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgClass.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        let class_index_path = system_index_path(&base, 1, "pg_class_relname_nsp_index");
        let proc_meta_before = fs::metadata(&proc_path).unwrap();
        let class_meta_before = fs::metadata(&class_path).unwrap();
        let class_index_meta_before = fs::metadata(&class_index_path).unwrap();

        store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let proc_meta_after = fs::metadata(&proc_path).unwrap();
        let class_meta_after = fs::metadata(&class_path).unwrap();
        let class_index_meta_after = fs::metadata(&class_index_path).unwrap();
        assert_eq!(proc_meta_before.ino(), proc_meta_after.ino());
        assert_eq!(
            proc_meta_before.modified().unwrap(),
            proc_meta_after.modified().unwrap()
        );
        assert_eq!(class_meta_before.ino(), class_meta_after.ino());
        assert_eq!(class_index_meta_before.ino(), class_index_meta_after.ino());
    }

    #[cfg(unix)]
    #[test]
    fn catalog_store_create_index_appends_to_touched_catalog_relations() {
        let base = temp_dir("selective_catalog_sync_create_index");
        let mut store = CatalogStore::load(&base).unwrap();
        store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();
        let proc_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgProc.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        let class_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgClass.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        let index_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgIndex.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        let class_index_path = system_index_path(&base, 1, "pg_class_relname_nsp_index");
        let proc_meta_before = fs::metadata(&proc_path).unwrap();
        let class_meta_before = fs::metadata(&class_path).unwrap();
        let index_meta_before = fs::metadata(&index_path).unwrap();
        let class_index_meta_before = fs::metadata(&class_index_path).unwrap();

        store
            .create_index("people_id_idx", "people", false, &["id".into()])
            .unwrap();

        let proc_meta_after = fs::metadata(&proc_path).unwrap();
        let class_meta_after = fs::metadata(&class_path).unwrap();
        let index_meta_after = fs::metadata(&index_path).unwrap();
        let class_index_meta_after = fs::metadata(&class_index_path).unwrap();
        assert_eq!(proc_meta_before.ino(), proc_meta_after.ino());
        assert_eq!(
            proc_meta_before.modified().unwrap(),
            proc_meta_after.modified().unwrap()
        );
        assert_eq!(class_meta_before.ino(), class_meta_after.ino());
        assert_eq!(index_meta_before.ino(), index_meta_after.ino());
        assert_eq!(class_index_meta_before.ino(), class_index_meta_after.ino());
    }

    #[cfg(unix)]
    #[test]
    fn catalog_store_drop_table_updates_catalog_indexes_in_place() {
        let base = temp_dir("selective_catalog_sync_drop_table");
        let mut store = CatalogStore::load(&base).unwrap();
        store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();
        let class_index_path = system_index_path(&base, 1, "pg_class_relname_nsp_index");
        let type_index_path = system_index_path(&base, 1, "pg_type_typname_nsp_index");
        let class_index_meta_before = fs::metadata(&class_index_path).unwrap();
        let type_index_meta_before = fs::metadata(&type_index_path).unwrap();

        store.drop_table("people").unwrap();

        let class_index_meta_after = fs::metadata(&class_index_path).unwrap();
        let type_index_meta_after = fs::metadata(&type_index_path).unwrap();
        assert_eq!(class_index_meta_before.ino(), class_index_meta_after.ino());
        assert_eq!(type_index_meta_before.ino(), type_index_meta_after.ino());
    }

    #[cfg(unix)]
    #[test]
    fn catalog_store_drop_table_requires_manual_vacuum_for_dead_system_index_tuples() {
        let base = temp_dir("catalog_index_tuple_cleanup");
        let mut store = CatalogStore::load(&base).unwrap();
        let class_index_rel = system_index_rel(1, "pg_class_relname_nsp_index");
        let before = count_leaf_btree_items(&base, class_index_rel);

        store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();
        let after_create = count_leaf_btree_items(&base, class_index_rel);
        assert_eq!(after_create, before + 1);

        store.drop_table("people").unwrap();

        let after_drop = count_leaf_btree_items(&base, class_index_rel);
        assert_eq!(after_drop, before + 1);

        vacuum_relation_via_command(&base, "pg_catalog.pg_class", None);

        let after_vacuum = count_leaf_btree_items(&base, class_index_rel);
        assert_eq!(after_vacuum, before);
    }

    #[cfg(unix)]
    #[test]
    fn catalog_store_rename_table_manual_vacuum_cleans_pg_class_relname_index() {
        let base = temp_dir("catalog_index_tuple_cleanup_rename");
        let mut store = CatalogStore::load(&base).unwrap();
        let created = store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();
        let class_index_rel = system_index_rel(1, "pg_class_relname_nsp_index");
        let before = count_leaf_btree_items(&base, class_index_rel);

        let (_pool, txns, ctx) = durable_write_context(&base);
        let _effect = store
            .rename_relation_mvcc(created.relation_oid, "customers", &ctx)
            .unwrap();
        txns.write().commit(ctx.xid).unwrap();

        let after_rename = count_leaf_btree_items(&base, class_index_rel);
        assert!(after_rename > before);

        vacuum_relation_via_command(
            &base,
            "pg_catalog.pg_class",
            Some(Arc::clone(&txns)),
        );

        let after_vacuum = count_leaf_btree_items(&base, class_index_rel);
        assert_eq!(after_vacuum, before);
    }

    #[cfg(unix)]
    #[test]
    fn catalog_store_rename_role_requires_manual_vacuum_for_dead_shared_system_index_tuples() {
        let base = temp_dir("catalog_index_tuple_cleanup_role_rename");
        let mut store = CatalogStore::load(&base).unwrap();
        let role_index_rel = system_index_rel(1, "pg_authid_rolname_index");
        let before = count_leaf_btree_items(&base, role_index_rel);

        let _created = store
            .create_role_direct(
                "app_user",
                &crate::backend::catalog::roles::RoleAttributes {
                    rolcanlogin: true,
                    ..crate::backend::catalog::roles::RoleAttributes::default()
                },
            )
            .unwrap();
        let after_create = count_leaf_btree_items(&base, role_index_rel);
        assert_eq!(after_create, before + 1);

        store
            .rename_role_direct("app_user", "customer_user")
            .unwrap();

        let after_rename = count_leaf_btree_items(&base, role_index_rel);
        assert!(after_rename > after_create);

        vacuum_relation_via_command(&base, "pg_catalog.pg_authid", None);

        let after_vacuum = count_leaf_btree_items(&base, role_index_rel);
        assert_eq!(after_vacuum, after_create);
    }

    #[cfg(unix)]
    #[test]
    fn catalog_store_load_reuses_existing_catalog_relfiles() {
        let base = temp_dir("load_existing_catalog_relfiles");
        let _ = CatalogStore::load(&base).unwrap();

        let proc_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgProc.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        let class_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgClass.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        let index_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgIndex.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );

        let proc_meta_before = fs::metadata(&proc_path).unwrap();
        let class_meta_before = fs::metadata(&class_path).unwrap();
        let index_meta_before = fs::metadata(&index_path).unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        assert!(
            reopened
                .catalog_snapshot()
                .unwrap()
                .get("pg_class")
                .is_some()
        );

        let proc_meta_after = fs::metadata(&proc_path).unwrap();
        let class_meta_after = fs::metadata(&class_path).unwrap();
        let index_meta_after = fs::metadata(&index_path).unwrap();
        assert_eq!(proc_meta_before.ino(), proc_meta_after.ino());
        assert_eq!(class_meta_before.ino(), class_meta_after.ino());
        assert_eq!(index_meta_before.ino(), index_meta_after.ino());
        assert_eq!(
            proc_meta_before.modified().unwrap(),
            proc_meta_after.modified().unwrap()
        );
        assert_eq!(
            class_meta_before.modified().unwrap(),
            class_meta_after.modified().unwrap()
        );
        assert_eq!(
            index_meta_before.modified().unwrap(),
            index_meta_after.modified().unwrap()
        );
    }

    #[test]
    fn catalog_store_bootstraps_physical_core_catalog_relfiles() {
        let base = temp_dir("physical_bootstrap");
        let store = CatalogStore::load(&base).unwrap();
        let catalog = store.catalog_snapshot().unwrap();
        for name in ["pg_namespace", "pg_type", "pg_attribute", "pg_class"] {
            let entry = catalog.get(name).unwrap();
            let path = segment_path(&base, entry.rel, ForkNumber::Main, 0);
            let meta = fs::metadata(path).unwrap();
            assert!(meta.len() > 0, "{name} should have heap data");
        }

        let attrdef = catalog.get("pg_attrdef").unwrap();
        let attrdef_path = segment_path(&base, attrdef.rel, ForkNumber::Main, 0);
        assert!(attrdef_path.exists(), "pg_attrdef relfile should exist");
        let depend = catalog.get("pg_depend").unwrap();
        let depend_path = segment_path(&base, depend.rel, ForkNumber::Main, 0);
        assert!(depend_path.exists(), "pg_depend relfile should exist");
        let index = catalog.get("pg_index").unwrap();
        let index_path = segment_path(&base, index.rel, ForkNumber::Main, 0);
        assert!(index_path.exists(), "pg_index relfile should exist");
        let database = catalog.get("pg_database").unwrap();
        let database_path = segment_path(&base, database.rel, ForkNumber::Main, 0);
        assert!(database_path.exists(), "pg_database relfile should exist");
        let authid = catalog.get("pg_authid").unwrap();
        let authid_path = segment_path(&base, authid.rel, ForkNumber::Main, 0);
        assert!(authid_path.exists(), "pg_authid relfile should exist");
        let auth_members = catalog.get("pg_auth_members").unwrap();
        let auth_members_path = segment_path(&base, auth_members.rel, ForkNumber::Main, 0);
        assert!(
            auth_members_path.exists(),
            "pg_auth_members relfile should exist"
        );
        let collation = catalog.get("pg_collation").unwrap();
        let collation_path = segment_path(&base, collation.rel, ForkNumber::Main, 0);
        assert!(collation_path.exists(), "pg_collation relfile should exist");
        let language = catalog.get("pg_language").unwrap();
        let language_path = segment_path(&base, language.rel, ForkNumber::Main, 0);
        assert!(language_path.exists(), "pg_language relfile should exist");
        let operator = catalog.get("pg_operator").unwrap();
        let operator_path = segment_path(&base, operator.rel, ForkNumber::Main, 0);
        assert!(operator_path.exists(), "pg_operator relfile should exist");
        let proc = catalog.get("pg_proc").unwrap();
        let proc_path = segment_path(&base, proc.rel, ForkNumber::Main, 0);
        assert!(proc_path.exists(), "pg_proc relfile should exist");
        let cast = catalog.get("pg_cast").unwrap();
        let cast_path = segment_path(&base, cast.rel, ForkNumber::Main, 0);
        assert!(cast_path.exists(), "pg_cast relfile should exist");
        let constraint = catalog.get("pg_constraint").unwrap();
        let constraint_path = segment_path(&base, constraint.rel, ForkNumber::Main, 0);
        assert!(
            constraint_path.exists(),
            "pg_constraint relfile should exist"
        );
        let am = catalog.get("pg_am").unwrap();
        let am_path = segment_path(&base, am.rel, ForkNumber::Main, 0);
        assert!(am_path.exists(), "pg_am relfile should exist");
        let tablespace = catalog.get("pg_tablespace").unwrap();
        let tablespace_path = segment_path(&base, tablespace.rel, ForkNumber::Main, 0);
        assert!(
            tablespace_path.exists(),
            "pg_tablespace relfile should exist"
        );
    }

    #[test]
    fn catalog_store_loads_from_physical_catalogs_without_schema_file() {
        let base = temp_dir("physical_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        store
            .create_table(
                "records",
                RelationDesc {
                    columns: vec![column_desc(
                        "tags",
                        SqlType::array_of(SqlType::new(SqlTypeKind::Varchar)),
                        true,
                    )],
                },
            )
            .unwrap();
        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        let entry = reopened_catalog.get("records").unwrap();
        assert_eq!(
            entry.desc.columns[0].sql_type,
            SqlType::array_of(SqlType::new(SqlTypeKind::Varchar))
        );
    }

    #[test]
    fn catalog_store_roundtrips_zero_column_tables() {
        let base = temp_dir("zero_columns");
        let mut store = CatalogStore::load(&base).unwrap();
        store
            .create_table(
                "zerocol",
                RelationDesc {
                    columns: Vec::new(),
                },
            )
            .unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let reopened_catalog = reopened.catalog_snapshot().unwrap();
        let entry = reopened_catalog.get("zerocol").unwrap();
        assert!(entry.desc.columns.is_empty());
    }

    #[test]
    fn catalog_store_preserves_relation_allocators_across_drop_and_reload() {
        let base = temp_dir("allocator_reload");
        let mut store = CatalogStore::load(&base).unwrap();
        let first = store
            .create_table(
                "first",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();
        store.drop_table("first").unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let mut reopened = reopened;
        let second = reopened
            .create_table(
                "second",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        assert!(second.rel.rel_number > first.rel.rel_number);
        assert!(second.relation_oid > first.relation_oid);
        assert!(second.row_type_oid > first.row_type_oid);
    }

    #[test]
    fn catalog_store_migrates_legacy_defaults_json_into_pg_attrdef() {
        let base = temp_dir("legacy_defaults_migration");
        let mut store = CatalogStore::load(&base).unwrap();
        let mut desc = RelationDesc {
            columns: vec![
                column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                column_desc("note", SqlType::new(SqlTypeKind::Text), true),
            ],
        };
        desc.columns[1].default_expr = Some("'legacy'".into());
        let entry = store.create_table("notes", desc).unwrap();

        let attrdef_path = segment_path(
            &base,
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: BootstrapCatalogKind::PgAttrdef.relation_oid(),
            },
            ForkNumber::Main,
            0,
        );
        fs::remove_file(&attrdef_path).unwrap();
        let legacy_dir = base.join("catalog");
        fs::create_dir_all(&legacy_dir).unwrap();
        fs::write(
            legacy_dir.join("defaults.json"),
            format!(
                r#"[{{"relation_oid":{},"attnum":2,"expr":"'legacy'"}}]"#,
                entry.relation_oid
            ),
        )
        .unwrap();

        let reopened = CatalogStore::load(&base).unwrap();
        let relcache = reopened.relcache().unwrap();
        let migrated = relcache.get_by_name("notes").unwrap();
        assert_eq!(
            migrated.desc.columns[1].default_expr.as_deref(),
            Some("'legacy'")
        );
        assert!(migrated.desc.columns[1].attrdef_oid.is_some());

        let rows = load_physical_catalog_rows(&base).unwrap();
        let attrdef = rows
            .attrdefs
            .iter()
            .find(|row| row.adrelid == entry.relation_oid && row.adnum == 2)
            .unwrap();
        assert_eq!(attrdef.adbin, "'legacy'");
        assert!(attrdef.oid > entry.row_type_oid);
    }

    fn assert_missing_bootstrap_relfile_fails(label: &str, kind: BootstrapCatalogKind) {
        let base = temp_dir(label);
        let mut store = CatalogStore::load(&base).unwrap();
        store
            .create_table(
                "people",
                RelationDesc {
                    columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
                },
            )
            .unwrap();

        let path = segment_path(&base, bootstrap_catalog_rel(kind, 1), ForkNumber::Main, 0);
        fs::remove_file(&path).unwrap();

        let err = CatalogStore::load(&base).unwrap_err();
        assert!(matches!(
            err,
            CatalogError::Corrupt("missing physical relation relfile")
        ));
    }

    #[test]
    fn catalog_store_load_fails_when_local_bootstrap_relfile_is_missing() {
        for (label, kind) in [
            ("missing_depend_reload", BootstrapCatalogKind::PgDepend),
            ("missing_index_reload", BootstrapCatalogKind::PgIndex),
            ("missing_am_reload", BootstrapCatalogKind::PgAm),
            (
                "missing_collation_reload",
                BootstrapCatalogKind::PgCollation,
            ),
            ("missing_cast_reload", BootstrapCatalogKind::PgCast),
            ("missing_proc_reload", BootstrapCatalogKind::PgProc),
            ("missing_language_reload", BootstrapCatalogKind::PgLanguage),
            ("missing_operator_reload", BootstrapCatalogKind::PgOperator),
            (
                "missing_constraint_reload",
                BootstrapCatalogKind::PgConstraint,
            ),
        ] {
            assert_missing_bootstrap_relfile_fails(label, kind);
        }
    }

    #[test]
    fn catalog_store_load_fails_when_shared_bootstrap_relfile_is_missing() {
        for (label, kind) in [
            ("missing_database_reload", BootstrapCatalogKind::PgDatabase),
            ("missing_authid_reload", BootstrapCatalogKind::PgAuthId),
            (
                "missing_auth_members_reload",
                BootstrapCatalogKind::PgAuthMembers,
            ),
            (
                "missing_tablespace_reload",
                BootstrapCatalogKind::PgTablespace,
            ),
        ] {
            assert_missing_bootstrap_relfile_fails(label, kind);
        }
    }
}
