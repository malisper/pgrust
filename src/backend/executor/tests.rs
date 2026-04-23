use super::tests_support::SeededSqlHarness;
use super::*;
use crate::RelFileLocator;
use crate::backend::access::heap::heapam::{heap_flush, heap_insert_mvcc, heap_update};
use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
use crate::backend::libpq::pqformat::format_exec_error;
use crate::backend::parser::{Catalog, CatalogEntry, CatalogLookup, IndexColumnDef};
use crate::backend::storage::smgr::{ForkNumber, MdStorageManager, StorageManager};
use crate::include::access::htup::TupleValue;
use crate::include::access::htup::{AttributeDesc, HeapTuple};
use crate::include::catalog::{CONSTRAINT_PRIMARY, CONSTRAINT_UNIQUE};
use crate::include::nodes::datetime::{DateADT, TimestampADT};
use crate::include::nodes::primnodes::{Var, user_attrno};
use crate::pgrust::database::{Database, Session};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

fn local_var(index: usize) -> Expr {
    Expr::Var(Var {
        varno: 1,
        varattno: user_attrno(index),
        varlevelsup: 0,
        vartype: crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
    })
}

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

fn temp_dir(label: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "pgrust_executor_{}_{}_{}",
        label,
        std::process::id(),
        NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&p);
    fs::create_dir_all(&p).unwrap();
    p
}

fn run_with_large_stack<F>(name: &str, f: F)
where
    F: FnOnce() + Send + 'static,
{
    thread::Builder::new()
        .name(name.into())
        .stack_size(32 * 1024 * 1024)
        .spawn(f)
        .unwrap()
        .join()
        .unwrap();
}

fn run_with_large_stack_result<F, T>(name: &str, f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    thread::Builder::new()
        .name(name.into())
        .stack_size(32 * 1024 * 1024)
        .spawn(f)
        .unwrap()
        .join()
        .unwrap()
}

fn rel() -> RelFileLocator {
    RelFileLocator {
        spc_oid: 0,
        db_oid: 1,
        rel_number: 14000,
    }
}

fn pets_rel() -> RelFileLocator {
    RelFileLocator {
        spc_oid: 0,
        db_oid: 1,
        rel_number: 14001,
    }
}

fn t1_rel() -> RelFileLocator {
    RelFileLocator {
        spc_oid: 0,
        db_oid: 1,
        rel_number: 14020,
    }
}

fn t2_rel() -> RelFileLocator {
    RelFileLocator {
        spc_oid: 0,
        db_oid: 1,
        rel_number: 14021,
    }
}

fn t3_rel() -> RelFileLocator {
    RelFileLocator {
        spc_oid: 0,
        db_oid: 1,
        rel_number: 14022,
    }
}

fn relation_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            crate::backend::catalog::catalog::column_desc(
                "id",
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                false,
            ),
            crate::backend::catalog::catalog::column_desc(
                "name",
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Text),
                false,
            ),
            crate::backend::catalog::catalog::column_desc(
                "note",
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Text),
                true,
            ),
        ],
    }
}

fn test_catalog_entry(rel: RelFileLocator, desc: RelationDesc) -> CatalogEntry {
    CatalogEntry {
        rel,
        relation_oid: 50_000u32.saturating_add(rel.rel_number),
        namespace_oid: crate::include::catalog::PUBLIC_NAMESPACE_OID,
        owner_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
        relacl: None,
        row_type_oid: 60_000u32.saturating_add(rel.rel_number),
        array_type_oid: 61_000u32.saturating_add(rel.rel_number),
        reltoastrelid: 0,
        relpersistence: 'p',
        relkind: 'r',
        am_oid: crate::include::catalog::relam_for_relkind('r'),
        relhastriggers: false,
        relhassubclass: false,
        relispartition: false,
        relpartbound: None,
        relrowsecurity: false,
        relforcerowsecurity: false,
        relpages: 0,
        reltuples: 0.0,
        relallvisible: 0,
        relallfrozen: 0,
        relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
        desc,
        partitioned_table: None,
        index_meta: None,
    }
}

fn catalog() -> Catalog {
    let mut catalog = Catalog::default();
    catalog.insert("people", test_catalog_entry(rel(), relation_desc()));
    catalog
}

fn add_ready_people_index(
    catalog: &mut Catalog,
    index_name: &str,
    unique: bool,
    primary: bool,
    columns: &[IndexColumnDef],
    constraint: Option<(char, &str)>,
) {
    let relation_oid = catalog.lookup_any_relation("people").unwrap().relation_oid;
    let entry = catalog
        .create_index_for_relation_with_flags(index_name, relation_oid, unique, primary, columns)
        .unwrap();
    catalog
        .set_index_ready_valid(entry.relation_oid, true, true)
        .unwrap();
    if let Some((contype, conname)) = constraint {
        catalog
            .create_index_backed_constraint(relation_oid, entry.relation_oid, conname, contype, &[])
            .unwrap();
    }
}

fn catalog_with_people_primary_key() -> Catalog {
    let mut catalog = catalog();
    add_ready_people_index(
        &mut catalog,
        "people_pkey",
        true,
        true,
        &[IndexColumnDef::from("id")],
        Some((CONSTRAINT_PRIMARY, "people_pkey")),
    );
    catalog
}

fn catalog_with_people_note_unique_index() -> Catalog {
    let mut catalog = catalog();
    add_ready_people_index(
        &mut catalog,
        "people_note_key",
        true,
        false,
        &[IndexColumnDef::from("note")],
        Some((CONSTRAINT_UNIQUE, "people_note_key")),
    );
    catalog
}

fn pets_relation_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            crate::backend::catalog::catalog::column_desc(
                "id",
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                false,
            ),
            crate::backend::catalog::catalog::column_desc(
                "name",
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Text),
                false,
            ),
            crate::backend::catalog::catalog::column_desc(
                "owner_id",
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                true,
            ),
        ],
    }
}

fn join_name_n_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            crate::backend::catalog::catalog::column_desc(
                "name",
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Text),
                false,
            ),
            crate::backend::catalog::catalog::column_desc(
                "n",
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                false,
            ),
        ],
    }
}

fn catalog_with_pets() -> Catalog {
    let mut catalog = catalog();
    catalog.insert("pets", test_catalog_entry(pets_rel(), pets_relation_desc()));
    catalog
}

fn join_chain_catalog() -> Catalog {
    let mut catalog = Catalog::default();
    catalog.insert("t1", test_catalog_entry(t1_rel(), join_name_n_desc()));
    catalog.insert("t2", test_catalog_entry(t2_rel(), join_name_n_desc()));
    catalog.insert("t3", test_catalog_entry(t3_rel(), join_name_n_desc()));
    catalog
}

fn people_scan_plan() -> Plan {
    Plan::SeqScan {
        plan_info: PlanEstimate::default(),
        source_id: 1,
        rel: rel(),
        relation_name: "people".into(),
        relation_oid: 0,
        relkind: 'r',
        toast: None,
        desc: relation_desc(),
    }
}

fn pets_scan_plan() -> Plan {
    Plan::SeqScan {
        plan_info: PlanEstimate::default(),
        source_id: 2,
        rel: pets_rel(),
        relation_name: "pets".into(),
        relation_oid: 0,
        relkind: 'r',
        toast: None,
        desc: pets_relation_desc(),
    }
}

fn people_pets_hash_join_plan(kind: JoinType, join_qual: Vec<Expr>, qual: Vec<Expr>) -> Plan {
    Plan::HashJoin {
        plan_info: PlanEstimate::default(),
        left: Box::new(people_scan_plan()),
        right: Box::new(Plan::Hash {
            plan_info: PlanEstimate::default(),
            input: Box::new(pets_scan_plan()),
            hash_keys: vec![local_var(2)],
        }),
        kind,
        hash_clauses: vec![Expr::op_auto(
            crate::include::nodes::primnodes::OpExprKind::Eq,
            vec![local_var(0), local_var(5)],
        )],
        hash_keys: vec![local_var(0)],
        join_qual,
        qual,
    }
}

fn multidimensional_array_catalog() -> Catalog {
    let mut catalog = Catalog::default();
    catalog.insert(
        "t",
        test_catalog_entry(
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 14002,
            },
            RelationDesc {
                columns: vec![crate::backend::catalog::catalog::column_desc(
                    "a",
                    crate::backend::parser::SqlType::array_of(
                        crate::backend::parser::SqlType::new(
                            crate::backend::parser::SqlTypeKind::Int4,
                        ),
                    ),
                    true,
                )],
            },
        ),
    );
    catalog
}

fn array_subscript_catalog() -> Catalog {
    let mut catalog = Catalog::default();
    catalog.insert(
        "t",
        test_catalog_entry(
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 14003,
            },
            RelationDesc {
                columns: vec![
                    crate::backend::catalog::catalog::column_desc(
                        "a",
                        crate::backend::parser::SqlType::array_of(
                            crate::backend::parser::SqlType::new(
                                crate::backend::parser::SqlTypeKind::Int4,
                            ),
                        ),
                        true,
                    ),
                    crate::backend::catalog::catalog::column_desc(
                        "b",
                        crate::backend::parser::SqlType::array_of(
                            crate::backend::parser::SqlType::new(
                                crate::backend::parser::SqlTypeKind::Int4,
                            ),
                        ),
                        true,
                    ),
                ],
            },
        ),
    );
    catalog
}

fn array_assignment_catalog() -> Catalog {
    let mut catalog = Catalog::default();
    catalog.insert(
        "t",
        test_catalog_entry(
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 14004,
            },
            RelationDesc {
                columns: vec![
                    crate::backend::catalog::catalog::column_desc(
                        "a",
                        crate::backend::parser::SqlType::array_of(
                            crate::backend::parser::SqlType::new(
                                crate::backend::parser::SqlTypeKind::Int4,
                            ),
                        ),
                        true,
                    ),
                    crate::backend::catalog::catalog::column_desc(
                        "f",
                        crate::backend::parser::SqlType::array_of(
                            crate::backend::parser::SqlType::with_char_len(
                                crate::backend::parser::SqlTypeKind::Char,
                                5,
                            ),
                        ),
                        true,
                    ),
                ],
            },
        ),
    );
    catalog
}

fn varchar_catalog(name: &str, len: i32) -> Catalog {
    let mut catalog = Catalog::default();
    catalog.insert(
        name,
        test_catalog_entry(
            crate::RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 15002,
            },
            RelationDesc {
                columns: vec![crate::backend::catalog::catalog::column_desc(
                    "name",
                    crate::backend::parser::SqlType::with_char_len(
                        crate::backend::parser::SqlTypeKind::Varchar,
                        len,
                    ),
                    false,
                )],
            },
        ),
    );
    catalog
}

fn char_catalog(name: &str, len: i32) -> Catalog {
    let mut catalog = Catalog::default();
    catalog.insert(
        name,
        test_catalog_entry(
            crate::RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 15003,
            },
            RelationDesc {
                columns: vec![crate::backend::catalog::catalog::column_desc(
                    "name",
                    crate::backend::parser::SqlType::with_char_len(
                        crate::backend::parser::SqlTypeKind::Char,
                        len,
                    ),
                    false,
                )],
            },
        ),
    );
    catalog
}

fn numeric_catalog(name: &str) -> Catalog {
    let mut catalog = Catalog::default();
    catalog.insert(
        name,
        test_catalog_entry(
            crate::RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 15004,
            },
            RelationDesc {
                columns: vec![crate::backend::catalog::catalog::column_desc(
                    "value",
                    crate::backend::parser::SqlType::new(
                        crate::backend::parser::SqlTypeKind::Numeric,
                    ),
                    false,
                )],
            },
        ),
    );
    catalog
}

fn range_catalog(name: &str, ty: crate::backend::parser::SqlTypeKind) -> Catalog {
    let mut catalog = Catalog::default();
    catalog.insert(
        name,
        test_catalog_entry(
            crate::RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 15006,
            },
            RelationDesc {
                columns: vec![
                    crate::backend::catalog::catalog::column_desc(
                        "id",
                        crate::backend::parser::SqlType::new(
                            crate::backend::parser::SqlTypeKind::Int4,
                        ),
                        false,
                    ),
                    crate::backend::catalog::catalog::column_desc(
                        "span",
                        crate::backend::parser::SqlType::new(ty),
                        true,
                    ),
                ],
            },
        ),
    );
    catalog
}

fn oid_catalog(name: &str) -> Catalog {
    let mut catalog = Catalog::default();
    catalog.insert(
        name,
        test_catalog_entry(
            crate::RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 15005,
            },
            RelationDesc {
                columns: vec![crate::backend::catalog::catalog::column_desc(
                    "f1",
                    crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Oid),
                    false,
                )],
            },
        ),
    );
    catalog
}

fn tuple(id: i32, name: &str, note: Option<&str>) -> HeapTuple {
    let desc = relation_desc().attribute_descs();
    HeapTuple::from_values(
        &desc,
        &[
            TupleValue::Bytes(id.to_le_bytes().to_vec()),
            TupleValue::Bytes(name.as_bytes().to_vec()),
            match note {
                Some(note) => TupleValue::Bytes(note.as_bytes().to_vec()),
                None => TupleValue::Null,
            },
        ],
    )
    .unwrap()
}

fn pet_tuple(id: i32, name: &str, owner_id: i32) -> HeapTuple {
    let desc = pets_relation_desc().attribute_descs();
    HeapTuple::from_values(
        &desc,
        &[
            TupleValue::Bytes(id.to_le_bytes().to_vec()),
            TupleValue::Bytes(name.as_bytes().to_vec()),
            TupleValue::Bytes(owner_id.to_le_bytes().to_vec()),
        ],
    )
    .unwrap()
}

/// Test-only: create the storage fork for a relation.
fn create_fork(pool: &BufferPool<SmgrStorageBackend>, rel: RelFileLocator) {
    pool.with_storage_mut(|s| {
        s.smgr.open(rel).unwrap();
        match s.smgr.create(rel, ForkNumber::Main, false) {
            Ok(()) => {}
            Err(crate::backend::storage::smgr::SmgrError::AlreadyExists { .. }) => {}
            Err(e) => panic!("create_fork failed: {e:?}"),
        }
    });
}

/// Test-only: create a buffer pool with the "people" table fork ready.
fn test_pool(base: &PathBuf) -> std::sync::Arc<BufferPool<SmgrStorageBackend>> {
    let smgr = MdStorageManager::new(base);
    let pool = std::sync::Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 8));
    create_fork(&*pool, rel());
    pool
}

/// Test-only: create a buffer pool with both "people" and "pets" forks ready.
fn test_pool_with_pets(base: &PathBuf) -> std::sync::Arc<BufferPool<SmgrStorageBackend>> {
    let pool = test_pool(base);
    create_fork(&*pool, pets_rel());
    pool
}

fn empty_executor_context(base: &PathBuf) -> ExecutorContext {
    let txns = TransactionManager::new_durable(base).unwrap();
    let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
    ExecutorContext {
        pool: test_pool(base),
        txns: std::sync::Arc::new(parking_lot::RwLock::new(txns)),
        txn_waiter: None,
        sequences: Some(std::sync::Arc::new(
            crate::pgrust::database::SequenceRuntime::new_ephemeral(),
        )),
        large_objects: Some(std::sync::Arc::new(
            crate::pgrust::database::LargeObjectRuntime::new_ephemeral(),
        )),
        async_notify_runtime: None,
        advisory_locks: std::sync::Arc::new(
            crate::backend::storage::lmgr::AdvisoryLockManager::new(),
        ),
        row_locks: std::sync::Arc::new(crate::backend::storage::lmgr::RowLockManager::new()),
        checkpoint_stats: crate::backend::utils::misc::checkpoint::CheckpointStatsSnapshot::default(
        ),
        datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        interrupts: std::sync::Arc::new(
            crate::backend::utils::misc::interrupts::InterruptState::new(),
        ),
        stats: std::sync::Arc::new(parking_lot::RwLock::new(
            crate::pgrust::database::DatabaseStatsStore::with_default_io_rows(),
        )),
        session_stats: std::sync::Arc::new(parking_lot::RwLock::new(
            crate::pgrust::database::SessionStatsState::default(),
        )),
        snapshot,
        transaction_state: None,
        client_id: 1,
        current_database_name: "postgres".to_string(),
        session_user_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
        current_user_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
        active_role_oid: None,
        session_replication_role: Default::default(),
        statement_lock_scope_id: None,
        transaction_lock_scope_id: None,
        next_command_id: 0,
        default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
        expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
        case_test_values: Vec::new(),
        system_bindings: Vec::new(),
        subplans: Vec::new(),
        timed: false,
        allow_side_effects: true,
        pending_async_notifications: Vec::new(),
        catalog: None,
        compiled_functions: std::collections::HashMap::new(),
        cte_tables: std::collections::HashMap::new(),
        cte_producers: std::collections::HashMap::new(),
        recursive_worktables: std::collections::HashMap::new(),
        deferred_foreign_keys: None,
        trigger_depth: 0,
    }
}

fn run_plan(
    base: &PathBuf,
    txns: &TransactionManager,
    plan: Plan,
) -> Result<Vec<(Vec<String>, Vec<Value>)>, ExecError> {
    let pool = test_pool(base);
    let txns_arc = std::sync::Arc::new(parking_lot::RwLock::new(txns.clone()));
    let mut state = executor_start(plan);
    let mut ctx = ExecutorContext {
        pool,
        txns: txns_arc,
        txn_waiter: None,
        sequences: Some(std::sync::Arc::new(
            crate::pgrust::database::SequenceRuntime::new_ephemeral(),
        )),
        large_objects: Some(std::sync::Arc::new(
            crate::pgrust::database::LargeObjectRuntime::new_ephemeral(),
        )),
        async_notify_runtime: None,
        advisory_locks: std::sync::Arc::new(
            crate::backend::storage::lmgr::AdvisoryLockManager::new(),
        ),
        row_locks: std::sync::Arc::new(crate::backend::storage::lmgr::RowLockManager::new()),
        checkpoint_stats: crate::backend::utils::misc::checkpoint::CheckpointStatsSnapshot::default(
        ),
        datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        interrupts: std::sync::Arc::new(
            crate::backend::utils::misc::interrupts::InterruptState::new(),
        ),
        stats: std::sync::Arc::new(parking_lot::RwLock::new(
            crate::pgrust::database::DatabaseStatsStore::with_default_io_rows(),
        )),
        session_stats: std::sync::Arc::new(parking_lot::RwLock::new(
            crate::pgrust::database::SessionStatsState::default(),
        )),
        snapshot: txns.snapshot(INVALID_TRANSACTION_ID).unwrap(),
        transaction_state: None,
        client_id: 42,
        current_database_name: "postgres".to_string(),
        session_user_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
        current_user_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
        active_role_oid: None,
        session_replication_role: Default::default(),
        statement_lock_scope_id: None,
        transaction_lock_scope_id: None,
        next_command_id: 0,
        default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
        expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
        case_test_values: Vec::new(),
        system_bindings: Vec::new(),
        subplans: Vec::new(),
        timed: false,
        allow_side_effects: true,
        pending_async_notifications: Vec::new(),
        catalog: None,
        compiled_functions: std::collections::HashMap::new(),
        cte_tables: std::collections::HashMap::new(),
        cte_producers: std::collections::HashMap::new(),
        recursive_worktables: std::collections::HashMap::new(),
        deferred_foreign_keys: None,
        trigger_depth: 0,
    };

    let names = state.column_names().to_vec();
    let mut rows = Vec::new();
    while let Some(slot) = exec_next(&mut state, &mut ctx)? {
        rows.push((
            names.clone(),
            slot.values()?.iter().cloned().collect::<Vec<_>>(),
        ));
    }
    Ok(rows)
}

fn explain_lines(plan: Plan) -> Vec<String> {
    let state = executor_start(plan);
    let mut lines = Vec::new();
    crate::backend::commands::explain::format_explain_lines(state.as_ref(), 0, false, &mut lines);
    lines
}

fn run_sql(
    base: &PathBuf,
    txns: &TransactionManager,
    xid: TransactionId,
    sql: &str,
) -> Result<StatementResult, ExecError> {
    run_sql_with_catalog(base, txns, xid, sql, catalog())
}

fn run_sql_with_catalog(
    base: &PathBuf,
    txns: &TransactionManager,
    xid: TransactionId,
    sql: &str,
    mut catalog: Catalog,
) -> Result<StatementResult, ExecError> {
    let base = base.clone();
    let txns = txns.clone();
    let sql = sql.to_string();
    run_with_large_stack_result("executor-test-sql", move || {
        crate::backend::catalog::store::sync_catalog_heaps_for_tests(&base, &catalog).unwrap();
        let smgr = MdStorageManager::new(&base);
        let pool = std::sync::Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 8));
        for name in catalog.table_names().collect::<Vec<_>>() {
            if let Some(entry) = catalog.get(&name) {
                create_fork(&*pool, entry.rel);
            }
        }
        let txns_arc = std::sync::Arc::new(parking_lot::RwLock::new(txns.clone()));
        let mut ctx = ExecutorContext {
            pool,
            txns: txns_arc,
            txn_waiter: None,
            sequences: Some(std::sync::Arc::new(
                crate::pgrust::database::SequenceRuntime::new_ephemeral(),
            )),
            large_objects: Some(std::sync::Arc::new(
                crate::pgrust::database::LargeObjectRuntime::new_ephemeral(),
            )),
            async_notify_runtime: None,
            advisory_locks: std::sync::Arc::new(
                crate::backend::storage::lmgr::AdvisoryLockManager::new(),
            ),
            row_locks: std::sync::Arc::new(crate::backend::storage::lmgr::RowLockManager::new()),
            checkpoint_stats:
                crate::backend::utils::misc::checkpoint::CheckpointStatsSnapshot::default(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            interrupts: std::sync::Arc::new(
                crate::backend::utils::misc::interrupts::InterruptState::new(),
            ),
            stats: std::sync::Arc::new(parking_lot::RwLock::new(
                crate::pgrust::database::DatabaseStatsStore::with_default_io_rows(),
            )),
            session_stats: std::sync::Arc::new(parking_lot::RwLock::new(
                crate::pgrust::database::SessionStatsState::default(),
            )),
            snapshot: txns.snapshot(xid).unwrap(),
            transaction_state: None,
            client_id: 77,
            current_database_name: "postgres".to_string(),
            session_user_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
            current_user_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
            active_role_oid: None,
            session_replication_role: Default::default(),
            statement_lock_scope_id: None,
            transaction_lock_scope_id: None,
            next_command_id: 0,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: Vec::new(),
            timed: false,
            allow_side_effects: true,
            pending_async_notifications: Vec::new(),
            catalog: catalog.materialize_visible_catalog(),
            compiled_functions: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
            deferred_foreign_keys: None,
            trigger_depth: 0,
        };
        execute_sql(&sql, &mut catalog, &mut ctx, xid)
    })
}

fn assert_query_rows(result: StatementResult, expected: Vec<Vec<Value>>) {
    match result {
        StatementResult::Query { rows, .. } => assert_eq!(rows, expected),
        other => panic!("expected query result, got {:?}", other),
    }
}

fn seed_people_and_pets(label: &str) -> SeededSqlHarness {
    let mut harness = SeededSqlHarness::new(label, catalog_with_pets());
    let xid = harness.txns.begin();
    harness
        .execute(
            xid,
            "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'b'), (3, 'carol', null)",
        )
        .unwrap();
    harness
        .execute(
            xid,
            "insert into pets (id, name, owner_id) values (10, 'mocha', 1), (11, 'pixel', 1), (12, 'otis', 2), (13, 'stray', null)",
        )
        .unwrap();
    harness.txns.commit(xid).unwrap();
    harness
}

#[test]
fn expr_eval_obeys_null_semantics() {
    let base = temp_dir("expr_eval_obeys_null_semantics");
    let mut ctx = empty_executor_context(&base);
    let mut slot = TupleSlot::virtual_row(vec![
        Value::Int32(7),
        Value::Text("alice".into()),
        Value::Null,
    ]);
    assert_eq!(
        eval_expr(
            &Expr::op_auto(
                crate::include::nodes::primnodes::OpExprKind::Eq,
                vec![local_var(0), Expr::Const(Value::Int32(7))]
            ),
            &mut slot,
            &mut ctx
        )
        .unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        eval_expr(
            &Expr::op_auto(
                crate::include::nodes::primnodes::OpExprKind::Eq,
                vec![local_var(2), Expr::Const(Value::Text("x".into()))]
            ),
            &mut slot,
            &mut ctx
        )
        .unwrap(),
        Value::Null
    );
    assert_eq!(
        eval_expr(
            &Expr::and(Expr::Const(Value::Bool(true)), Expr::Const(Value::Null)),
            &mut slot,
            &mut ctx
        )
        .unwrap(),
        Value::Null
    );
    assert_eq!(
        eval_expr(&Expr::IsNull(Box::new(local_var(2))), &mut slot, &mut ctx).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        eval_expr(
            &Expr::IsNotNull(Box::new(local_var(2))),
            &mut slot,
            &mut ctx
        )
        .unwrap(),
        Value::Bool(false)
    );
    assert_eq!(
        eval_expr(
            &Expr::IsDistinctFrom(Box::new(local_var(2)), Box::new(Expr::Const(Value::Null))),
            &mut slot,
            &mut ctx
        )
        .unwrap(),
        Value::Bool(false)
    );
    assert_eq!(
        eval_expr(
            &Expr::IsDistinctFrom(Box::new(local_var(1)), Box::new(Expr::Const(Value::Null))),
            &mut slot,
            &mut ctx
        )
        .unwrap(),
        Value::Bool(true)
    );
}

#[test]
fn pg_column_compression_reports_compressed_heap_values() {
    let base = temp_dir("pg_column_compression_reports_compressed_heap_values");
    let mut ctx = empty_executor_context(&base);
    let input = "1234567890".repeat(1000);
    let compressed = crate::backend::access::common::toast_compression::compress_inline_datum(
        input.as_bytes(),
        crate::include::access::htup::AttributeCompression::Pglz,
        crate::include::access::htup::AttributeCompression::Pglz,
    )
    .unwrap()
    .expect("value should compress inline");
    let desc = Rc::new(relation_desc());
    let attr_descs: Rc<[AttributeDesc]> = desc.attribute_descs().into();
    let tuple = HeapTuple::from_values(
        &attr_descs,
        &[
            TupleValue::Bytes(1i32.to_le_bytes().to_vec()),
            TupleValue::EncodedVarlena(compressed.encoded),
            TupleValue::Null,
        ],
    )
    .unwrap();
    let mut slot = TupleSlot::from_heap_tuple(
        desc,
        attr_descs,
        crate::include::access::htup::ItemPointerData {
            block_number: 0,
            offset_number: 1,
        },
        tuple,
    );
    let expr = Expr::func_with_impl(
        6604,
        Some(crate::backend::parser::SqlType::new(
            crate::backend::parser::SqlTypeKind::Text,
        )),
        false,
        crate::include::nodes::primnodes::ScalarFunctionImpl::Builtin(
            crate::include::nodes::primnodes::BuiltinScalarFunction::PgColumnCompression,
        ),
        vec![Expr::Var(Var {
            varno: crate::include::nodes::primnodes::OUTER_VAR,
            varattno: user_attrno(1),
            varlevelsup: 0,
            vartype: crate::backend::parser::SqlType::new(
                crate::backend::parser::SqlTypeKind::Text,
            ),
        })],
    );

    assert_eq!(
        eval_expr(&expr, &mut slot, &mut ctx).unwrap(),
        Value::Text("pglz".into())
    );
    assert_eq!(
        slot.values().unwrap(),
        &[Value::Int32(1), Value::Text(input.into()), Value::Null]
    );
}

#[test]
fn advisory_lock_builtins_are_rejected_in_read_only_executor_context() {
    let base = temp_dir("advisory_lock_builtins_are_rejected_in_read_only_executor_context");
    let mut ctx = empty_executor_context(&base);
    ctx.allow_side_effects = false;
    let mut slot = TupleSlot::virtual_row(vec![]);

    let err = eval_expr(
        &Expr::builtin_func(
            crate::include::nodes::primnodes::BuiltinScalarFunction::PgAdvisoryLock,
            Some(crate::backend::parser::SqlType::new(
                crate::backend::parser::SqlTypeKind::Void,
            )),
            false,
            vec![Expr::Const(Value::Int64(1))],
        ),
        &mut slot,
        &mut ctx,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ExecError::DetailedError {
            sqlstate: "25006",
            ..
        }
    ));
}

#[test]
fn pg_notify_is_rejected_in_read_only_executor_context() {
    let base = temp_dir("pg_notify_is_rejected_in_read_only_executor_context");
    let mut ctx = empty_executor_context(&base);
    ctx.allow_side_effects = false;
    let mut slot = TupleSlot::virtual_row(vec![]);

    let err = eval_expr(
        &Expr::builtin_func(
            crate::include::nodes::primnodes::BuiltinScalarFunction::PgNotify,
            Some(crate::backend::parser::SqlType::new(
                crate::backend::parser::SqlTypeKind::Void,
            )),
            false,
            vec![
                Expr::Const(Value::Text("alerts".into())),
                Expr::Const(Value::Text("payload".into())),
            ],
        ),
        &mut slot,
        &mut ctx,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ExecError::DetailedError {
            sqlstate: "25006",
            ..
        }
    ));
}

#[test]
fn pg_notification_queue_usage_returns_zero_with_empty_runtime() {
    let base = temp_dir("pg_notification_queue_usage_returns_zero_with_empty_runtime");
    let mut ctx = empty_executor_context(&base);
    ctx.async_notify_runtime = Some(std::sync::Arc::new(
        crate::pgrust::database::AsyncNotifyRuntime::new(),
    ));
    let mut slot = TupleSlot::virtual_row(vec![]);

    let value = eval_expr(
        &Expr::builtin_func(
            crate::include::nodes::primnodes::BuiltinScalarFunction::PgNotificationQueueUsage,
            Some(crate::backend::parser::SqlType::new(
                crate::backend::parser::SqlTypeKind::Float8,
            )),
            false,
            vec![],
        ),
        &mut slot,
        &mut ctx,
    )
    .unwrap();

    assert_eq!(value, Value::Float64(0.0));
}

#[test]
fn physical_slot_lazily_deforms_heap_tuple() {
    use crate::include::access::htup::ItemPointerData;
    let desc = Rc::new(relation_desc());
    let attr_descs: Rc<[AttributeDesc]> = desc.attribute_descs().into();
    let mut slot = TupleSlot::from_heap_tuple(
        desc,
        attr_descs,
        ItemPointerData {
            block_number: 0,
            offset_number: 1,
        },
        tuple(1, "alice", None),
    );
    assert_eq!(
        slot.values().unwrap(),
        &[Value::Int32(1), Value::Text("alice".into()), Value::Null]
    );
    assert_eq!(
        slot.tid(),
        Some(ItemPointerData {
            block_number: 0,
            offset_number: 1
        })
    );
}

#[test]
fn seqscan_filter_projection_returns_expected_rows() {
    let base = temp_dir("scan_filter_project");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let pool = test_pool(&base);
    let xid = txns.begin();
    let rows = [
        tuple(1, "alice", Some("alpha")),
        tuple(2, "bob", None),
        tuple(3, "carol", Some("gamma")),
    ];
    let mut blocks = Vec::new();
    for row in rows {
        let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &row).unwrap();
        blocks.push(tid.block_number);
    }
    txns.commit(xid).unwrap();
    blocks.sort();
    blocks.dedup();
    for block in blocks {
        heap_flush(&*pool, 1, rel(), block).unwrap();
    }
    drop(pool);
    let plan = Plan::Projection {
        plan_info: crate::backend::executor::PlanEstimate::default(),
        input: Box::new(Plan::Filter {
            plan_info: crate::backend::executor::PlanEstimate::default(),
            input: Box::new(Plan::SeqScan {
                plan_info: crate::backend::executor::PlanEstimate::default(),
                source_id: 1,
                rel: rel(),
                relation_name: "people".into(),
                relation_oid: 0,
                relkind: 'r',
                toast: None,
                desc: relation_desc(),
            }),
            predicate: Expr::op_auto(
                crate::include::nodes::primnodes::OpExprKind::Gt,
                vec![local_var(0), Expr::Const(Value::Int32(1))],
            ),
        }),
        targets: vec![
            TargetEntry::new(
                "name",
                local_var(1),
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Text),
                1,
            ),
            TargetEntry::new(
                "note_is_null",
                Expr::IsNull(Box::new(local_var(2))),
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Bool),
                2,
            ),
        ],
    };
    let rows = run_plan(&base, &txns, plan).unwrap();
    assert_eq!(
        rows,
        vec![
            (
                vec!["name".into(), "note_is_null".into()],
                vec![Value::Text("bob".into()), Value::Bool(true)]
            ),
            (
                vec!["name".into(), "note_is_null".into()],
                vec![Value::Text("carol".into()), Value::Bool(false)]
            ),
        ]
    );
}

#[test]
fn seqscan_skips_superseded_versions() {
    let base = temp_dir("visible_versions");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let pool = test_pool(&base);
    let insert_xid = txns.begin();
    let old_tid = heap_insert_mvcc(
        &*pool,
        1,
        rel(),
        insert_xid,
        &tuple(1, "alice", Some("old")),
    )
    .unwrap();
    txns.commit(insert_xid).unwrap();
    let update_xid = txns.begin();
    let new_tid = heap_update(
        &*pool,
        1,
        rel(),
        &txns,
        update_xid,
        old_tid,
        &tuple(1, "alice", Some("new")),
    )
    .unwrap();
    txns.commit(update_xid).unwrap();
    heap_flush(&*pool, 1, rel(), old_tid.block_number).unwrap();
    if new_tid.block_number != old_tid.block_number {
        heap_flush(&*pool, 1, rel(), new_tid.block_number).unwrap();
    }
    drop(pool);
    let plan = Plan::SeqScan {
        plan_info: crate::backend::executor::PlanEstimate::default(),
        source_id: 1,
        rel: rel(),
        relation_name: "people".into(),
        relation_oid: 0,
        relkind: 'r',
        toast: None,
        desc: relation_desc(),
    };
    let rows = run_plan(&base, &txns, plan).unwrap();
    assert_eq!(
        rows,
        vec![(
            vec!["id".into(), "name".into(), "note".into()],
            vec![
                Value::Int32(1),
                Value::Text("alice".into()),
                Value::Text("new".into())
            ]
        )]
    );
}

#[test]
fn manual_hash_join_inner_returns_matching_rows() {
    let harness = seed_people_and_pets("manual_hash_join_inner");

    let plan = Plan::Projection {
        plan_info: PlanEstimate::default(),
        input: Box::new(people_pets_hash_join_plan(JoinType::Inner, vec![], vec![])),
        targets: vec![
            TargetEntry::new(
                "person_id",
                local_var(0),
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                1,
            ),
            TargetEntry::new(
                "pet_id",
                local_var(3),
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                2,
            ),
        ],
    };

    let rows = run_plan(&harness.base, &harness.txns, plan).unwrap();
    assert_eq!(
        rows,
        vec![
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Int32(1), Value::Int32(10)],
            ),
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Int32(1), Value::Int32(11)],
            ),
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Int32(2), Value::Int32(12)],
            ),
        ]
    );
}

#[test]
fn manual_hash_join_left_emits_null_extended_rows() {
    let harness = seed_people_and_pets("manual_hash_join_left");

    let plan = Plan::Projection {
        plan_info: PlanEstimate::default(),
        input: Box::new(people_pets_hash_join_plan(JoinType::Left, vec![], vec![])),
        targets: vec![
            TargetEntry::new(
                "person_id",
                local_var(0),
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                1,
            ),
            TargetEntry::new(
                "pet_id",
                local_var(3),
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                2,
            ),
        ],
    };

    let rows = run_plan(&harness.base, &harness.txns, plan).unwrap();
    assert_eq!(
        rows,
        vec![
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Int32(1), Value::Int32(10)],
            ),
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Int32(1), Value::Int32(11)],
            ),
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Int32(2), Value::Int32(12)],
            ),
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Int32(3), Value::Null],
            ),
        ]
    );
}

#[test]
fn manual_hash_join_right_emits_unmatched_inner_rows() {
    let harness = seed_people_and_pets("manual_hash_join_right");

    let plan = Plan::Projection {
        plan_info: PlanEstimate::default(),
        input: Box::new(people_pets_hash_join_plan(JoinType::Right, vec![], vec![])),
        targets: vec![
            TargetEntry::new(
                "person_id",
                local_var(0),
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                1,
            ),
            TargetEntry::new(
                "pet_id",
                local_var(3),
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                2,
            ),
        ],
    };

    let rows = run_plan(&harness.base, &harness.txns, plan).unwrap();
    assert_eq!(
        rows,
        vec![
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Int32(1), Value::Int32(10)],
            ),
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Int32(1), Value::Int32(11)],
            ),
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Int32(2), Value::Int32(12)],
            ),
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Null, Value::Int32(13)],
            ),
        ]
    );
}

#[test]
fn manual_hash_join_full_emits_unmatched_rows_from_both_sides() {
    let harness = seed_people_and_pets("manual_hash_join_full");

    let plan = Plan::Projection {
        plan_info: PlanEstimate::default(),
        input: Box::new(people_pets_hash_join_plan(JoinType::Full, vec![], vec![])),
        targets: vec![
            TargetEntry::new(
                "person_id",
                local_var(0),
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                1,
            ),
            TargetEntry::new(
                "pet_id",
                local_var(3),
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                2,
            ),
        ],
    };

    let rows = run_plan(&harness.base, &harness.txns, plan).unwrap();
    assert_eq!(
        rows,
        vec![
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Int32(1), Value::Int32(10)],
            ),
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Int32(1), Value::Int32(11)],
            ),
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Int32(2), Value::Int32(12)],
            ),
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Int32(3), Value::Null],
            ),
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Null, Value::Int32(13)],
            ),
        ]
    );
}

#[test]
fn manual_hash_join_null_hash_keys_do_not_match_each_other() {
    let base = temp_dir("manual_hash_join_null_keys");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let int4 = crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4);
    let output_columns = vec![QueryColumn {
        name: "id".into(),
        sql_type: int4,
        wire_type_oid: None,
    }];

    let plan = Plan::HashJoin {
        plan_info: PlanEstimate::default(),
        left: Box::new(Plan::Values {
            plan_info: PlanEstimate::default(),
            rows: vec![
                vec![Expr::Const(Value::Null)],
                vec![Expr::Const(Value::Int32(1))],
            ],
            output_columns: output_columns.clone(),
        }),
        right: Box::new(Plan::Hash {
            plan_info: PlanEstimate::default(),
            input: Box::new(Plan::Values {
                plan_info: PlanEstimate::default(),
                rows: vec![
                    vec![Expr::Const(Value::Null)],
                    vec![Expr::Const(Value::Int32(1))],
                ],
                output_columns,
            }),
            hash_keys: vec![local_var(0)],
        }),
        kind: JoinType::Inner,
        hash_clauses: vec![Expr::op_auto(
            crate::include::nodes::primnodes::OpExprKind::Eq,
            vec![local_var(0), local_var(1)],
        )],
        hash_keys: vec![local_var(0)],
        join_qual: vec![],
        qual: vec![],
    };

    let rows = run_plan(&base, &txns, plan).unwrap();
    assert_eq!(
        rows,
        vec![(
            vec!["id".into(), "id".into()],
            vec![Value::Int32(1), Value::Int32(1)],
        )]
    );
}

#[test]
fn manual_hash_join_join_qual_preserves_left_outer_fill() {
    let harness = seed_people_and_pets("manual_hash_join_join_qual");

    let plan = Plan::Projection {
        plan_info: PlanEstimate::default(),
        input: Box::new(people_pets_hash_join_plan(
            JoinType::Left,
            vec![Expr::op_auto(
                crate::include::nodes::primnodes::OpExprKind::Eq,
                vec![local_var(3), Expr::Const(Value::Int32(11))],
            )],
            vec![],
        )),
        targets: vec![
            TargetEntry::new(
                "person_id",
                local_var(0),
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                1,
            ),
            TargetEntry::new(
                "pet_id",
                local_var(3),
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                2,
            ),
        ],
    };

    let rows = run_plan(&harness.base, &harness.txns, plan).unwrap();
    assert_eq!(
        rows,
        vec![
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Int32(1), Value::Int32(11)],
            ),
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Int32(2), Value::Null],
            ),
            (
                vec!["person_id".into(), "pet_id".into()],
                vec![Value::Int32(3), Value::Null],
            ),
        ]
    );
}

#[test]
fn manual_hash_join_explain_formats_hash_child() {
    let lines = explain_lines(people_pets_hash_join_plan(JoinType::Inner, vec![], vec![]));
    assert!(lines.first().is_some_and(|line| line.contains("Hash Join")));
    assert!(lines.iter().any(|line| line.contains("Hash  (cost=")));
}

#[test]
#[should_panic(expected = "HashJoin right child must be Plan::Hash")]
fn manual_hash_join_rejects_non_hash_inner_plan() {
    let _ = executor_start(Plan::HashJoin {
        plan_info: PlanEstimate::default(),
        left: Box::new(people_scan_plan()),
        right: Box::new(pets_scan_plan()),
        kind: JoinType::Inner,
        hash_clauses: vec![Expr::op_auto(
            crate::include::nodes::primnodes::OpExprKind::Eq,
            vec![local_var(0), local_var(5)],
        )],
        hash_keys: vec![local_var(0)],
        join_qual: vec![],
        qual: vec![],
    });
}

#[test]
fn insert_sql_inserts_row() {
    let mut harness = SeededSqlHarness::new("insert_sql", catalog());
    let xid = harness.txns.begin();
    assert_eq!(
        harness
            .execute(
                xid,
                "insert into people (id, name, note) values (1, 'alice', 'alpha')",
            )
            .unwrap(),
        StatementResult::AffectedRows(1)
    );
    harness.txns.commit(xid).unwrap();
    match harness
        .execute(INVALID_TRANSACTION_ID, "select name, note from people")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            println!("{rows:?}");
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("alice".into()),
                    Value::Text("alpha".into())
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn analyze_sql_validates_existing_targets() {
    let base = temp_dir("analyze_sql");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_eq!(
        run_sql(&base, &txns, INVALID_TRANSACTION_ID, "analyze people(note)").unwrap(),
        StatementResult::AffectedRows(0)
    );
}
#[test]
fn analyze_sql_rejects_missing_columns() {
    let base = temp_dir("analyze_missing_column");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "analyze people(nope)").unwrap_err() {
        ExecError::Parse(ParseError::UnknownColumn(name)) => assert_eq!(name, "nope"),
        other => panic!("expected unknown column, got {:?}", other),
    }
}
#[test]
fn vacuum_analyze_sql_succeeds_outside_transaction() {
    let base = temp_dir("vacuum_analyze_sql");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_eq!(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "vacuum analyze people(note)"
        )
        .unwrap(),
        StatementResult::AffectedRows(0)
    );
}
#[test]
fn select_sql_with_table_alias() {
    let base = temp_dir("select_sql_table_alias");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'alpha')",
    )
    .unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select p.name from people p where p.id = 1",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["name"]);
            assert_eq!(rows, vec![vec![Value::Text("alice".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn select_sql_text_cast() {
    let base = temp_dir("select_sql_text_cast");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'alpha')",
    )
    .unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select (id)::text from people",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["id"]);
            assert_eq!(rows, vec![vec![Value::Text("1".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn select_sql_varchar_cast_truncates() {
    let base = temp_dir("select_sql_varchar_cast");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'abcdef'::varchar(3)",
    )
    .unwrap()
    {
        StatementResult::Query { columns, rows, .. } => {
            assert_eq!(
                columns[0].sql_type,
                crate::backend::parser::SqlType::with_char_len(
                    crate::backend::parser::SqlTypeKind::Varchar,
                    3
                )
            );
            assert_eq!(rows, vec![vec![Value::Text("abc".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn setop_join_branch_executes_with_child_local_vars() {
    let base = temp_dir("setop_join_branch_child_roots");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select x
             from (values (1)) base(x)
             union all
             select l.x + r.y
             from (values (1)) l(x)
             join (values (2)) r(y) on true",
        )
        .unwrap(),
        vec![vec![Value::Int32(1)], vec![Value::Int32(3)]],
    );
}
#[test]
fn select_sql_plain_varchar_cast_preserves_text() {
    let base = temp_dir("select_sql_plain_varchar_cast");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'abcdef'::varchar",
    )
    .unwrap()
    {
        StatementResult::Query { columns, rows, .. } => {
            assert_eq!(
                columns[0].sql_type,
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Varchar)
            );
            assert_eq!(rows, vec![vec![Value::Text("abcdef".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn select_sql_type_cast_with_alias() {
    let base = temp_dir("select_sql_type_cast_alias");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'alpha')",
    )
    .unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select (p.name)::text as w from people p",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["w"]);
            assert_eq!(rows, vec![vec![Value::Text("alice".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn select_star_sql_with_table_alias() {
    let base = temp_dir("select_star_sql_table_alias");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'alpha')",
    )
    .unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from people p",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["id", "name", "note"]);
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int32(1),
                    Value::Text("alice".into()),
                    Value::Text("alpha".into())
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn select_sql_explicit_alias_overrides_column_name() {
    let base = temp_dir("select_sql_explicit_alias");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'alpha')",
    )
    .unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select p.name as w from people p",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["w"]);
            assert_eq!(rows, vec![vec![Value::Text("alice".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn select_sql_explicit_alias_preserved_for_empty_result() {
    let base = temp_dir("select_sql_explicit_alias_empty");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select p.name as w from people p",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["w"]);
            assert!(rows.is_empty());
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn insert_sql_inserts_multiple_rows() {
    let mut harness = SeededSqlHarness::new("insert_multi_sql", catalog());
    let xid = harness.txns.begin();
    assert_eq!(
        harness
            .execute(
                xid,
                "insert into people (id, name, note) values (1, 'alice', 'alpha'), (2, 'bob', null)",
            )
            .unwrap(),
        StatementResult::AffectedRows(2)
    );
    harness.txns.commit(xid).unwrap();
    match harness
        .execute(INVALID_TRANSACTION_ID, "select id, name, note from people")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Int32(1),
                        Value::Text("alice".into()),
                        Value::Text("alpha".into())
                    ],
                    vec![Value::Int32(2), Value::Text("bob".into()), Value::Null]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn on_conflict_do_nothing_inserts_when_no_conflict() {
    let mut harness = SeededSqlHarness::new(
        "upsert_insert_no_conflict",
        catalog_with_people_primary_key(),
    );
    let xid = harness.txns.begin();
    assert_eq!(
        harness
            .execute(
                xid,
                "insert into people (id, name, note) values (1, 'alice', 'alpha') on conflict (id) do nothing",
            )
            .unwrap(),
        StatementResult::AffectedRows(1)
    );
    harness.txns.commit(xid).unwrap();
}

#[test]
fn on_conflict_targeted_do_nothing_skips_duplicate() {
    let mut harness = SeededSqlHarness::new(
        "upsert_targeted_do_nothing",
        catalog_with_people_primary_key(),
    );

    let insert_xid = harness.txns.begin();
    harness
        .execute(
            insert_xid,
            "insert into people (id, name, note) values (1, 'alice', 'alpha')",
        )
        .unwrap();
    harness.txns.commit(insert_xid).unwrap();

    let upsert_xid = harness.txns.begin();
    assert_eq!(
        harness
            .execute(
                upsert_xid,
                "insert into people (id, name, note) values (1, 'bob', 'beta') on conflict (id) do nothing",
            )
            .unwrap(),
        StatementResult::AffectedRows(0)
    );
    harness.txns.commit(upsert_xid).unwrap();

    assert_query_rows(
        harness
            .execute(INVALID_TRANSACTION_ID, "select id, name, note from people")
            .unwrap(),
        vec![vec![
            Value::Int32(1),
            Value::Text("alice".into()),
            Value::Text("alpha".into()),
        ]],
    );
}

#[test]
fn on_conflict_targetless_do_nothing_skips_duplicate() {
    let mut harness = SeededSqlHarness::new(
        "upsert_targetless_do_nothing",
        catalog_with_people_primary_key(),
    );

    let insert_xid = harness.txns.begin();
    harness
        .execute(
            insert_xid,
            "insert into people (id, name, note) values (1, 'alice', 'alpha')",
        )
        .unwrap();
    harness.txns.commit(insert_xid).unwrap();

    let upsert_xid = harness.txns.begin();
    assert_eq!(
        harness
            .execute(
                upsert_xid,
                "insert into people (id, name, note) values (1, 'bob', 'beta') on conflict do nothing",
            )
            .unwrap(),
        StatementResult::AffectedRows(0)
    );
    harness.txns.commit(upsert_xid).unwrap();
}

#[test]
fn on_conflict_do_update_can_use_target_and_excluded_values() {
    let mut harness = SeededSqlHarness::new(
        "upsert_do_update_target_and_excluded",
        catalog_with_people_primary_key(),
    );

    let insert_xid = harness.txns.begin();
    harness
        .execute(
            insert_xid,
            "insert into people (id, name, note) values (1, 'alice', 'alpha')",
        )
        .unwrap();
    harness.txns.commit(insert_xid).unwrap();

    let upsert_xid = harness.txns.begin();
    assert_eq!(
        harness
            .execute(
                upsert_xid,
                "insert into people (id, name, note) values (1, 'bob', 'beta') on conflict (id) do update set name = excluded.name, note = people.name",
            )
            .unwrap(),
        StatementResult::AffectedRows(1)
    );
    harness.txns.commit(upsert_xid).unwrap();

    assert_query_rows(
        harness
            .execute(INVALID_TRANSACTION_ID, "select name, note from people")
            .unwrap(),
        vec![vec![Value::Text("bob".into()), Value::Text("alice".into())]],
    );
}

#[test]
fn on_conflict_do_update_where_false_skips_row() {
    let mut harness = SeededSqlHarness::new(
        "upsert_do_update_where_false",
        catalog_with_people_primary_key(),
    );

    let insert_xid = harness.txns.begin();
    harness
        .execute(
            insert_xid,
            "insert into people (id, name, note) values (1, 'alice', 'alpha')",
        )
        .unwrap();
    harness.txns.commit(insert_xid).unwrap();

    let upsert_xid = harness.txns.begin();
    assert_eq!(
        harness
            .execute(
                upsert_xid,
                "insert into people (id, name, note) values (1, 'bob', 'beta') on conflict (id) do update set name = excluded.name where false",
            )
            .unwrap(),
        StatementResult::AffectedRows(0)
    );
    harness.txns.commit(upsert_xid).unwrap();

    assert_query_rows(
        harness
            .execute(INVALID_TRANSACTION_ID, "select name, note from people")
            .unwrap(),
        vec![vec![
            Value::Text("alice".into()),
            Value::Text("alpha".into()),
        ]],
    );
}

#[test]
fn on_conflict_do_update_rejects_duplicate_input_rows() {
    let mut harness = SeededSqlHarness::new(
        "upsert_duplicate_input_rows",
        catalog_with_people_primary_key(),
    );
    let xid = harness.txns.begin();
    let err = harness
        .execute(
            xid,
            "insert into people (id, name) values (1, 'alice'), (1, 'bob') on conflict (id) do update set name = excluded.name",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::CardinalityViolation { message, hint }
            if message == "ON CONFLICT DO UPDATE command cannot affect row a second time"
                && hint.as_deref()
                    == Some("Ensure that no rows proposed for insertion within the same command have duplicate constrained values.")
    ));
    harness.txns.commit(xid).unwrap();

    assert_query_rows(
        harness
            .execute(INVALID_TRANSACTION_ID, "select count(*) from people")
            .unwrap(),
        vec![vec![Value::Int64(0)]],
    );
}

#[test]
fn on_conflict_do_update_duplicate_existing_conflicts_leave_row_unchanged() {
    let mut harness = SeededSqlHarness::new(
        "upsert_duplicate_existing_conflicts",
        catalog_with_people_primary_key(),
    );

    let seed_xid = harness.txns.begin();
    harness
        .execute(
            seed_xid,
            "insert into people (id, name, note) values (1, 'alice', 'alpha')",
        )
        .unwrap();
    harness.txns.commit(seed_xid).unwrap();

    let xid = harness.txns.begin();
    let err = harness
        .execute(
            xid,
            "insert into people (id, name) values (1, 'bob'), (1, 'carol') on conflict (id) do update set name = excluded.name",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::CardinalityViolation { message, .. }
            if message == "ON CONFLICT DO UPDATE command cannot affect row a second time"
    ));
    harness.txns.commit(xid).unwrap();

    assert_query_rows(
        harness
            .execute(INVALID_TRANSACTION_ID, "select id, name, note from people")
            .unwrap(),
        vec![vec![
            Value::Int32(1),
            Value::Text("alice".into()),
            Value::Text("alpha".into()),
        ]],
    );
}

#[test]
fn on_conflict_do_update_where_false_allows_duplicate_existing_conflicts() {
    let mut harness = SeededSqlHarness::new(
        "upsert_duplicate_where_false_existing_conflicts",
        catalog_with_people_primary_key(),
    );

    let seed_xid = harness.txns.begin();
    harness
        .execute(
            seed_xid,
            "insert into people (id, name, note) values (1, 'alice', 'alpha')",
        )
        .unwrap();
    harness.txns.commit(seed_xid).unwrap();

    let xid = harness.txns.begin();
    assert_eq!(
        harness
            .execute(
                xid,
                "insert into people (id, name) values (1, 'bob'), (1, 'carol') on conflict (id) do update set name = excluded.name where false",
            )
            .unwrap(),
        StatementResult::AffectedRows(0)
    );
    harness.txns.commit(xid).unwrap();

    assert_query_rows(
        harness
            .execute(INVALID_TRANSACTION_ID, "select id, name, note from people")
            .unwrap(),
        vec![vec![
            Value::Int32(1),
            Value::Text("alice".into()),
            Value::Text("alpha".into()),
        ]],
    );
}

#[test]
fn on_conflict_do_update_allows_duplicate_input_after_arbiter_key_changes() {
    let mut harness = SeededSqlHarness::new(
        "upsert_duplicate_after_arbiter_key_change",
        catalog_with_people_note_unique_index(),
    );

    let seed_xid = harness.txns.begin();
    harness
        .execute(
            seed_xid,
            "insert into people (id, name, note) values (1, 'seed', 'key')",
        )
        .unwrap();
    harness.txns.commit(seed_xid).unwrap();

    let xid = harness.txns.begin();
    assert_eq!(
        harness
            .execute(
                xid,
                "insert into people (id, name, note) values (2, 'newkey1', 'key'), (3, 'newkey2', 'key') on conflict (note) do update set name = excluded.name, note = excluded.name",
            )
            .unwrap(),
        StatementResult::AffectedRows(2)
    );
    harness.txns.commit(xid).unwrap();

    assert_query_rows(
        harness
            .execute(
                INVALID_TRANSACTION_ID,
                "select id, name, note from people order by id",
            )
            .unwrap(),
        vec![
            vec![
                Value::Int32(1),
                Value::Text("newkey1".into()),
                Value::Text("newkey1".into()),
            ],
            vec![
                Value::Int32(3),
                Value::Text("newkey2".into()),
                Value::Text("key".into()),
            ],
        ],
    );
}

#[test]
fn on_conflict_null_arbiter_keys_do_not_conflict() {
    let mut harness = SeededSqlHarness::new(
        "upsert_null_arbiter_keys",
        catalog_with_people_note_unique_index(),
    );

    let first_xid = harness.txns.begin();
    harness
        .execute(
            first_xid,
            "insert into people (id, name, note) values (1, 'alice', null)",
        )
        .unwrap();
    harness.txns.commit(first_xid).unwrap();

    let second_xid = harness.txns.begin();
    assert_eq!(
        harness
            .execute(
                second_xid,
                "insert into people (id, name, note) values (2, 'bob', null) on conflict (note) do nothing",
            )
            .unwrap(),
        StatementResult::AffectedRows(1)
    );
    harness.txns.commit(second_xid).unwrap();

    assert_query_rows(
        harness
            .execute(INVALID_TRANSACTION_ID, "select count(*) from people")
            .unwrap(),
        vec![vec![Value::Int64(2)]],
    );
}

#[test]
fn update_sql_updates_matching_rows() {
    let mut harness = SeededSqlHarness::new("update_sql", catalog());
    let insert_xid = harness.txns.begin();
    harness
        .execute(
            insert_xid,
            "insert into people (id, name, note) values (1, 'alice', 'old')",
        )
        .unwrap();
    harness.txns.commit(insert_xid).unwrap();
    let update_xid = harness.txns.begin();
    assert_eq!(
        harness
            .execute(update_xid, "update people set note = 'new' where id = 1")
            .unwrap(),
        StatementResult::AffectedRows(1)
    );
    harness.txns.commit(update_xid).unwrap();
    match harness
        .execute(
            INVALID_TRANSACTION_ID,
            "select note from people where id = 1",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("new".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn delete_sql_deletes_matching_rows() {
    let mut harness = SeededSqlHarness::new("delete_sql", catalog());
    let insert_xid = harness.txns.begin();
    harness
        .execute(
            insert_xid,
            "insert into people (id, name, note) values (1, 'alice', null)",
        )
        .unwrap();
    harness
        .execute(
            insert_xid,
            "insert into people (id, name, note) values (2, 'bob', 'keep')",
        )
        .unwrap();
    harness.txns.commit(insert_xid).unwrap();
    let delete_xid = harness.txns.begin();
    assert_eq!(
        harness
            .execute(delete_xid, "delete from people where note is null")
            .unwrap(),
        StatementResult::AffectedRows(1)
    );
    harness.txns.commit(delete_xid).unwrap();
    match harness
        .execute(INVALID_TRANSACTION_ID, "select name from people")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("bob".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn order_by_limit_offset_returns_expected_rows() {
    let mut harness = SeededSqlHarness::new("order_by_limit_offset", catalog());
    let insert_xid = harness.txns.begin();
    harness
        .execute(
            insert_xid,
            "insert into people (id, name, note) values (1, 'alice', 'a'), (3, 'carol', 'c'), (2, 'bob', null)",
        )
        .unwrap();
    harness.txns.commit(insert_xid).unwrap();
    match harness
        .execute(
            INVALID_TRANSACTION_ID,
            "select id, name from people order by id desc limit 2 offset 1",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(2), Value::Text("bob".into())],
                    vec![Value::Int32(1), Value::Text("alice".into())]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn explain_mentions_sort_and_limit_nodes() {
    let base = temp_dir("explain_sort_limit");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "explain select name from people order by id desc limit 1 offset 2",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            let rendered = rows
                .into_iter()
                .map(|row| match &row[0] {
                    Value::Text(text) => text.clone(),
                    other => panic!("expected explain text row, got {:?}", other),
                })
                .collect::<Vec<_>>();
            assert!(rendered.iter().any(|line| line.contains("Projection")));
            assert!(rendered.iter().any(|line| line.contains("Limit")));
            assert!(rendered.iter().any(|line| line.contains("Sort")));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn order_by_nulls_first_and_last_work() {
    let mut harness = SeededSqlHarness::new("order_by_nulls", catalog());
    let insert_xid = harness.txns.begin();
    harness
        .execute(
            insert_xid,
            "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'c')",
        )
        .unwrap();
    harness.txns.commit(insert_xid).unwrap();
    match harness
        .execute(
            INVALID_TRANSACTION_ID,
            "select id from people order by note asc nulls first",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(2)],
                    vec![Value::Int32(1)],
                    vec![Value::Int32(3)]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
    match harness
        .execute(
            INVALID_TRANSACTION_ID,
            "select id from people order by note desc nulls last",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(3)],
                    vec![Value::Int32(1)],
                    vec![Value::Int32(2)]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn null_predicates_work_in_where_clause() {
    let mut harness = SeededSqlHarness::new("null_predicates", catalog());
    let insert_xid = harness.txns.begin();
    harness
        .execute(
            insert_xid,
            "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'c')",
        )
        .unwrap();
    harness.txns.commit(insert_xid).unwrap();
    match harness
        .execute(
            INVALID_TRANSACTION_ID,
            "select id from people where note is null",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
    match harness
        .execute(
            INVALID_TRANSACTION_ID,
            "select id from people where note is not null order by id",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(3)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
    match harness
        .execute(
            INVALID_TRANSACTION_ID,
            "select id from people where note is distinct from null order by id",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(3)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
    match harness
        .execute(
            INVALID_TRANSACTION_ID,
            "select id from people where note is not distinct from null",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn explain_returns_plan_lines() {
    let base = temp_dir("explain_sql");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "explain select name from people",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["QUERY PLAN".to_string()]);
            let rendered = rows
                .into_iter()
                .map(|row| match &row[0] {
                    Value::Text(text) => text.clone(),
                    other => panic!("expected text explain line, got {:?}", other),
                })
                .collect::<Vec<_>>();
            assert!(rendered.iter().any(|line| line.contains("Projection")));
            assert!(rendered.iter().any(|line| line.contains("Seq Scan")));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn explain_scan_filter_renders_single_seq_scan_line() {
    let base = temp_dir("explain_single_scan_filter");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "explain (costs off) select * from people where id > 1",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            let rendered = rows
                .into_iter()
                .map(|row| match &row[0] {
                    Value::Text(text) => text.clone(),
                    other => panic!("expected text explain line, got {:?}", other),
                })
                .collect::<Vec<_>>();
            assert_eq!(
                rendered
                    .iter()
                    .filter(|line| line.contains("Seq Scan on people"))
                    .count(),
                1,
                "expected one Seq Scan line, got {rendered:?}"
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn explain_const_false_scan_filter_uses_one_time_filter() {
    let base = temp_dir("explain_const_false_scan_filter");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "explain (costs off) select * from people where nullif(1, 2) = 2",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            let rendered = rows
                .into_iter()
                .map(|row| match &row[0] {
                    Value::Text(text) => text.clone(),
                    other => panic!("expected text explain line, got {:?}", other),
                })
                .collect::<Vec<_>>();
            assert!(rendered.iter().any(|line| line.as_str() == "Result"));
            assert!(
                rendered
                    .iter()
                    .any(|line| line.trim() == "One-Time Filter: false")
            );
            assert!(
                !rendered.iter().any(|line| line.contains("Seq Scan")),
                "expected Result-only explain, got {rendered:?}"
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn select_without_from_returns_constant_row() {
    let base = temp_dir("select_without_from");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 1").unwrap() {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["?column?".to_string()]);
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn select_case_without_from_uses_case_column_name() {
    let base = temp_dir("select_case_without_from");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select case when true then 1 else 0 end",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["case".to_string()]);
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn select_array_literal_uses_array_column_name() {
    let base = temp_dir("select_array_literal_column_name");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select array[1,null,3]",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["array".to_string()]);
            assert_eq!(
                rows,
                vec![vec![Value::PgArray(ArrayValue::from_dimensions(
                    vec![ArrayDimension {
                        lower_bound: 1,
                        length: 3,
                    }],
                    vec![Value::Int32(1), Value::Null, Value::Int32(3)],
                ))]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn select_from_people_returns_zero_column_rows() {
    let base = temp_dir("select_from_people");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'alpha'), (2, 'bob', null)",
    )
    .unwrap();
    txns.commit(xid).unwrap();
    match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select from people").unwrap() {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert!(column_names.is_empty());
            assert_eq!(rows, vec![vec![], vec![]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn explain_analyze_buffers_reports_runtime_and_buffers() {
    let mut harness = SeededSqlHarness::new("explain_analyze_sql", catalog());
    let xid = harness.txns.begin();
    harness
        .execute(
            xid,
            "insert into people (id, name, note) values (1, 'alice', 'alpha')",
        )
        .unwrap();
    harness.txns.commit(xid).unwrap();
    match harness
        .execute(
            INVALID_TRANSACTION_ID,
            "explain (analyze, buffers) select name from people",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            let rendered = rows
                .into_iter()
                .map(|row| match &row[0] {
                    Value::Text(text) => text.clone(),
                    other => panic!("expected text explain line, got {:?}", other),
                })
                .collect::<Vec<_>>();
            assert!(rendered.iter().any(|line| line.contains("actual time=")));
            assert!(rendered.iter().any(|line| line.contains("Execution Time:")));
            assert!(rendered.iter().any(|line| line.contains("Buffers: shared")));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn explain_analyze_timing_off_still_reports_nonzero_actual_rows() {
    let mut harness = SeededSqlHarness::new("explain_analyze_timing_off_rows", catalog());
    let xid = harness.txns.begin();
    harness
        .execute(
            xid,
            "insert into people (id, name, note) values (1, 'alice', 'alpha')",
        )
        .unwrap();
    harness.txns.commit(xid).unwrap();
    match harness
        .execute(
            INVALID_TRANSACTION_ID,
            "explain (analyze, timing off) select name from people order by name",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            let rendered = rows
                .into_iter()
                .map(|row| match &row[0] {
                    Value::Text(text) => text.clone(),
                    other => panic!("expected text explain line, got {:?}", other),
                })
                .collect::<Vec<_>>();
            let plan_lines = rendered
                .iter()
                .filter(|line| line.contains("actual time="))
                .collect::<Vec<_>>();
            assert!(
                !plan_lines.is_empty(),
                "expected explain analyze plan lines"
            );
            assert!(
                plan_lines.iter().all(|line| !line.contains("rows=0.00")),
                "expected nonzero actual rows for populated plan nodes, got {plan_lines:?}"
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn explain_analyze_reports_single_loop_for_simple_scan_and_sort() {
    let mut harness = SeededSqlHarness::new("explain_analyze_simple_loops", catalog());
    let xid = harness.txns.begin();
    harness
        .execute(
            xid,
            "insert into people (id, name, note) values
         (1, 'alice', 'alpha'),
         (2, 'bob', null),
         (3, 'carol', 'storage')",
        )
        .unwrap();
    harness.txns.commit(xid).unwrap();
    match harness
        .execute(
            INVALID_TRANSACTION_ID,
            "explain (analyze, buffers) select name from people where id >= 1 order by name",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            let rendered = rows
                .into_iter()
                .map(|row| match &row[0] {
                    Value::Text(text) => text.clone(),
                    other => panic!("expected text explain line, got {:?}", other),
                })
                .collect::<Vec<_>>();
            let plan_lines = rendered
                .iter()
                .filter(|line| line.contains("actual time="))
                .collect::<Vec<_>>();
            assert!(
                plan_lines
                    .iter()
                    .any(|line| line.contains("Sort") && line.contains("loops=1")),
                "expected Sort loops=1, got {plan_lines:?}"
            );
            assert!(
                plan_lines
                    .iter()
                    .any(|line| line.contains("Seq Scan") && line.contains("loops=1")),
                "expected Seq Scan loops=1, got {plan_lines:?}"
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn explain_hash_join_conditions_render_readably() {
    let base = temp_dir("explain_hash_join_rendering");
    let db = Database::open(&base, 16).unwrap();
    db.execute(1, "create table customers (customer_id int4, name text)")
        .unwrap();
    db.execute(
        1,
        "create table orders (order_id int4, customer_id int4, total int4)",
    )
    .unwrap();
    db.execute(
        1,
        "insert into customers values (1, 'ada'), (2, 'ben'), (3, 'cora')",
    )
    .unwrap();
    db.execute(
        1,
        "insert into orders values (101, 1, 44), (102, 1, 65), (103, 3, 27), (104, 2, 18)",
    )
    .unwrap();

    match db
        .execute(
            1,
            "explain (analyze, buffers)
             select c.name, o.order_id, o.total
             from customers c
             join orders o on o.customer_id = c.customer_id
             where o.total >= 25
             order by o.order_id",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            let rendered = rows
                .into_iter()
                .map(|row| match &row[0] {
                    Value::Text(text) => text.clone(),
                    other => panic!("expected text explain line, got {:?}", other),
                })
                .collect::<Vec<_>>();
            assert!(
                rendered
                    .iter()
                    .any(|line| line.contains("Hash Cond: (customer_id = customer_id)")),
                "expected readable hash condition, got {rendered:?}"
            );
            assert!(
                rendered.iter().all(|line| !line.contains("Op(OpExpr")),
                "expected explain output without debug op expression formatting, got {rendered:?}"
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn explain_indents_child_plan_nodes() {
    let base = temp_dir("explain_indent_children");
    let db = Database::open(&base, 16).unwrap();
    db.execute(1, "create table people_indent (id int4, name text)")
        .unwrap();
    db.execute(
        1,
        "insert into people_indent values (2, 'bob'), (1, 'alice'), (3, 'carol')",
    )
    .unwrap();

    match db
        .execute(
            1,
            "explain (analyze, buffers)
             select name
             from people_indent
             order by name",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            let rendered = rows
                .into_iter()
                .map(|row| match &row[0] {
                    Value::Text(text) => text.clone(),
                    other => panic!("expected text explain line, got {:?}", other),
                })
                .collect::<Vec<_>>();
            assert!(
                rendered.iter().any(|line| line.starts_with("Projection  ")),
                "expected top-level projection line, got {rendered:?}"
            );
            assert!(
                rendered.iter().any(|line| line.starts_with("  Sort")),
                "expected indented sort child line, got {rendered:?}"
            );
            assert!(
                rendered.iter().any(|line| line.starts_with("    Seq Scan")),
                "expected doubly indented seq scan child line, got {rendered:?}"
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn inner_join_returns_matching_rows() {
    let base = temp_dir("join_sql");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let pool = test_pool_with_pets(&base);
    let xid = txns.begin();
    for row in [
        tuple(1, "alice", Some("alpha")),
        tuple(2, "bob", None),
        tuple(3, "carol", Some("storage")),
    ] {
        let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &row).unwrap();
        heap_flush(&*pool, 1, rel(), tid.block_number).unwrap();
    }
    for row in [pet_tuple(10, "Kitchen", 2), pet_tuple(11, "Mocha", 3)] {
        let tid = heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &row).unwrap();
        heap_flush(&*pool, 1, pets_rel(), tid.block_number).unwrap();
    }
    txns.commit(xid).unwrap();
    match run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select people.name, pets.name from people join pets on people.id = pets.owner_id",
        catalog_with_pets(),
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Text("bob".into()), Value::Text("Kitchen".into())],
                    vec![Value::Text("carol".into()), Value::Text("Mocha".into())]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn cross_join_returns_cartesian_product() {
    let base = temp_dir("cross_join_sql");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let pool = test_pool_with_pets(&base);
    let xid = txns.begin();
    for row in [tuple(1, "alice", Some("alpha")), tuple(2, "bob", None)] {
        let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &row).unwrap();
        heap_flush(&*pool, 1, rel(), tid.block_number).unwrap();
    }
    for row in [pet_tuple(10, "Kitchen", 2), pet_tuple(11, "Mocha", 3)] {
        let tid = heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &row).unwrap();
        heap_flush(&*pool, 1, pets_rel(), tid.block_number).unwrap();
    }
    txns.commit(xid).unwrap();
    match run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select people.name, pets.name from people, pets order by pets.name, people.name",
        catalog_with_pets(),
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Text("alice".into()), Value::Text("Kitchen".into())],
                    vec![Value::Text("bob".into()), Value::Text("Kitchen".into())],
                    vec![Value::Text("alice".into()), Value::Text("Mocha".into())],
                    vec![Value::Text("bob".into()), Value::Text("Mocha".into())]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn cross_join_where_clause_can_use_addition() {
    let base = temp_dir("cross_join_addition_sql");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let pool = test_pool_with_pets(&base);
    let xid = txns.begin();
    for row in [tuple(1, "alice", Some("alpha")), tuple(2, "bob", None)] {
        let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &row).unwrap();
        heap_flush(&*pool, 1, rel(), tid.block_number).unwrap();
    }
    for row in [pet_tuple(10, "Kitchen", 1), pet_tuple(11, "Mocha", 2)] {
        let tid = heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &row).unwrap();
        heap_flush(&*pool, 1, pets_rel(), tid.block_number).unwrap();
    }
    txns.commit(xid).unwrap();
    match run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select people.name, pets.name from people, pets where pets.owner_id + 1 = people.id",
        catalog_with_pets(),
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("bob".into()),
                    Value::Text("Kitchen".into())
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn count_star_without_group_by() {
    let base = temp_dir("count_star");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'c')").unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select count(*) from people",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["count"]);
            assert_eq!(rows, vec![vec![Value::Int64(3)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn count_star_on_empty_table() {
    let base = temp_dir("count_star_empty");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select count(*) from people",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(0)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn aggregate_filter_clause_counts_matching_rows() {
    let base = temp_dir("aggregate_filter_clause");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'c')").unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select count(*) filter (where note is not null) from people",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn group_by_with_count() {
    let base = temp_dir("group_by_count");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'a'), (3, 'carol', 'b')").unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select note, count(*) from people group by note order by note",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["note", "count"]);
            assert_eq!(
                rows,
                vec![
                    vec![Value::Text("a".into()), Value::Int64(2)],
                    vec![Value::Text("b".into()), Value::Int64(1)]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn sum_avg_min_max_aggregates() {
    let base = temp_dir("sum_avg_min_max");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(&base, &txns, xid, "insert into people (id, name, note) values (10, 'alice', 'a'), (20, 'bob', 'b'), (30, 'carol', 'c')").unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select sum(id), avg(id), min(id), max(id) from people",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["sum", "avg", "min", "max"]);
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int64(60),
                    Value::Numeric("20".into()),
                    Value::Int32(10),
                    Value::Int32(30)
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn any_value_over_values_skips_null_type_bias() {
    let base = temp_dir("any_value_values_null");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select any_value(v) from (values (null), (1), (2)) as v(v)",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["any_value"]);
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn variance_and_stddev_pop_samp_single_row_match_pg() {
    let base = temp_dir("variance_stddev_single_row");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select var_pop(1.0::float8), var_samp(1.0::float8), stddev_pop(1.0::float8), stddev_samp(1.0::float8)",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(
                column_names,
                vec!["var_pop", "var_samp", "stddev_pop", "stddev_samp"]
            );
            assert_eq!(
                rows,
                vec![vec![
                    Value::Float64(0.0),
                    Value::Null,
                    Value::Float64(0.0),
                    Value::Null
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn variance_and_stddev_aliases_use_sample_semantics() {
    let base = temp_dir("variance_stddev_aliases");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select variance(1.0::float8), stddev(1.0::float8)",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["variance", "stddev"]);
            assert_eq!(rows, vec![vec![Value::Null, Value::Null]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn bool_and_every_and_bool_or_match_pg_null_semantics() {
    let base = temp_dir("bool_aggs");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select bool_and(v), every(v), bool_or(v) from (values (true), (null), (false)) as t(v)",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["bool_and", "every", "bool_or"]);
            assert_eq!(
                rows,
                vec![vec![
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(true)
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select bool_and(v), bool_or(v) from (values (null), (null)) as t(v)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Null, Value::Null]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn regression_aggregates_match_pg_formulas() {
    let base = temp_dir("regr_aggs");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select regr_count(y, x), regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x), \
         regr_avgx(y, x), regr_avgy(y, x), regr_r2(y, x), regr_slope(y, x), \
         regr_intercept(y, x), covar_pop(y, x), covar_samp(y, x), corr(y, x) \
         from (values (2.0::float8, 1.0::float8), (4.0::float8, 2.0::float8), \
         (6.0::float8, 3.0::float8), (null::float8, 4.0::float8)) as t(y, x)",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(
                column_names,
                vec![
                    "regr_count",
                    "regr_sxx",
                    "regr_syy",
                    "regr_sxy",
                    "regr_avgx",
                    "regr_avgy",
                    "regr_r2",
                    "regr_slope",
                    "regr_intercept",
                    "covar_pop",
                    "covar_samp",
                    "corr",
                ]
            );
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int64(3),
                    Value::Float64(2.0),
                    Value::Float64(8.0),
                    Value::Float64(4.0),
                    Value::Float64(2.0),
                    Value::Float64(4.0),
                    Value::Float64(1.0),
                    Value::Float64(2.0),
                    Value::Float64(0.0),
                    Value::Float64(4.0 / 3.0),
                    Value::Float64(2.0),
                    Value::Float64(1.0),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn covariance_single_row_pg_edge_cases_match() {
    let base = temp_dir("covar_single_row");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select covar_pop(1::float8, 2::float8), covar_samp(3::float8, 4::float8), \
         covar_pop(1::float8, 'inf'::float8), covar_pop(1::float8, 'nan'::float8)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Float64(0.0));
            assert_eq!(rows[0][1], Value::Null);
            assert!(matches!(rows[0][2], Value::Float64(v) if v.is_nan()));
            assert!(matches!(rows[0][3], Value::Float64(v) if v.is_nan()));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn string_agg_skips_null_values() {
    let base = temp_dir("string_agg_text");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'c')",
    )
    .unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select string_agg(note, ',') from people",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["string_agg"]);
            assert_eq!(rows, vec![vec![Value::Text("a,c".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn string_agg_supports_bytea_inputs() {
    let base = temp_dir("string_agg_bytea");
    let db = Database::open(&base, 16).unwrap();
    db.execute(
        1,
        "create table bytes_demo (payload bytea, delimiter bytea)",
    )
    .unwrap();
    db.execute(
        1,
        "insert into bytes_demo (payload, delimiter) values (E'\\\\001'::bytea, E'\\\\377'::bytea), (E'\\\\002'::bytea, E'\\\\377'::bytea)",
    )
    .unwrap();
    match db
        .execute(
            1,
            "select encode(agg_payload, 'hex')
             from (
                 select string_agg(payload, delimiter) as agg_payload
                 from bytes_demo
             ) agg_bytes",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("01ff02".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn string_agg_coerces_unknown_delimiter_for_bytea_inputs() {
    let base = temp_dir("string_agg_bytea_unknown_delimiter");
    let db = Database::open(&base, 16).unwrap();
    db.execute(1, "create table bytes_demo (payload bytea)")
        .unwrap();
    db.execute(
        1,
        "insert into bytes_demo (payload) values (decode('ff', 'hex')), (decode('aa', 'hex'))",
    )
    .unwrap();
    match db
        .execute(
            1,
            "select encode(agg_payload, 'hex')
             from (
                 select string_agg(payload, '') as agg_payload
                 from bytes_demo
             ) agg_bytes",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("ffaa".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn having_filters_groups() {
    let base = temp_dir("having_filter");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'a'), (3, 'carol', 'b')").unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select note, count(*) from people group by note having count(*) > 1",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("a".into()), Value::Int64(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn count_expr_skips_nulls() {
    let base = temp_dir("count_expr_nulls");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'c')").unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select count(note) from people",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn sum_of_all_nulls_returns_null() {
    let base = temp_dir("sum_all_nulls");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', null), (2, 'bob', null)",
    )
    .unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select min(note), max(note) from people",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Null, Value::Null]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn null_group_by_keys_are_grouped_together() {
    let base = temp_dir("null_group_keys");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', null), (2, 'bob', 'a'), (3, 'carol', null), (4, 'dave', 'a')").unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select note, count(*) from people group by note order by note",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0], vec![Value::Text("a".into()), Value::Int64(2)]);
            assert_eq!(rows[1], vec![Value::Null, Value::Int64(2)]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn sum_and_avg_skip_nulls() {
    let base = temp_dir("sum_avg_skip_nulls");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(&base, &txns, xid, "insert into people (id, name, note) values (10, 'alice', 'a'), (20, 'bob', null), (30, 'carol', 'c')").unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select count(*), count(note), sum(id), avg(id), min(id), max(id) from people",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(
                column_names,
                vec!["count", "count", "sum", "avg", "min", "max"]
            );
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int64(3),
                    Value::Int64(2),
                    Value::Int64(60),
                    Value::Numeric("20".into()),
                    Value::Int32(10),
                    Value::Int32(30)
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

// regex (~) operator tests
#[test]
fn count_distinct_counts_unique_values() {
    let base = temp_dir("count_distinct");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'a'), (3, 'carol', 'b'), (4, 'dave', 'b'), (5, 'eve', 'c')").unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select count(distinct note) from people",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(3)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn count_distinct_skips_nulls() {
    let base = temp_dir("count_distinct_nulls");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'a'), (4, 'dave', null)").unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select count(distinct note) from people",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn count_distinct_with_group_by() {
    let base = temp_dir("count_distinct_group");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'x'), (2, 'alice', 'x'), (3, 'alice', 'y'), (4, 'bob', 'x'), (5, 'bob', 'x')").unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select name, count(distinct note) from people group by name order by name",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Text("alice".into()), Value::Int64(2)],
                    vec![Value::Text("bob".into()), Value::Int64(1)]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn sum_distinct_with_group_by() {
    let base = temp_dir("sum_distinct_group");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select grp, sum(distinct val) from (values ('a', 1), ('a', 1), ('a', 2), ('b', 2), ('b', 2), ('b', 3)) t(grp, val) group by grp order by grp",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Text("a".into()), Value::Int64(3)],
                    vec![Value::Text("b".into()), Value::Int64(5)],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn generate_series_basic() {
    let base = temp_dir("gen_series_basic");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from generate_series(1, 5)",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["generate_series"]);
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1)],
                    vec![Value::Int32(2)],
                    vec![Value::Int32(3)],
                    vec![Value::Int32(4)],
                    vec![Value::Int32(5)]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn generate_series_with_step() {
    let base = temp_dir("gen_series_step");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from generate_series(0, 10, 3)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(0)],
                    vec![Value::Int32(3)],
                    vec![Value::Int32(6)],
                    vec![Value::Int32(9)]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn generate_series_supports_int8_ranges() {
    let base = temp_dir("generate_series_int8");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select * from generate_series(4567890123456789::int8, 4567890123456793::int8, 2::int8)",
            )
            .unwrap(),
            vec![
                vec![Value::Int64(4_567_890_123_456_789)],
                vec![Value::Int64(4_567_890_123_456_791)],
                vec![Value::Int64(4_567_890_123_456_793)],
            ],
        );
}

#[test]
fn cast_int8_to_oid_reports_range_error() {
    let err = expr_casts::cast_value(
        Value::Int64(-1),
        crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Oid),
    )
    .unwrap_err();
    assert!(matches!(err, ExecError::OidOutOfRange));
}

#[test]
fn oid_text_input_wraps_negative_values_and_orders_unsigned() {
    let wrapped = expr_casts::cast_value(
        Value::Text("-1040".into()),
        crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Oid),
    )
    .unwrap();
    let small = expr_casts::cast_value(
        Value::Text("1234".into()),
        crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Oid),
    )
    .unwrap();

    assert_eq!(wrapped, Value::Int64(4_294_966_256));
    assert_eq!(small, Value::Int64(1234));
    assert_eq!(
        crate::backend::executor::expr_ops::compare_order_values(
            &small, &wrapped, None, None, false,
        )
        .unwrap(),
        std::cmp::Ordering::Less
    );
}

#[test]
fn oid_comparisons_bind_and_execute_with_unsigned_semantics() {
    let base = temp_dir("oid_comparisons_unsigned");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();

    run_sql_with_catalog(
        &base,
        &txns,
        xid,
        "insert into oid_tbl (f1) values ('1234'), ('-1040'), ('1235')",
        oid_catalog("oid_tbl"),
    )
    .unwrap();
    txns.commit(xid).unwrap();

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select f1 from oid_tbl where f1 >= 1234 order by f1",
            oid_catalog("oid_tbl"),
        )
        .unwrap(),
        vec![
            vec![Value::Int64(1234)],
            vec![Value::Int64(1235)],
            vec![Value::Int64(4_294_966_256)],
        ],
    );
}

#[test]
fn pg_rust_internal_binary_coercible_reports_builtin_compatibility() {
    let base = temp_dir("pg_rust_internal_binary_coercible");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            xid,
            "select pg_rust_internal_binary_coercible(1043::oid, 25::oid), pg_rust_internal_binary_coercible(1042::oid, 25::oid), pg_rust_internal_binary_coercible(23::oid, 25::oid)",
            catalog(),
        )
        .unwrap(),
        vec![vec![Value::Bool(true), Value::Bool(false), Value::Bool(false)]],
    );
}

#[test]
fn pg_rust_internal_binary_coercible_matches_opr_sanity_cast_check() {
    let base = temp_dir("pg_rust_internal_binary_coercible_opr_sanity");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            xid,
            "select c.oid \
             from pg_cast c, pg_proc p \
             where c.castfunc = p.oid \
               and c.castsource = 1042::oid \
               and c.casttarget = 25::oid \
               and (p.pronargs < 1 or p.pronargs > 3 \
                    or pg_rust_internal_binary_coercible(c.castsource, c.casttarget) \
                    or not (c.castsource = 1042::oid and p.oid = 6237::oid) \
                    or not pg_rust_internal_binary_coercible(p.prorettype, c.casttarget))",
            catalog(),
        )
        .unwrap(),
        vec![],
    );
}

#[test]
fn sub_values_supports_date_difference() {
    use crate::include::nodes::datetime::DateADT;

    assert_eq!(
        crate::backend::executor::expr_ops::sub_values(
            Value::Date(DateADT(10)),
            Value::Date(DateADT(3))
        )
        .unwrap(),
        Value::Int32(7)
    );
}

#[test]
fn select_date_subtraction_returns_day_count() {
    let base = temp_dir("select_date_subtraction");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select date '2000-01-02' - date '2000-01-01'",
        )
        .unwrap(),
        vec![vec![Value::Int32(1)]],
    );
}

#[test]
fn select_date_part_extracts_date_fields() {
    let base = temp_dir("select_date_part_fields");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select date_part('week', date '2020-08-11'), date_part('isodow', date '2020-08-16'), date_part('year', date '2020-08-11 BC')",
        )
        .unwrap(),
        vec![vec![
            Value::Float64(33.0),
            Value::Float64(7.0),
            Value::Float64(-2020.0),
        ]],
    );
}

#[test]
fn select_date_part_handles_infinity() {
    let base = temp_dir("select_date_part_infinity");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select date_part('day', date 'infinity'), date_part('epoch', date 'infinity')",
        )
        .unwrap(),
        vec![vec![Value::Null, Value::Float64(f64::INFINITY)]],
    );
}

#[test]
fn select_extract_uses_date_part_runtime() {
    let base = temp_dir("select_extract_date_part");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select extract(week from date '2020-08-11'), extract(isodow from date '2020-08-16')",
        )
        .unwrap(),
        vec![vec![Value::Float64(33.0), Value::Float64(7.0)]],
    );
}

#[test]
fn select_extract_uses_extract_as_default_column_name() {
    let base = temp_dir("select_extract_column_name");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select extract(day from date '2020-08-11')",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["extract"]);
            assert_eq!(rows, vec![vec![Value::Float64(11.0)]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn select_extract_rejects_unsupported_date_units_with_postgres_diagnostic() {
    let base = temp_dir("select_extract_unsupported_date_unit");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select extract(microseconds from date '2020-08-11')",
    )
    .unwrap_err()
    {
        ExecError::DetailedError {
            message, sqlstate, ..
        } => {
            assert_eq!(message, "unit \"microseconds\" not supported for type date");
            assert_eq!(sqlstate, "0A000");
        }
        other => panic!("expected detailed error, got {other:?}"),
    }
}

#[test]
fn select_extract_rejects_unrecognized_date_units_with_postgres_diagnostic() {
    let base = temp_dir("select_extract_unrecognized_date_unit");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select extract(microsec from date 'infinity')",
    )
    .unwrap_err()
    {
        ExecError::DetailedError {
            message, sqlstate, ..
        } => {
            assert_eq!(message, "unit \"microsec\" not recognized for type date");
            assert_eq!(sqlstate, "22023");
        }
        other => panic!("expected detailed error, got {other:?}"),
    }
}

#[test]
fn select_date_trunc_on_date_values() {
    let base = temp_dir("select_date_trunc_date");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select date_trunc('century', date '2004-08-10'), date_trunc('decade', date '0002-12-31 BC')",
    )
    .unwrap(),
        vec![vec![
            Value::Timestamp(TimestampADT(
                i64::from(crate::backend::utils::time::datetime::days_from_ymd(2001, 1, 1).unwrap())
                    * crate::include::nodes::datetime::USECS_PER_DAY,
            )),
            Value::Timestamp(TimestampADT(
                i64::from(crate::backend::utils::time::datetime::days_from_ymd(-10, 1, 1).unwrap())
                    * crate::include::nodes::datetime::USECS_PER_DAY,
            )),
        ]],
    );
}

#[test]
fn select_isfinite_and_make_date_for_date() {
    let base = temp_dir("select_isfinite_make_date");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select isfinite(date 'infinity'), isfinite(date 'today'), make_date(-44, 3, 15)",
        )
        .unwrap(),
        vec![vec![
            Value::Bool(false),
            Value::Bool(true),
            Value::Date(DateADT(
                crate::backend::utils::time::datetime::days_from_ymd(-43, 3, 15).unwrap(),
            )),
        ]],
    );
}

#[test]
fn pg_input_error_info_supports_oidvector_tokens() {
    let valid = expr_casts::soft_input_error_info(" 1 2  4 ", "oidvector").unwrap();
    assert!(valid.is_none());

    let invalid = expr_casts::soft_input_error_info("01 01XYZ", "oidvector")
        .unwrap()
        .expect("expected invalid oidvector input");
    assert_eq!(
        invalid.message,
        "invalid input syntax for type oid: \"XYZ\""
    );
    assert_eq!(invalid.sqlstate, "22P02");

    let out_of_range = expr_casts::soft_input_error_info("01 9999999999", "oidvector")
        .unwrap()
        .expect("expected out of range oidvector input");
    assert_eq!(
        out_of_range.message,
        "value \"9999999999\" is out of range for type oid"
    );
    assert_eq!(out_of_range.sqlstate, "22003");
}

#[test]
fn pg_class_exposes_oid_column_through_normal_catalog_plan() {
    let base = temp_dir("pg_class_oid");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select oid::int8 from pg_class where relname = 'pg_class'",
        )
        .unwrap(),
        vec![vec![Value::Int64(1259)]],
    );
}

#[test]
fn pg_attribute_exposes_bootstrap_columns() {
    let base = temp_dir("pg_attribute_bootstrap");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let expected = crate::include::catalog::pg_class_desc()
        .columns
        .into_iter()
        .map(|column| vec![Value::Text(column.name.into())])
        .collect();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select attname from pg_attribute where attrelid = 1259 order by attnum",
        )
        .unwrap(),
        expected,
    );
}

#[test]
fn int8_bitwise_operators_execute() {
    let base = temp_dir("int8_bitwise");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select (123::int8 & 456::int8), (123::int8 | 456::int8), (123::int8 # 456::int8), (~123::int8)",
            )
            .unwrap(),
            vec![vec![
                Value::Int64(72),
                Value::Int64(507),
                Value::Int64(435),
                Value::Int64(-124),
            ]],
        );
}
#[test]
fn generate_series_negative_step() {
    let base = temp_dir("gen_series_neg");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from generate_series(5, 1, -1)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(5)],
                    vec![Value::Int32(4)],
                    vec![Value::Int32(3)],
                    vec![Value::Int32(2)],
                    vec![Value::Int32(1)]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn generate_series_empty() {
    let base = temp_dir("gen_series_empty");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from generate_series(1, 0)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, Vec::<Vec<Value>>::new());
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn generate_series_with_where() {
    let base = temp_dir("gen_series_where");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from generate_series(1, 10) where generate_series > 8",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(9)], vec![Value::Int32(10)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn select_array_literal_round_trips() {
    let base = temp_dir("array_literal_round_trip");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select ARRAY['a', 'b']::varchar[]",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Array(vec![
                    Value::Text("a".into()),
                    Value::Text("b".into())
                ])]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn multidimensional_array_text_input_round_trips() {
    let base = temp_dir("multidim_array_round_trip");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '{{1,2},{3,4}}'::int4[]",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Array(vec![
                    Value::Array(vec![Value::Int32(1), Value::Int32(2)]),
                    Value::Array(vec![Value::Int32(3), Value::Int32(4)]),
                ])]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn multidimensional_array_columns_round_trip_through_storage() {
    let base = temp_dir("multidim_array_storage_roundtrip");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let insert_xid = txns.begin();
    assert_eq!(
        run_sql_with_catalog(
            &base,
            &txns,
            insert_xid,
            "insert into t values ('{{{1,2},{3,4}}}'::int4[])",
            multidimensional_array_catalog(),
        )
        .unwrap(),
        StatementResult::AffectedRows(1)
    );
    txns.commit(insert_xid).unwrap();
    match run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select a from t",
        multidimensional_array_catalog(),
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::PgArray(
                    crate::include::nodes::datum::ArrayValue::from_dimensions(
                        vec![
                            crate::include::nodes::datum::ArrayDimension {
                                lower_bound: 1,
                                length: 1,
                            },
                            crate::include::nodes::datum::ArrayDimension {
                                lower_bound: 1,
                                length: 2,
                            },
                            crate::include::nodes::datum::ArrayDimension {
                                lower_bound: 1,
                                length: 2,
                            },
                        ],
                        vec![
                            Value::Int32(1),
                            Value::Int32(2),
                            Value::Int32(3),
                            Value::Int32(4),
                        ],
                    )
                    .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
                )]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn array_append_prepend_and_cat_match_postgres() {
    let base = temp_dir("array_append_prepend_cat");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select array_append(array[42], 6)",
        )
        .unwrap(),
        vec![vec![Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound: 1,
                length: 2,
            }],
            vec![Value::Int32(42), Value::Int32(6)],
        ))]],
    );

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select array_prepend(6, array[42])",
        )
        .unwrap(),
        vec![vec![Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound: 1,
                length: 2,
            }],
            vec![Value::Int32(6), Value::Int32(42)],
        ))]],
    );

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select array_cat(ARRAY[1,2], ARRAY[3,4])",
        )
        .unwrap(),
        vec![vec![Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound: 1,
                length: 4,
            }],
            vec![
                Value::Int32(1),
                Value::Int32(2),
                Value::Int32(3),
                Value::Int32(4),
            ],
        ))]],
    );

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select array_cat(ARRAY[1,2], ARRAY[[3,4],[5,6]])",
        )
        .unwrap(),
        vec![vec![Value::PgArray(ArrayValue::from_dimensions(
            vec![
                ArrayDimension {
                    lower_bound: 1,
                    length: 3,
                },
                ArrayDimension {
                    lower_bound: 1,
                    length: 2,
                },
            ],
            vec![
                Value::Int32(1),
                Value::Int32(2),
                Value::Int32(3),
                Value::Int32(4),
                Value::Int32(5),
                Value::Int32(6),
            ],
        ))]],
    );

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select array_cat(ARRAY[[3,4],[5,6]], ARRAY[1,2])",
        )
        .unwrap(),
        vec![vec![Value::PgArray(ArrayValue::from_dimensions(
            vec![
                ArrayDimension {
                    lower_bound: 1,
                    length: 3,
                },
                ArrayDimension {
                    lower_bound: 1,
                    length: 2,
                },
            ],
            vec![
                Value::Int32(3),
                Value::Int32(4),
                Value::Int32(5),
                Value::Int32(6),
                Value::Int32(1),
                Value::Int32(2),
            ],
        ))]],
    );
}

#[test]
fn array_concat_operator_preserves_multidimensional_shape() {
    let base = temp_dir("array_concat_operator_shape");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select ARRAY[[1,2],[3,4]] || ARRAY[5,6]",
        )
        .unwrap(),
        vec![vec![Value::PgArray(ArrayValue::from_dimensions(
            vec![
                ArrayDimension {
                    lower_bound: 1,
                    length: 3,
                },
                ArrayDimension {
                    lower_bound: 1,
                    length: 2,
                },
            ],
            vec![
                Value::Int32(1),
                Value::Int32(2),
                Value::Int32(3),
                Value::Int32(4),
                Value::Int32(5),
                Value::Int32(6),
            ],
        ))]],
    );
}

#[test]
fn implicit_row_constructor_works_in_array_position() {
    let base = temp_dir("implicit_row_array_position");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select array_position(ids, (1, 1)), array_positions(ids, (1, 1)) from (values (ARRAY[(0, 0), (1, 1)]), (ARRAY[(1, 1)])) as f(ids)",
        )
        .unwrap(),
        vec![
            vec![
                Value::Int32(2),
                Value::PgArray(ArrayValue::from_dimensions(
                    vec![ArrayDimension {
                        lower_bound: 1,
                        length: 1,
                    }],
                    vec![Value::Int32(2)],
                )),
            ],
            vec![
                Value::Int32(1),
                Value::PgArray(ArrayValue::from_dimensions(
                    vec![ArrayDimension {
                        lower_bound: 1,
                        length: 1,
                    }],
                    vec![Value::Int32(1)],
                )),
            ],
        ],
    );
}

#[test]
fn row_to_array_concat_operator_keeps_row_elements() {
    let base = temp_dir("row_to_array_concat");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select ((ROW(1,2) || array_agg(x))[1]).f1, ((ROW(1,2) || array_agg(x))[2]).f1, ((ROW(1,2) || array_agg(x))[3]).f2 from (values (ROW(3,4)), (ROW(5,6))) v(x)",
        )
        .unwrap(),
        vec![vec![Value::Int32(1), Value::Int32(3), Value::Int32(6)]],
    );
}

#[test]
fn composite_array_field_assignment_and_selection_work() {
    let db = Database::open(temp_dir("composite_array_field_assignment"), 16).unwrap();
    let mut session = Session::new(1);

    db.execute(1, "create type pair as (q1 int4, q2 int4)")
        .unwrap();
    db.execute(1, "create temp table t1 (f1 pair[])").unwrap();
    db.execute(1, "insert into t1 (f1[5].q1) values (42)")
        .unwrap();

    match session
        .execute(&db, "select (f1[5]).q1, (f1[5]).q2 from t1")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(42), Value::Null]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }

    db.execute(1, "update t1 set f1[5].q2 = 43").unwrap();

    match session
        .execute(&db, "select (f1[5]).q1, (f1[5]).q2 from t1")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(42), Value::Int32(43)]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn named_composite_array_field_selection_after_row_cast_works() {
    let db = Database::open(temp_dir("named_composite_array_field_selection"), 16).unwrap();
    let mut session = Session::new(1);

    db.execute(1, "create type textpair as (c1 text, c2 text)")
        .unwrap();
    db.execute(1, "create temp table dest (f1 textpair[])")
        .unwrap();
    db.execute(
        1,
        "insert into dest select array[row('left','right')::textpair]",
    )
    .unwrap();

    match session.execute(&db, "select (f1[1]).c2 from dest").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("right".into())]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn interval_array_literals_preserve_interval_array_values() {
    let base = temp_dir("interval_array_literals");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select '{0 second,1 hour 42 minutes 20 seconds}'::interval[]",
        )
        .unwrap(),
        vec![vec![Value::PgArray(
            ArrayValue::from_1d(vec![
                Value::Text("@ 0 secs".into()),
                Value::Text("@ 1 hour 42 mins 20 secs".into()),
            ])
            .with_element_type_oid(crate::include::catalog::INTERVAL_TYPE_OID),
        )]],
    );
}

#[test]
fn interval_text_cast_canonicalizes_interval_value() {
    let base = temp_dir("interval_text_cast");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select '1 day'::interval",
        )
        .unwrap(),
        vec![vec![Value::Text("@ 1 day".into())]],
    );
}

#[test]
fn interval_array_text_casts_render_postgres_interval_style() {
    let base = temp_dir("interval_array_text_casts");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select '{0 second,1 hour 42 minutes 20 seconds}'::interval[]::text, ('{0 second,1 hour 42 minutes 20 seconds}'::interval[])[1]::text",
        )
        .unwrap(),
        vec![vec![
            Value::Text("{\"@ 0\",\"@ 1 hour 42 mins 20 secs\"}".into()),
            Value::Text("@ 0 secs".into()),
        ]],
    );
}

#[test]
fn array_position_reports_multidimensional_search_error() {
    let base = temp_dir("array_position_multidimensional_error");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select array_position(ARRAY[[1,2],[3,4]], 3)",
    )
    .unwrap_err()
    {
        ExecError::DetailedError {
            message, sqlstate, ..
        } => {
            assert_eq!(
                message,
                "searching for elements in multidimensional arrays is not supported"
            );
            assert_eq!(sqlstate, "0A000");
        }
        other => panic!("expected detailed error, got {other:?}"),
    }

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select array_positions(ARRAY[[1,2],[3,4]], 4)",
    )
    .unwrap_err()
    {
        ExecError::DetailedError {
            message, sqlstate, ..
        } => {
            assert_eq!(
                message,
                "searching for elements in multidimensional arrays is not supported"
            );
            assert_eq!(sqlstate, "0A000");
        }
        other => panic!("expected detailed error, got {other:?}"),
    }
}

#[test]
fn array_subscript_select_and_update_work() {
    let base = temp_dir("array_subscript_update");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    assert_eq!(
        run_sql_with_catalog(
            &base,
            &txns,
            xid,
            "insert into t values (ARRAY[1,2,3], ARRAY[4,5,6])",
            array_subscript_catalog(),
        )
        .unwrap(),
        StatementResult::AffectedRows(1)
    );
    txns.commit(xid).unwrap();

    match run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select a[2], b[1:2] from t",
        array_subscript_catalog(),
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int32(2),
                    Value::PgArray(crate::include::nodes::datum::ArrayValue::from_dimensions(
                        vec![crate::include::nodes::datum::ArrayDimension {
                            lower_bound: 1,
                            length: 2,
                        }],
                        vec![Value::Int32(4), Value::Int32(5)],
                    )),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    let update_xid = txns.begin();
    assert_eq!(
        run_sql_with_catalog(
            &base,
            &txns,
            update_xid,
            "update t set a[2] = 22, b[2:3] = ARRAY[50,60]",
            array_subscript_catalog(),
        )
        .unwrap(),
        StatementResult::AffectedRows(1)
    );
    txns.commit(update_xid).unwrap();

    match run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select a, b from t",
        array_subscript_catalog(),
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::PgArray(
                        crate::include::nodes::datum::ArrayValue::from_dimensions(
                            vec![crate::include::nodes::datum::ArrayDimension {
                                lower_bound: 1,
                                length: 3,
                            }],
                            vec![Value::Int32(1), Value::Int32(22), Value::Int32(3)],
                        )
                        .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID)
                    ),
                    Value::PgArray(
                        crate::include::nodes::datum::ArrayValue::from_dimensions(
                            vec![crate::include::nodes::datum::ArrayDimension {
                                lower_bound: 1,
                                length: 3,
                            }],
                            vec![Value::Int32(4), Value::Int32(50), Value::Int32(60)],
                        )
                        .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID)
                    ),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn array_assignment_coerces_text_literals_using_target_type() {
    let base = temp_dir("array_assignment_text_literals");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    assert_eq!(
        run_sql_with_catalog(
            &base,
            &txns,
            xid,
            "insert into t (a) values ('{1,2,3}')",
            array_assignment_catalog(),
        )
        .unwrap(),
        StatementResult::AffectedRows(1)
    );
    txns.commit(xid).unwrap();

    match run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select a from t",
        array_assignment_catalog(),
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::PgArray(
                    crate::include::nodes::datum::ArrayValue::from_1d(vec![
                        Value::Int32(1),
                        Value::Int32(2),
                        Value::Int32(3),
                    ])
                    .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
                )]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    let err = run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "insert into t (f) values ('{\"too long\"}')",
        array_assignment_catalog(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::StringDataRightTruncation { ref ty } if ty == "character(5)"
    ));
}

#[test]
fn array_slice_assignment_multidimensional_cases_match_postgres() {
    let base = temp_dir("array_slice_assignment_multidimensional");
    let mut txns = TransactionManager::new_durable(&base).unwrap();

    let insert_xid = txns.begin();
    assert_eq!(
        run_sql_with_catalog(
            &base,
            &txns,
            insert_xid,
            "insert into t values ('{1,2,3,4,5}'::int[], '{{1,2,3},{4,5,6},{7,8,9}}'::int[])",
            array_subscript_catalog(),
        )
        .unwrap(),
        StatementResult::AffectedRows(1)
    );
    txns.commit(insert_xid).unwrap();

    let update_xid = txns.begin();
    assert_eq!(
        run_sql_with_catalog(
            &base,
            &txns,
            update_xid,
            "update t set a[:3] = '{11,12,13}', b[:2][:2] = '{{11,12},{14,15}}'",
            array_subscript_catalog(),
        )
        .unwrap(),
        StatementResult::AffectedRows(1)
    );
    txns.commit(update_xid).unwrap();

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select a, b from t",
            array_subscript_catalog(),
        )
        .unwrap(),
        vec![vec![
            Value::PgArray(
                ArrayValue::from_dimensions(
                    vec![ArrayDimension {
                        lower_bound: 1,
                        length: 5,
                    }],
                    vec![
                        Value::Int32(11),
                        Value::Int32(12),
                        Value::Int32(13),
                        Value::Int32(4),
                        Value::Int32(5),
                    ],
                )
                .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
            ),
            Value::PgArray(
                ArrayValue::from_dimensions(
                    vec![
                        ArrayDimension {
                            lower_bound: 1,
                            length: 3,
                        },
                        ArrayDimension {
                            lower_bound: 1,
                            length: 3,
                        },
                    ],
                    vec![
                        Value::Int32(11),
                        Value::Int32(12),
                        Value::Int32(3),
                        Value::Int32(14),
                        Value::Int32(15),
                        Value::Int32(6),
                        Value::Int32(7),
                        Value::Int32(8),
                        Value::Int32(9),
                    ],
                )
                .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
            ),
        ]],
    );

    let second_update_xid = txns.begin();
    assert_eq!(
        run_sql_with_catalog(
            &base,
            &txns,
            second_update_xid,
            "update t set a[3:] = '{23,24,25}', b[2:][2:] = '{{25,26},{28,29}}'",
            array_subscript_catalog(),
        )
        .unwrap(),
        StatementResult::AffectedRows(1)
    );
    txns.commit(second_update_xid).unwrap();

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select a, b from t",
            array_subscript_catalog(),
        )
        .unwrap(),
        vec![vec![
            Value::PgArray(
                ArrayValue::from_dimensions(
                    vec![ArrayDimension {
                        lower_bound: 1,
                        length: 5,
                    }],
                    vec![
                        Value::Int32(11),
                        Value::Int32(12),
                        Value::Int32(23),
                        Value::Int32(24),
                        Value::Int32(25),
                    ],
                )
                .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
            ),
            Value::PgArray(
                ArrayValue::from_dimensions(
                    vec![
                        ArrayDimension {
                            lower_bound: 1,
                            length: 3,
                        },
                        ArrayDimension {
                            lower_bound: 1,
                            length: 3,
                        },
                    ],
                    vec![
                        Value::Int32(11),
                        Value::Int32(12),
                        Value::Int32(3),
                        Value::Int32(14),
                        Value::Int32(25),
                        Value::Int32(26),
                        Value::Int32(7),
                        Value::Int32(28),
                        Value::Int32(29),
                    ],
                )
                .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
            ),
        ]],
    );
}

#[test]
fn array_slice_assignment_uses_existing_bounds_for_omitted_limits() {
    let base = temp_dir("array_slice_assignment_existing_bounds");
    let mut txns = TransactionManager::new_durable(&base).unwrap();

    let insert_xid = txns.begin();
    assert_eq!(
        run_sql_with_catalog(
            &base,
            &txns,
            insert_xid,
            "insert into t values ('[0:4]={1,2,3,4,5}', '[0:2][0:2]={{1,2,3},{4,5,6},{7,8,9}}')",
            array_subscript_catalog(),
        )
        .unwrap(),
        StatementResult::AffectedRows(1)
    );
    txns.commit(insert_xid).unwrap();

    let update_xid = txns.begin();
    assert_eq!(
        run_sql_with_catalog(
            &base,
            &txns,
            update_xid,
            "update t set a[3:] = '{23,24,25}', b[2:][2:] = '{{25,26},{28,29}}'",
            array_subscript_catalog(),
        )
        .unwrap(),
        StatementResult::AffectedRows(1)
    );
    txns.commit(update_xid).unwrap();

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select a, b from t",
            array_subscript_catalog(),
        )
        .unwrap(),
        vec![vec![
            Value::PgArray(
                ArrayValue::from_dimensions(
                    vec![ArrayDimension {
                        lower_bound: 0,
                        length: 5,
                    }],
                    vec![
                        Value::Int32(1),
                        Value::Int32(2),
                        Value::Int32(3),
                        Value::Int32(23),
                        Value::Int32(24),
                    ],
                )
                .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
            ),
            Value::PgArray(
                ArrayValue::from_dimensions(
                    vec![
                        ArrayDimension {
                            lower_bound: 0,
                            length: 3,
                        },
                        ArrayDimension {
                            lower_bound: 0,
                            length: 3,
                        },
                    ],
                    vec![
                        Value::Int32(1),
                        Value::Int32(2),
                        Value::Int32(3),
                        Value::Int32(4),
                        Value::Int32(5),
                        Value::Int32(6),
                        Value::Int32(7),
                        Value::Int32(8),
                        Value::Int32(25),
                    ],
                )
                .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
            ),
        ]],
    );
}

#[test]
fn array_slice_assignment_rejects_too_small_sources() {
    let base = temp_dir("array_slice_assignment_source_too_small");
    let mut txns = TransactionManager::new_durable(&base).unwrap();

    let insert_xid = txns.begin();
    assert_eq!(
        run_sql_with_catalog(
            &base,
            &txns,
            insert_xid,
            "insert into t values ('{1,2,3,4,5}'::int[], null)",
            array_subscript_catalog(),
        )
        .unwrap(),
        StatementResult::AffectedRows(1)
    );
    txns.commit(insert_xid).unwrap();

    let update_xid = txns.begin();
    let err = run_sql_with_catalog(
        &base,
        &txns,
        update_xid,
        "update t set a[:] = '{23,24,25}'",
        array_subscript_catalog(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, sqlstate, .. }
            if message == "source array too small" && sqlstate == "2202E"
    ));
}

#[test]
fn array_slice_assignment_requires_full_bounds_for_null_arrays() {
    let base = temp_dir("array_slice_assignment_null_array_bounds");
    let mut txns = TransactionManager::new_durable(&base).unwrap();

    let insert_xid = txns.begin();
    assert_eq!(
        run_sql_with_catalog(
            &base,
            &txns,
            insert_xid,
            "insert into t values (null, null)",
            array_subscript_catalog(),
        )
        .unwrap(),
        StatementResult::AffectedRows(1)
    );
    txns.commit(insert_xid).unwrap();

    let update_xid = txns.begin();
    let err = run_sql_with_catalog(
        &base,
        &txns,
        update_xid,
        "update t set a[:] = '{11,12,13,14,15}'",
        array_subscript_catalog(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError {
            message,
            detail,
            sqlstate,
            ..
        }
            if message == "array slice subscript must provide both boundaries"
                && detail
                    == Some("When assigning to a slice of an empty array value, slice boundaries must be fully specified.".into())
                && sqlstate == "2202E"
    ));
}

#[test]
fn array_assignment_overflow_reports_program_limit() {
    let base = temp_dir("array_assignment_overflow_limit");
    let mut txns = TransactionManager::new_durable(&base).unwrap();

    let xid = txns.begin();
    assert_eq!(
        run_sql_with_catalog(
            &base,
            &txns,
            xid,
            "insert into t values ('[-2147483648:-2147483647]={1,2}', null)",
            array_subscript_catalog(),
        )
        .unwrap(),
        StatementResult::AffectedRows(1)
    );
    txns.commit(xid).unwrap();

    let update_xid = txns.begin();
    let err = run_sql_with_catalog(
        &base,
        &txns,
        update_xid,
        "update t set a[2147483647] = 42",
        array_subscript_catalog(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, sqlstate, .. }
            if message == "array size exceeds the maximum allowed" && sqlstate == "54000"
    ));
}

#[test]
fn array_slice_assignment_overflow_reports_program_limit() {
    let base = temp_dir("array_slice_assignment_overflow_limit");
    let mut txns = TransactionManager::new_durable(&base).unwrap();

    let xid = txns.begin();
    assert_eq!(
        run_sql_with_catalog(
            &base,
            &txns,
            xid,
            "insert into t values ('[-2147483648:-2147483647]={1,2}', null)",
            array_subscript_catalog(),
        )
        .unwrap(),
        StatementResult::AffectedRows(1)
    );
    txns.commit(xid).unwrap();

    let update_xid = txns.begin();
    let err = run_sql_with_catalog(
        &base,
        &txns,
        update_xid,
        "update t set a[2147483646:2147483647] = array[4,2]",
        array_subscript_catalog(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, sqlstate, .. }
            if message == "array size exceeds the maximum allowed" && sqlstate == "54000"
    ));
}

#[test]
fn array_slice_assignment_three_dimensional_serial_updates_match_postgres() {
    let base = temp_dir("array_slice_assignment_three_dimensional");
    let mut txns = TransactionManager::new_durable(&base).unwrap();

    let insert_xid = txns.begin();
    assert_eq!(
        run_sql_with_catalog(
            &base,
            &txns,
            insert_xid,
            "insert into t values ('{{{0,0},{1,2}}}'::int[])",
            multidimensional_array_catalog(),
        )
        .unwrap(),
        StatementResult::AffectedRows(1)
    );
    txns.commit(insert_xid).unwrap();

    let update_xid = txns.begin();
    assert_eq!(
        run_sql_with_catalog(
            &base,
            &txns,
            update_xid,
            "update t set a[1:1][1:1][1:2] = '{113,117}', a[1:1][1:2][2:2] = '{142,147}'",
            multidimensional_array_catalog(),
        )
        .unwrap(),
        StatementResult::AffectedRows(1)
    );
    txns.commit(update_xid).unwrap();

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select a from t",
            multidimensional_array_catalog(),
        )
        .unwrap(),
        vec![vec![Value::PgArray(
            ArrayValue::from_dimensions(
                vec![
                    ArrayDimension {
                        lower_bound: 1,
                        length: 1,
                    },
                    ArrayDimension {
                        lower_bound: 1,
                        length: 2,
                    },
                    ArrayDimension {
                        lower_bound: 1,
                        length: 2,
                    },
                ],
                vec![
                    Value::Int32(113),
                    Value::Int32(142),
                    Value::Int32(1),
                    Value::Int32(147),
                ],
            )
            .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
        )]],
    );
}

#[test]
fn array_slice_assignment_rejects_too_small_multidimensional_sources() {
    let base = temp_dir("array_slice_assignment_multidimensional_source_too_small");
    let mut txns = TransactionManager::new_durable(&base).unwrap();

    let insert_xid = txns.begin();
    assert_eq!(
        run_sql_with_catalog(
            &base,
            &txns,
            insert_xid,
            "insert into t values (null, '{{1,2,3},{4,5,6},{7,8,9}}'::int[])",
            array_subscript_catalog(),
        )
        .unwrap(),
        StatementResult::AffectedRows(1)
    );
    txns.commit(insert_xid).unwrap();

    let update_xid = txns.begin();
    let err = run_sql_with_catalog(
        &base,
        &txns,
        update_xid,
        "update t set b[1:2][1:2] = '{{11,12,13}}'",
        array_subscript_catalog(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, sqlstate, .. }
            if message == "source array too small" && sqlstate == "2202E"
    ));
}

#[test]
fn array_subscript_assignment_type_mismatch_uses_postgres_message() {
    let base = temp_dir("array_subscript_assignment_type_mismatch");
    let mut txns = TransactionManager::new_durable(&base).unwrap();

    let xid = txns.begin();
    let err = run_sql_with_catalog(
        &base,
        &txns,
        xid,
        "insert into t (b[2]) values(now())",
        array_subscript_catalog(),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ExecError::DetailedError {
            message,
            hint,
            sqlstate,
            ..
        }
            if message
                == "subscripted assignment to \"b\" requires type integer but expression is of type timestamp with time zone"
                && hint.as_deref()
                    == Some("You will need to rewrite or cast the expression.")
                && sqlstate == "42804"
    ));
}

#[test]
fn array_slice_assignment_type_mismatch_uses_postgres_message() {
    let base = temp_dir("array_slice_assignment_type_mismatch");
    let mut txns = TransactionManager::new_durable(&base).unwrap();

    let xid = txns.begin();
    let err = run_sql_with_catalog(
        &base,
        &txns,
        xid,
        "insert into t (b[1:2]) values(now())",
        array_subscript_catalog(),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ExecError::DetailedError {
            message,
            hint,
            sqlstate,
            ..
        }
            if message
                == "subscripted assignment to \"b\" requires type integer[] but expression is of type timestamp with time zone"
                && hint.as_deref()
                    == Some("You will need to rewrite or cast the expression.")
                && sqlstate == "42804"
    ));
}
#[test]
fn any_array_truth_table_and_overlap_work() {
    let base = temp_dir("array_any_overlap");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'b' = any(ARRAY['a', 'b']::varchar[]), 'z' = any(ARRAY['a', null]::varchar[]), ARRAY['a']::varchar[] && ARRAY['b', 'a']::varchar[]").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(true), Value::Null, Value::Bool(true)]]); } other => panic!("expected query result, got {:?}", other), }
}
#[test]
fn unnest_single_and_multi_arg_work() {
    let base = temp_dir("unnest_multi");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from unnest(ARRAY[1, 2], ARRAY['x']::varchar[]) as u(a, b)",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["a", "b"]);
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1), Value::Text("x".into())],
                    vec![Value::Int32(2), Value::Null]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn unnest_with_ordinality_aliases_and_counts_rows() {
    let base = temp_dir("unnest_with_ordinality");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from unnest(ARRAY[10, 20], ARRAY['x']::varchar[]) with ordinality as u(a, b, ord)",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["a", "b", "ord"]);
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(10), Value::Text("x".into()), Value::Int64(1)],
                    vec![Value::Int32(20), Value::Null, Value::Int64(2)],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn casts_support_int2_int8_float4_and_float8() {
    let base = temp_dir("extended_numeric_casts");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '7'::int2, '9000000000'::int8, '1.5'::real, '2.5'::double precision",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int16(7),
                    Value::Int64(9_000_000_000),
                    Value::Float64(1.5),
                    Value::Float64(2.5),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn float_text_input_accepts_whitespace_and_special_literals() {
    let base = temp_dir("float_text_input_whitespace");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '   NAN  '::float4, '          -INFINiTY   '::float8, '    0.0   '::float8",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            match &rows[0][0] {
                Value::Float64(v) => assert!(v.is_nan()),
                other => panic!("expected float NaN, got {other:?}"),
            }
            match &rows[0][1] {
                Value::Float64(v) => assert!(v.is_infinite() && *v < 0.0),
                other => panic!("expected negative infinity, got {other:?}"),
            }
            assert_eq!(rows[0][2], Value::Float64(0.0));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn float_text_out_of_range_errors_are_type_aware() {
    let base = temp_dir("float_text_out_of_range");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert!(matches!(
        run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select '10e70'::float4").unwrap_err(),
        ExecError::FloatOutOfRange {
            ty: "real",
            value,
        } if value == "10e70"
    ));
    assert!(matches!(
        run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select '10e400'::float8").unwrap_err(),
        ExecError::FloatOutOfRange {
            ty: "double precision",
            value,
        } if value == "10e400"
    ));
}

#[test]
fn float4_narrowing_reports_overflow_and_underflow() {
    let base = temp_dir("float4_narrowing_out_of_range");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert!(matches!(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select cast('10e70'::float8 as float4)",
        )
        .unwrap_err(),
        ExecError::FloatOverflow
    ));
    assert!(matches!(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select cast('10e-70'::float8 as float4)",
        )
        .unwrap_err(),
        ExecError::FloatUnderflow
    ));
}

#[test]
fn float_arithmetic_handles_infinity_and_nan() {
    let base = temp_dir("float_arithmetic_specials");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select 'Infinity'::float8 + 100.0, 'Infinity'::float8 / 'Infinity'::float8, '42'::float8 / 'Infinity'::float8, 'nan'::float8 / 'nan'::float8, 'nan'::float8 / '0'::float8",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                match &rows[0][0] {
                    Value::Float64(v) => assert!(v.is_infinite() && *v > 0.0),
                    other => panic!("expected infinity, got {other:?}"),
                }
                match &rows[0][1] {
                    Value::Float64(v) => assert!(v.is_nan()),
                    other => panic!("expected NaN, got {other:?}"),
                }
                assert_eq!(rows[0][2], Value::Float64(0.0));
                match &rows[0][3] {
                    Value::Float64(v) => assert!(v.is_nan()),
                    other => panic!("expected NaN, got {other:?}"),
                }
                match &rows[0][4] {
                    Value::Float64(v) => assert!(v.is_nan()),
                    other => panic!("expected NaN, got {other:?}"),
                }
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn quoted_float_literals_coerce_in_numeric_comparisons() {
    let base = temp_dir("quoted_float_literal_coercion");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select 1004.3::float8 = '1004.3', '1004.3' > 0.0::float8, 0.0::float8 < '1004.3', 1004.3::float8 + '1.2'",
            )
            .unwrap(),
            vec![vec![
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(true),
                Value::Float64(1005.5),
            ]],
        );
}

#[test]
fn float_math_builtins_cover_common_operations() {
    let base = temp_dir("float_math_builtins");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select trunc(42.8::float8), round(42.5::float8), ceil(42.2::float8), floor(42.8::float8), sign((-42.8)::float8), sqrt(81.0::float8), cbrt(27.0::float8), power(9.0::float8, 0.5::float8), exp(1.0::float8), ln(exp(1.0::float8))",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows[0],
                    vec![
                        Value::Float64(42.0),
                        Value::Float64(43.0),
                        Value::Float64(43.0),
                        Value::Float64(42.0),
                        Value::Float64(-1.0),
                        Value::Float64(9.0),
                        Value::Float64(3.0),
                        Value::Float64(3.0),
                        Value::Float64(std::f64::consts::E),
                        Value::Float64(1.0),
                    ]
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn float_math_domain_errors_are_explicit() {
    let base = temp_dir("float_math_domain_errors");
    let txns = TransactionManager::new_durable(&base).unwrap();

    for sql in [
        "select sqrt((-1.0)::float8)",
        "select ln(0.0::float8)",
        "select power((-1.0)::float8, 0.5::float8)",
        "select acosh(0.5::float8)",
        "select atanh(1.0::float8)",
    ] {
        assert!(matches!(
            run_sql(&base, &txns, INVALID_TRANSACTION_ID, sql).unwrap_err(),
            ExecError::InvalidStorageValue { .. }
        ));
    }
}

#[test]
fn box_text_input_accepts_adjacent_point_pairs() {
    let base = temp_dir("box_text_adjacent_point_pairs");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select '(0,0)(0,100)'::box::text, '(Infinity,0)(0,-Infinity)'::box::text",
        )
        .unwrap(),
        vec![vec![
            Value::Text("(0,100),(0,0)".into()),
            Value::Text("(Infinity,0),(0,-Infinity)".into()),
        ]],
    );
}

#[test]
fn degree_trig_builtins_snap_landmarks() {
    let base = temp_dir("degree_trig_builtins");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select sind(30.0::float8), cosd(60.0::float8), tand(45.0::float8), cotd(45.0::float8), asind(0.5::float8), acosd(0.5::float8), atand(1.0::float8), atan2d(1.0::float8, 1.0::float8)",
            )
            .unwrap(),
            vec![vec![
                Value::Float64(0.5),
                Value::Float64(0.5),
                Value::Float64(1.0),
                Value::Float64(1.0),
                Value::Float64(30.0),
                Value::Float64(60.0),
                Value::Float64(45.0),
                Value::Float64(45.0),
            ]],
        );
}

#[test]
fn float_send_functions_return_network_hex() {
    let base = temp_dir("float_send_functions");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select float4send('1.1754944e-38'::float4), float8send('2.2250738585072014E-308'::float8)",
            )
            .unwrap(),
            vec![vec![
                Value::Text("\\x00800000".into()),
                Value::Text("\\x0010000000000000".into()),
            ]],
        );
}

#[test]
fn power_accepts_quoted_numeric_literals_and_special_exponents() {
    let base = temp_dir("power_special_exponents");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select power(1004.3::float8, '2.0'), power((-1.0)::float8, 'nan'::float8), power(1.0::float8, 'nan'::float8), power((-1.0)::float8, 'inf'::float8), power((-1.1)::float8, 'inf'::float8), power((-1.1)::float8, '-inf'::float8), power('inf'::float8, '-2'::float8), power('-inf'::float8, '-3'::float8), power('-inf'::float8, '3'::float8)",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows[0][0], Value::Float64(1008618.4899999999));
                match &rows[0][1] {
                    Value::Float64(v) => assert!(v.is_nan()),
                    other => panic!("expected NaN, got {other:?}"),
                }
                assert_eq!(rows[0][2], Value::Float64(1.0));
                assert_eq!(rows[0][3], Value::Float64(1.0));
                match &rows[0][4] {
                    Value::Float64(v) => assert!(v.is_infinite() && *v > 0.0),
                    other => panic!("expected infinity, got {other:?}"),
                }
                assert_eq!(rows[0][5], Value::Float64(0.0));
                assert_eq!(rows[0][6], Value::Float64(0.0));
                assert_eq!(rows[0][7], Value::Float64(-0.0));
                match &rows[0][8] {
                    Value::Float64(v) => assert!(v.is_infinite() && *v < 0.0),
                    other => panic!("expected negative infinity, got {other:?}"),
                }
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn power_zero_to_negative_infinity_errors() {
    let base = temp_dir("power_zero_negative_infinity");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert!(matches!(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select power(0.0::float8, '-inf'::float8)",
        )
        .unwrap_err(),
        ExecError::InvalidStorageValue { .. }
    ));
}

#[test]
fn quantified_in_coerces_float_results_against_integer_lists() {
    let base = temp_dir("quantified_in_float_integer");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select sind(30.0::float8) in (-1, -0.5, 0, 0.5, 1), acosd(0.5::float8) in (0, 60, 90, 120, 180), atand(1.0::float8) in (-90, -45, 0, 45, 90)",
            )
            .unwrap(),
            vec![vec![Value::Bool(true), Value::Bool(true), Value::Bool(true)]],
        );
}

#[test]
fn erf_and_gamma_float_builtins_cover_expected_edges() {
    let base = temp_dir("erf_gamma_builtins");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select erf(0.45::float8), erfc(0.45::float8), gamma(5.0::float8), lgamma(5.0::float8), atanh('nan'::float8), gamma('infinity'::float8), lgamma('infinity'::float8), lgamma('-infinity'::float8)",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                match &rows[0][0] {
                    Value::Float64(v) => assert!((*v - 0.47548171978692366).abs() < 1e-12),
                    other => panic!("expected float result, got {other:?}"),
                }
                match &rows[0][1] {
                    Value::Float64(v) => assert!((*v - 0.5245182802130763).abs() < 1e-12),
                    other => panic!("expected float result, got {other:?}"),
                }
                assert_eq!(rows[0][2], Value::Float64(24.0));
                match &rows[0][3] {
                    Value::Float64(v) => assert!((*v - 3.1780538303479458).abs() < 1e-12),
                    other => panic!("expected float result, got {other:?}"),
                }
                match &rows[0][4] {
                    Value::Float64(v) => assert!(v.is_nan()),
                    other => panic!("expected NaN, got {other:?}"),
                }
                assert_eq!(rows[0][5], Value::Float64(f64::INFINITY));
                assert_eq!(rows[0][6], Value::Float64(f64::INFINITY));
                assert_eq!(rows[0][7], Value::Float64(f64::INFINITY));
            }
            other => panic!("expected query result, got {:?}", other),
        }

    assert!(matches!(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select gamma(0.0::float8)",
        )
        .unwrap_err(),
        ExecError::FloatOverflow
    ));
    assert!(matches!(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select gamma(1000.0::float8), lgamma(1e308::float8)",
        )
        .unwrap_err(),
        ExecError::FloatOverflow
    ));
}

#[test]
fn float_runtime_semantics_cover_sign_ordering_and_overflow_edges() {
    let base = temp_dir("float_runtime_semantics");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select sign(0.0::float8), sign((-34.84)::float8)",
        )
        .unwrap(),
        vec![vec![Value::Float64(0.0), Value::Float64(-1.0)]],
    );

    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select x from (values (0.0::float8), (-34.84::float8), (-1004.3::float8), (-1.2345678901234e200::float8), (-1.2345678901234e-200::float8)) t(x) order by 1",
            )
            .unwrap(),
            vec![
                vec![Value::Float64(-1.2345678901234e200)],
                vec![Value::Float64(-1004.3)],
                vec![Value::Float64(-34.84)],
                vec![Value::Float64(-1.2345678901234e-200)],
                vec![Value::Float64(0.0)],
            ],
        );

    assert!(matches!(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select (-1.2345678901234e200::float8) * (1e200::float8)",
        )
        .unwrap_err(),
        ExecError::FloatOverflow
    ));
}

#[test]
fn float_to_int8_cast_rejects_upper_boundary_round_up() {
    let base = temp_dir("float_to_int8_upper_boundary");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert!(matches!(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select '9223372036854775807'::float8::int8, '9223372036854775807'::float4::int8",
        )
        .unwrap_err(),
        ExecError::Int8OutOfRange
    ));
}

#[test]
fn narrowing_integer_casts_raise_out_of_range_errors() {
    let base = temp_dir("narrowing_integer_casts");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert!(matches!(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select cast(4567890123456789::int8 as int4)",
        )
        .unwrap_err(),
        ExecError::Int4OutOfRange
    ));
    assert!(matches!(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select cast(4567890123456789::int8 as int2)",
        )
        .unwrap_err(),
        ExecError::Int2OutOfRange
    ));
}

#[test]
fn extended_numeric_columns_round_trip_through_storage() {
    let base = temp_dir("extended_numeric_storage");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let mut catalog = Catalog::default();
    catalog.insert(
        "metrics",
        test_catalog_entry(
            crate::RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 15004,
            },
            RelationDesc {
                columns: vec![
                    crate::backend::catalog::catalog::column_desc(
                        "a",
                        crate::backend::parser::SqlType::new(
                            crate::backend::parser::SqlTypeKind::Int2,
                        ),
                        true,
                    ),
                    crate::backend::catalog::catalog::column_desc(
                        "b",
                        crate::backend::parser::SqlType::new(
                            crate::backend::parser::SqlTypeKind::Int8,
                        ),
                        true,
                    ),
                    crate::backend::catalog::catalog::column_desc(
                        "c",
                        crate::backend::parser::SqlType::new(
                            crate::backend::parser::SqlTypeKind::Float4,
                        ),
                        true,
                    ),
                    crate::backend::catalog::catalog::column_desc(
                        "d",
                        crate::backend::parser::SqlType::new(
                            crate::backend::parser::SqlTypeKind::Float8,
                        ),
                        true,
                    ),
                ],
            },
        ),
    );
    run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "insert into metrics (a, b, c, d) values ('7'::int2, '9000000000'::int8, '1.5'::real, '2.5'::double precision)",
            catalog.clone(),
        )
        .unwrap();
    match run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select a, b, c, d from metrics",
        catalog,
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int16(7),
                    Value::Int64(9_000_000_000),
                    Value::Float64(1.5),
                    Value::Float64(2.5),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn arithmetic_operators_work_for_extended_numeric_types() {
    let base = temp_dir("extended_numeric_operators");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 5 - 2, 3 * 4, 9 / 2, 9 % 4, +1.5, 2.5 * 2",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int32(3),
                    Value::Int32(12),
                    Value::Int32(4),
                    Value::Int32(1),
                    Value::Numeric("1.5".into()),
                    Value::Numeric("5".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn integer_division_overflow_returns_sql_error() {
    let base = temp_dir("integer_division_overflow");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert!(matches!(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select (-32768::int2) / (-1::int2)"
        )
        .unwrap_err(),
        ExecError::Int2OutOfRange
    ));
    assert!(matches!(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select (-2147483648::int4) / (-1::int4)"
        )
        .unwrap_err(),
        ExecError::Int4OutOfRange
    ));
    assert!(matches!(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select (-9223372036854775808::int8) / (-1::int8)"
        )
        .unwrap_err(),
        ExecError::Int8OutOfRange
    ));
}

#[test]
fn integer_modulo_min_over_negative_one_returns_zero() {
    let base = temp_dir("integer_modulo_min_over_negative_one");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select (-32768::int2) % (-1::int2), (-2147483648::int4) % (-1::int4), (-9223372036854775808::int8) % (-1::int8)",
            )
            .unwrap(),
            vec![vec![Value::Int16(0), Value::Int32(0), Value::Int64(0)]],
        );
}

#[test]
fn integer_arithmetic_overflow_raises_error() {
    let base = temp_dir("integer_arithmetic_overflow");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert!(matches!(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select (32767::int2) + (2::int2)",
        )
        .unwrap_err(),
        ExecError::Int2OutOfRange
    ));
    assert!(matches!(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select (-32768::int2) * (-1::int2)",
        )
        .unwrap_err(),
        ExecError::Int2OutOfRange
    ));
}

#[test]
fn float_and_numeric_casts_to_int2_follow_postgres_rounding() {
    let base = temp_dir("float_numeric_cast_int2_rounding");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select (-2.5::float8)::int2, (-1.5::float8)::int2, (-0.5::float8)::int2, (0.5::float8)::int2, (1.5::float8)::int2, (2.5::float8)::int2, (-2.5::numeric)::int2, (-0.5::numeric)::int2, (0.5::numeric)::int2, (2.5::numeric)::int2",
            )
            .unwrap(),
            vec![vec![
                Value::Int16(-2),
                Value::Int16(-2),
                Value::Int16(0),
                Value::Int16(0),
                Value::Int16(2),
                Value::Int16(2),
                Value::Int16(-3),
                Value::Int16(-1),
                Value::Int16(1),
                Value::Int16(3),
            ]],
        );
}

#[test]
fn numeric_special_values_to_integer_casts_raise_postgres_style_errors() {
    let base = temp_dir("numeric_special_values_to_int_casts");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'NaN'::numeric::int2",
    )
    .unwrap_err();
    assert!(matches!(err, ExecError::NumericNaNToInt { ty: "smallint" }));
    assert_eq!(format_exec_error(&err), "cannot convert NaN to smallint");

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'Infinity'::numeric::int4",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::NumericInfinityToInt { ty: "integer" }
    ));
    assert_eq!(
        format_exec_error(&err),
        "cannot convert infinity to integer"
    );

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '-Infinity'::numeric::int8",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::NumericInfinityToInt { ty: "bigint" }
    ));
    assert_eq!(format_exec_error(&err), "cannot convert infinity to bigint");
}

#[test]
fn abs_builtin_supports_smallint_filters() {
    let base = temp_dir("abs_builtin_smallint");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select abs((-1234)::int2), abs((-2.5)::float8), abs((-2.5)::numeric)",
        )
        .unwrap(),
        vec![vec![
            Value::Int16(1234),
            Value::Float64(2.5),
            Value::Numeric(crate::backend::executor::expr_ops::parse_numeric_text("2.5").unwrap()),
        ]],
    );
}

#[test]
fn gcd_and_lcm_support_integer_widths_and_overflow() {
    let base = temp_dir("gcd_lcm_integer_widths");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select gcd((-330)::int4, 462::int4), lcm((-330)::int4, 462::int4), gcd((-9223372036854775808)::int8, 1073741824::int8)",
            )
            .unwrap(),
            vec![vec![
                Value::Int32(66),
                Value::Int32(2310),
                Value::Int64(1073741824),
            ]],
        );

    assert!(matches!(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select gcd((-2147483648)::int4, 0::int4)",
        )
        .unwrap_err(),
        ExecError::Int4OutOfRange
    ));
    assert!(matches!(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select lcm(9223372036854775807::int8, 9223372036854775806::int8)",
        )
        .unwrap_err(),
        ExecError::Int8OutOfRange
    ));
}

#[test]
fn function_style_type_casts_lower_to_regular_casts() {
    let base = temp_dir("function_style_type_casts");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select float8((42)::int8), int8((42)::int2)",
        )
        .unwrap(),
        vec![vec![Value::Float64(42.0), Value::Int64(42)]],
    );
}

#[test]
fn int2_text_input_accepts_prefixed_and_underscored_literals() {
    let base = temp_dir("int2_text_input_literals");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select int2 '0b100101', int2 '0o273', int2 '0x42F', int2 '1_000', int2 '0b_10_0101', int2 '-0x8000'",
            )
            .unwrap(),
            vec![vec![
                Value::Int16(37),
                Value::Int16(187),
                Value::Int16(1071),
                Value::Int16(1000),
                Value::Int16(37),
                Value::Int16(-32768),
            ]],
        );
}

#[test]
fn int2_assignment_uses_input_errors_instead_of_type_mismatch() {
    let column = crate::backend::catalog::catalog::column_desc(
        "f1",
        crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int2),
        true,
    );

    let err =
        crate::backend::executor::value_io::encode_value(&column, &Value::Text("34.5".into()))
            .unwrap_err();
    assert!(
        matches!(err, ExecError::InvalidIntegerInput { ty: "smallint", .. }),
        "got {err:?}"
    );

    let err =
        crate::backend::executor::value_io::encode_value(&column, &Value::Text("100000".into()))
            .unwrap_err();
    assert!(
        matches!(err, ExecError::IntegerOutOfRange { ty: "smallint", .. }),
        "got {err:?}"
    );
}

#[test]
fn pg_input_is_valid_reports_int2_and_int2vector_results() {
    let base = temp_dir("pg_input_is_valid_int2");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select pg_input_is_valid('34', 'int2'), pg_input_is_valid('asdf', 'int2'), pg_input_is_valid(' 1 3  5 ', 'int2vector'), pg_input_is_valid('50000', 'int2vector')",
            )
            .unwrap(),
            vec![vec![
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(true),
                Value::Bool(false),
            ]],
        );
}

#[test]
fn publication_describe_builtins_run_via_normal_sql() {
    let base = temp_dir("publication_describe_builtins");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            &format!(
                "select pg_get_userbyid({}::oid), \
                        pg_get_expr(null::pg_node_tree, 1::oid), \
                        array_upper(array[1, 2]::int4[], 1)",
                crate::include::catalog::BOOTSTRAP_SUPERUSER_OID
            ),
        )
        .unwrap(),
        vec![vec![
            Value::Text("postgres".into()),
            Value::Null,
            Value::Int32(2),
        ]],
    );
}

#[test]
fn pg_backend_pid_returns_executor_client_id() {
    let base = temp_dir("pg_backend_pid_returns_executor_client_id");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select pg_backend_pid()",
        )
        .unwrap(),
        vec![vec![Value::Int32(77)]],
    );
}

#[test]
fn index_property_builtins_report_am_and_index_capabilities() {
    let base = temp_dir("index_property_builtins");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let mut catalog = Catalog::default();
    catalog.insert(
        "ints",
        test_catalog_entry(
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 14_040,
            },
            RelationDesc {
                columns: vec![
                    crate::backend::catalog::catalog::column_desc(
                        "a",
                        crate::backend::parser::SqlType::new(
                            crate::backend::parser::SqlTypeKind::Int4,
                        ),
                        false,
                    ),
                    crate::backend::catalog::catalog::column_desc(
                        "b",
                        crate::backend::parser::SqlType::new(
                            crate::backend::parser::SqlTypeKind::Int4,
                        ),
                        true,
                    ),
                ],
            },
        ),
    );
    catalog.insert(
        "boxes",
        test_catalog_entry(
            RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 14_041,
            },
            RelationDesc {
                columns: vec![crate::backend::catalog::catalog::column_desc(
                    "b",
                    crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Box),
                    false,
                )],
            },
        ),
    );
    let ints_oid = catalog.lookup_any_relation("ints").unwrap().relation_oid;
    let boxes_oid = catalog.lookup_any_relation("boxes").unwrap().relation_oid;
    catalog
        .create_index_for_relation_with_options_and_flags(
            "ints_a_idx",
            ints_oid,
            false,
            false,
            &[IndexColumnDef::from("a")],
            &crate::backend::catalog::CatalogIndexBuildOptions {
                am_oid: crate::include::catalog::BTREE_AM_OID,
                indclass: vec![crate::include::catalog::INT4_BTREE_OPCLASS_OID],
                indcollation: vec![0],
                indoption: vec![0],
                indnullsnotdistinct: false,
                brin_options: None,
            },
            None,
        )
        .unwrap();
    catalog
        .create_index_for_relation_with_options_and_flags(
            "boxes_gist_idx",
            boxes_oid,
            false,
            false,
            &[IndexColumnDef::from("b")],
            &crate::backend::catalog::CatalogIndexBuildOptions {
                am_oid: crate::include::catalog::GIST_AM_OID,
                indclass: vec![crate::include::catalog::BOX_GIST_OPCLASS_OID],
                indcollation: vec![0],
                indoption: vec![0],
                indnullsnotdistinct: false,
                brin_options: None,
            },
            None,
        )
        .unwrap();
    catalog
        .create_index_for_relation_with_options_and_flags(
            "boxes_spgist_idx",
            boxes_oid,
            false,
            false,
            &[IndexColumnDef::from("b")],
            &crate::backend::catalog::CatalogIndexBuildOptions {
                am_oid: crate::include::catalog::SPGIST_AM_OID,
                indclass: vec![crate::include::catalog::BOX_SPGIST_OPCLASS_OID],
                indcollation: vec![0],
                indoption: vec![0],
                indnullsnotdistinct: false,
                brin_options: None,
            },
            None,
        )
        .unwrap();

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select
                 pg_indexam_has_property((select oid from pg_am where amname = 'btree'), 'can_order'),
                 pg_indexam_has_property((select oid from pg_am where amname = 'hash'), 'can_multi_col'),
                 pg_indexam_has_property((select oid from pg_am where amname = 'gin'), 'can_multi_col'),
                 pg_indexam_has_property((select oid from pg_am where amname = 'spgist'), 'can_include'),
                 pg_indexam_has_property((select oid from pg_am where amname = 'brin'), 'bogus'),
                 pg_index_has_property('ints_a_idx'::regclass, 'clusterable'),
                 pg_index_has_property('boxes_gist_idx'::regclass, 'backward_scan'),
                 pg_index_has_property('boxes_spgist_idx'::regclass, 'index_scan'),
                 pg_index_column_has_property('ints_a_idx'::regclass, 1, 'asc'),
                 pg_index_column_has_property('ints_a_idx'::regclass, 1, 'nulls_last'),
                 pg_index_column_has_property('boxes_gist_idx'::regclass, 1, 'distance_orderable'),
                 pg_index_column_has_property('boxes_spgist_idx'::regclass, 1, 'returnable'),
                 pg_index_column_has_property('ints_a_idx'::regclass, 0, 'asc'),
                 pg_index_column_has_property('ints_a_idx'::regclass, 2, 'asc'),
                 pg_index_column_has_property('ints'::regclass, 1, 'asc')",
            catalog,
        )
        .unwrap(),
        vec![vec![
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(true),
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
            Value::Null,
            Value::Null,
            Value::Null,
        ]],
    );
}

#[test]
fn int2vector_casts_to_int2_array() {
    assert_eq!(
        crate::backend::executor::expr_casts::cast_value(
            Value::Text("1 2".into()),
            SqlType::array_of(SqlType::new(crate::backend::parser::SqlTypeKind::Int2)),
        )
        .unwrap(),
        Value::PgArray(
            crate::include::nodes::datum::ArrayValue::from_1d(vec![
                Value::Int16(1),
                Value::Int16(2),
            ])
            .with_element_type_oid(crate::include::catalog::INT2_TYPE_OID)
        ),
    );
}

#[test]
fn tid_and_xid_text_casts_accept_pg_input() {
    let base = temp_dir("tid_xid_text_casts");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select '(4294967295,65535)'::tid, '4294967295'::xid",
        )
        .unwrap(),
        vec![vec![
            Value::Text("(4294967295,65535)".into()),
            Value::Int64(4_294_967_295),
        ]],
    );
}

#[test]
fn xml_input_errors_format_primary_message() {
    let err = ExecError::XmlInput {
        raw_input: "<wrong".into(),
        message: "unsupported XML feature".into(),
        detail: Some(
            "This functionality requires the server to be built with libxml support.".into(),
        ),
        context: None,
        sqlstate: "0A000",
    };

    assert_eq!(format_exec_error(&err), "unsupported XML feature");
}

#[test]
fn oidvector_text_values_support_array_functions() {
    let base = temp_dir("oidvector_array_functions");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select array_lower(proargtypes, 1), array_upper(proargtypes, 1), 0::oid = any(proargtypes) from pg_proc where pronargs = 1 limit 1",
        )
        .unwrap(),
        vec![vec![Value::Int32(0), Value::Int32(0), Value::Bool(false)]],
    );
}

#[test]
fn pg_input_is_valid_reports_float_results() {
    let base = temp_dir("pg_input_is_valid_float");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select pg_input_is_valid('34.5', 'float4'), pg_input_is_valid('xyz', 'float4'), pg_input_is_valid('1e4000', 'float8'), pg_input_is_valid('   NAN  ', 'float8')",
            )
            .unwrap(),
            vec![vec![
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(true),
            ]],
        );
}

#[test]
fn pg_input_is_valid_reports_bool_results() {
    let base = temp_dir("pg_input_is_valid_bool");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select pg_input_is_valid('true', 'bool'), pg_input_is_valid('asdf', 'bool'), pg_input_is_valid('  of  ', 'bool')",
            )
            .unwrap(),
            vec![vec![Value::Bool(true), Value::Bool(false), Value::Bool(true)]],
        );
}

#[test]
fn position_text_function_uses_character_offsets() {
    let base = temp_dir("position_text_function_uses_character_offsets");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let result = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select position('각' in '가각나'), position('', '가각나'), position('다' in '가각나')",
    )
    .unwrap();
    assert_query_rows(
        result,
        vec![vec![Value::Int32(2), Value::Int32(1), Value::Int32(0)]],
    );
}

#[test]
fn convert_from_decodes_utf8_and_euc_kr_hex_text() {
    let base = temp_dir("convert_from_decodes_utf8_and_euc_kr_hex_text");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let result = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select convert_from('\\xc2b0', 'UTF8'), convert_from('\\xbcf6c7d0', 'EUC_KR')",
    )
    .unwrap();
    assert_query_rows(
        result,
        vec![vec![Value::Text("°".into()), Value::Text("수학".into())]],
    );
}

#[test]
fn pg_rust_test_enc_conversion_validates_utf8_prefixes() {
    let base = temp_dir("pg_rust_test_enc_conversion_validates_utf8_prefixes");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let result = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select pg_rust_test_enc_conversion('\\x66006f'::bytea, 'utf8', 'utf8', true)",
    )
    .unwrap();
    assert_query_rows(
        result,
        vec![vec![Value::Record(
            crate::include::nodes::datum::RecordValue::anonymous(vec![
                ("validlen".into(), Value::Int32(1)),
                ("result".into(), Value::Bytea(vec![0x66])),
            ]),
        )]],
    );
}

#[test]
fn pg_rust_test_enc_conversion_converts_euc_kr_to_utf8() {
    let base = temp_dir("pg_rust_test_enc_conversion_converts_euc_kr_to_utf8");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let result = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select pg_rust_test_enc_conversion('\\xbcf6c7d0'::bytea, 'euc_kr', 'utf8', false)",
    )
    .unwrap();
    assert_query_rows(
        result,
        vec![vec![Value::Record(
            crate::include::nodes::datum::RecordValue::anonymous(vec![
                ("validlen".into(), Value::Int32(4)),
                ("result".into(), Value::Bytea("수학".as_bytes().to_vec())),
            ]),
        )]],
    );
}

#[test]
fn pg_input_is_valid_reports_varchar_typmod_results() {
    let base = temp_dir("pg_input_is_valid_varchar");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select pg_input_is_valid('abcd  ', 'varchar(4)'), pg_input_is_valid('abcde', 'varchar(4)')",
            )
            .unwrap(),
            vec![vec![Value::Bool(true), Value::Bool(false)]],
        );
}

#[test]
fn pg_input_is_valid_reports_numeric_overflow_and_prefixed_literals() {
    let base = temp_dir("pg_input_is_valid_numeric");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select \
                pg_input_is_valid('1e400000', 'numeric'), \
                pg_input_is_valid('  -0B_1010  ', 'numeric'), \
                pg_input_is_valid('  +0X_FF  ', 'numeric')",
        )
        .unwrap(),
        vec![vec![
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(true),
        ]],
    );
}

#[test]
fn pg_input_error_info_returns_one_row_with_structured_fields() {
    let base = temp_dir("pg_input_error_info_int2");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from pg_input_error_info('50000', 'int2')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("value \"50000\" is out of range for type smallint".into()),
            Value::Null,
            Value::Null,
            Value::Text("22003".into()),
        ]],
    );

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from pg_input_error_info('34', 'int2')",
        )
        .unwrap(),
        vec![vec![Value::Null, Value::Null, Value::Null, Value::Null]],
    );
}

#[test]
fn pg_input_error_info_reports_float_out_of_range() {
    let base = temp_dir("pg_input_error_info_float");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from pg_input_error_info('1e400', 'float4')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("\"1e400\" is out of range for type real".into()),
            Value::Null,
            Value::Null,
            Value::Text("22003".into()),
        ]],
    );
}

#[test]
fn pg_input_error_info_reports_time_out_of_range_sqlstate() {
    let base = temp_dir("pg_input_error_info_time_out_of_range");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from pg_input_error_info('25:00:00', 'time')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("date/time field value out of range: \"25:00:00\"".into()),
            Value::Null,
            Value::Null,
            Value::Text("22008".into()),
        ]],
    );
}

#[test]
fn pg_input_error_info_reports_numeric_overflow() {
    let base = temp_dir("pg_input_error_info_numeric_overflow");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from pg_input_error_info('1e400000', 'numeric')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("value overflows numeric format".into()),
            Value::Null,
            Value::Null,
            Value::Text("22003".into()),
        ]],
    );
}

#[test]
fn pg_input_error_info_reports_numeric_typmod_overflow_details() {
    let base = temp_dir("pg_input_error_info_numeric_typmod");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select \
                pg_input_is_valid('1234.567', 'numeric(8,4)'), \
                pg_input_is_valid('1234.567', 'numeric(7,4)')",
        )
        .unwrap(),
        vec![vec![Value::Bool(true), Value::Bool(false)]],
    );

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from pg_input_error_info('1234.567', 'numeric(7,4)')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("numeric field overflow".into()),
            Value::Text(
                "A field with precision 7, scale 4 must round to an absolute value less than 10^3."
                    .into(),
            ),
            Value::Null,
            Value::Text("22003".into()),
        ]],
    );
}

#[test]
fn pg_input_error_info_rejects_numeric_prefixed_fractional_literal() {
    let base = temp_dir("pg_input_error_info_numeric_prefixed_fraction");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from pg_input_error_info('0x1234.567', 'numeric')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("invalid input syntax for type numeric: \"0x1234.567\"".into()),
            Value::Null,
            Value::Null,
            Value::Text("22P02".into()),
        ]],
    );
}

#[test]
fn pg_input_error_info_reports_jsonb_structured_error_fields() {
    let base = temp_dir("pg_input_error_info_jsonb");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from pg_input_error_info('{\"a\":true', 'jsonb')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("invalid input syntax for type json".into()),
            Value::Text("The input string ended unexpectedly.".into()),
            Value::Null,
            Value::Text("22P02".into()),
        ]],
    );
}

#[test]
fn pg_input_error_info_reports_jsonb_numeric_overflow() {
    let base = temp_dir("pg_input_error_info_jsonb_overflow");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from pg_input_error_info('{\"a\":1e1000000}', 'jsonb')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("value overflows numeric format".into()),
            Value::Null,
            Value::Null,
            Value::Text("22003".into()),
        ]],
    );
}

#[test]
fn pg_input_error_info_reports_bool_invalid_input() {
    let base = temp_dir("pg_input_error_info_bool");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from pg_input_error_info('junk', 'bool')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("invalid input syntax for type boolean: \"junk\"".into()),
            Value::Null,
            Value::Null,
            Value::Text("22P02".into()),
        ]],
    );
}

#[test]
fn pg_input_error_info_reports_array_element_input_error() {
    let base = temp_dir("pg_input_error_info_array");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from pg_input_error_info('{1,zed}', 'integer[]')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("invalid input syntax for type integer: \"zed\"".into()),
            Value::Null,
            Value::Null,
            Value::Text("22P02".into()),
        ]],
    );
}

#[test]
fn boolean_text_cast_accepts_whitespace_and_aliases() {
    let base = temp_dir("boolean_text_casts");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select bool '   f           ', bool 'yes', bool '1', '     FALSE'::text::boolean",
        )
        .unwrap(),
        vec![vec![
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
        ]],
    );

    let err = run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select bool 'yeah'").unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidBooleanInput { ref value } if value == "yeah"
    ));
}

#[test]
fn boolean_ordering_operators_match_postgres() {
    let base = temp_dir("boolean_ordering");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select bool 't' > bool 'f', bool 't' >= bool 'f', bool 'f' < bool 't', bool 'f' <= bool 't'",
            )
            .unwrap(),
            vec![vec![
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(true),
            ]],
        );
}

#[test]
fn booleq_and_boolne_execute() {
    let base = temp_dir("boolean_builtins");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select booleq(bool 'false', bool 'true'), boolne(bool 'false', bool 'true')",
        )
        .unwrap(),
        vec![vec![Value::Bool(false), Value::Bool(true)]],
    );
}

#[test]
fn integer_to_boolean_casts_match_postgres() {
    let base = temp_dir("integer_to_boolean_casts");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select 0::boolean, 1::boolean, 2::boolean",
        )
        .unwrap(),
        vec![vec![
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(true),
        ]],
    );
}

#[test]
fn pg_input_error_info_reports_varchar_typmod_truncation() {
    let base = temp_dir("pg_input_error_info_varchar");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from pg_input_error_info('abcde', 'varchar(4)')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("value too long for type character varying(4)".into()),
            Value::Null,
            Value::Null,
            Value::Text("22001".into()),
        ]],
    );
}

#[test]
fn pg_input_is_valid_reports_char_typmod_results() {
    let base = temp_dir("pg_input_is_valid_char");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select pg_input_is_valid('abcd  ', 'char(4)'), pg_input_is_valid('abcde', 'char(4)')",
        )
        .unwrap(),
        vec![vec![Value::Bool(true), Value::Bool(false)]],
    );
}

#[test]
fn pg_input_error_info_reports_char_typmod_truncation() {
    let base = temp_dir("pg_input_error_info_char");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from pg_input_error_info('abcde', 'char(4)')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("value too long for type character(4)".into()),
            Value::Null,
            Value::Null,
            Value::Text("22001".into()),
        ]],
    );
}

#[test]
fn internal_char_casts_follow_postgres_io_rules() {
    let base = temp_dir("internal_char_casts");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select 'a'::\"char\", '\\101'::\"char\", '\\377'::\"char\", '\\377'::\"char\"::text, '\\000'::\"char\"::text, ''::text::\"char\"::text",
            )
            .unwrap(),
            vec![vec![
                Value::InternalChar(b'a'),
                Value::InternalChar(b'A'),
                Value::InternalChar(0o377),
                Value::Text("\\377".into()),
                Value::Text("".into()),
                Value::Text("".into()),
            ]],
        );
}

#[test]
fn bytea_text_input_and_pg_input_helpers_follow_postgres_rules() {
    let base = temp_dir("bytea_input_helpers");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select E'\\\\xDe Ad Be Ef'::bytea, E'a\\\\\\\\b\\\\123'::bytea, pg_input_is_valid(E'\\\\xDe Ad Be Ef', 'bytea'), pg_input_is_valid(E'\\\\x123', 'bytea')",
            )
            .unwrap(),
            vec![vec![
                Value::Bytea(vec![0xde, 0xad, 0xbe, 0xef]),
                Value::Bytea(vec![b'a', b'\\', b'b', 0o123]),
                Value::Bool(true),
                Value::Bool(false),
            ]],
        );

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from pg_input_error_info(E'\\\\x123', 'bytea')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("invalid hexadecimal data: odd number of digits".into()),
            Value::Null,
            Value::Null,
            Value::Text("22023".into()),
        ]],
    );

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from pg_input_error_info(E'\\\\x12x3', 'bytea')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("invalid hexadecimal digit: \"x\"".into()),
            Value::Null,
            Value::Null,
            Value::Text("22023".into()),
        ]],
    );

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select E'foo\\\\99bar'::bytea",
    )
    .unwrap_err();
    assert_eq!(
        format_exec_error(&err),
        "invalid input syntax for type bytea"
    );
}

#[test]
fn bit_text_casts_and_pg_input_helpers_follow_postgres_rules() {
    let base = temp_dir("bit_input_helpers");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select '10'::bit(4), '101011'::varbit(4), pg_input_is_valid('10', 'bit(4)'), pg_input_is_valid('01010Z01', 'varbit')",
            )
            .unwrap(),
            vec![vec![
                Value::Bit(crate::include::nodes::datum::BitString::new(4, vec![0b1000_0000])),
                Value::Bit(crate::include::nodes::datum::BitString::new(4, vec![0b1010_0000])),
                Value::Bool(false),
                Value::Bool(false),
            ]],
        );

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from pg_input_error_info('10', 'bit(4)')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("bit string length 2 does not match type bit(4)".into()),
            Value::Null,
            Value::Null,
            Value::Text("22026".into()),
        ]],
    );

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from pg_input_error_info('01010Z01', 'varbit')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("\"Z\" is not a valid binary digit".into()),
            Value::Null,
            Value::Null,
            Value::Text("22P02".into()),
        ]],
    );
}

#[test]
fn bit_functions_and_operators_follow_postgres_rules() {
    let base = temp_dir("bit_functions_and_operators");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select B'0101' || B'11', length(B'0101'), substring(B'010101' from 2 for 3), overlay(B'010101' placing B'11' from 2), position(B'101' in B'010101'), get_bit(B'0101011000100', 10), set_bit(B'0101011000100100', 15, 1), bit_count(B'0101011100'::bit(10)), B'0011' & B'0101', B'0011' | B'0101', B'0011' # B'0101', ~B'0011', B'1100' << 1, B'1100' >> 2",
            )
            .unwrap(),
            vec![vec![
                Value::Bit(crate::include::nodes::datum::BitString::new(6, vec![0b0101_1100])),
                Value::Int32(4),
                Value::Bit(crate::include::nodes::datum::BitString::new(3, vec![0b1010_0000])),
                Value::Bit(crate::include::nodes::datum::BitString::new(6, vec![0b0111_0100])),
                Value::Int32(2),
                Value::Int32(1),
                Value::Bit(crate::include::nodes::datum::BitString::new(16, vec![0b0101_0110, 0b0010_0101])),
                Value::Int64(5),
                Value::Bit(crate::include::nodes::datum::BitString::new(4, vec![0b0001_0000])),
                Value::Bit(crate::include::nodes::datum::BitString::new(4, vec![0b0111_0000])),
                Value::Bit(crate::include::nodes::datum::BitString::new(4, vec![0b0110_0000])),
                Value::Bit(crate::include::nodes::datum::BitString::new(4, vec![0b1100_0000])),
                Value::Bit(crate::include::nodes::datum::BitString::new(4, vec![0b1000_0000])),
                Value::Bit(crate::include::nodes::datum::BitString::new(4, vec![0b0011_0000])),
            ]],
        );

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select B'001' & B'10'",
    )
    .unwrap_err();
    assert!(matches!(err, ExecError::BitStringSizeMismatch { op: "&" }));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select set_bit(B'0101011000100100', 16, 1)",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::BitIndexOutOfRange {
            index: 16,
            max_index: 15
        }
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select substring(B'01010101' from -10 for -2147483646)",
    )
    .unwrap_err();
    assert!(matches!(err, ExecError::NegativeSubstringLength));

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select overlay(B'0101011100' placing '001' from 2 for 3)",
        )
        .unwrap(),
        vec![vec![Value::Bit(
            crate::include::nodes::datum::BitString::new(10, vec![0b0001_0111, 0b0000_0000]),
        )]],
    );
}

#[test]
fn insert_select_default_values_and_table_stmt_work() {
    let mut catalog = Catalog::default();
    let mut desc = RelationDesc {
        columns: vec![
            crate::backend::catalog::catalog::column_desc(
                "b1",
                crate::backend::parser::SqlType::with_bit_len(
                    crate::backend::parser::SqlTypeKind::Bit,
                    4,
                ),
                false,
            ),
            crate::backend::catalog::catalog::column_desc(
                "b2",
                crate::backend::parser::SqlType::with_bit_len(
                    crate::backend::parser::SqlTypeKind::VarBit,
                    5,
                ),
                true,
            ),
        ],
    };
    desc.columns[0].default_expr = Some("'1001'".into());
    desc.columns[1].default_expr = Some("B'0101'".into());
    catalog.insert(
        "bit_defaults",
        test_catalog_entry(
            crate::RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 15006,
            },
            desc,
        ),
    );

    let mut harness = SeededSqlHarness::new("bit_insert_defaults", catalog);
    harness
        .execute(
            INVALID_TRANSACTION_ID,
            "insert into bit_defaults default values",
        )
        .unwrap();
    harness
        .execute(
            INVALID_TRANSACTION_ID,
            "insert into bit_defaults (b2) values (B'1')",
        )
        .unwrap();
    harness
        .execute(
            INVALID_TRANSACTION_ID,
            "insert into bit_defaults values (DEFAULT, B'11')",
        )
        .unwrap();
    harness
        .execute(
            INVALID_TRANSACTION_ID,
            "insert into bit_defaults select B'1111', B'1'",
        )
        .unwrap();
    assert_query_rows(
        harness
            .execute(INVALID_TRANSACTION_ID, "table bit_defaults")
            .unwrap(),
        vec![
            vec![
                Value::Bit(crate::include::nodes::datum::BitString::new(
                    4,
                    vec![0b1001_0000],
                )),
                Value::Bit(crate::include::nodes::datum::BitString::new(
                    4,
                    vec![0b0101_0000],
                )),
            ],
            vec![
                Value::Bit(crate::include::nodes::datum::BitString::new(
                    4,
                    vec![0b1001_0000],
                )),
                Value::Bit(crate::include::nodes::datum::BitString::new(
                    1,
                    vec![0b1000_0000],
                )),
            ],
            vec![
                Value::Bit(crate::include::nodes::datum::BitString::new(
                    4,
                    vec![0b1001_0000],
                )),
                Value::Bit(crate::include::nodes::datum::BitString::new(
                    2,
                    vec![0b1100_0000],
                )),
            ],
            vec![
                Value::Bit(crate::include::nodes::datum::BitString::new(
                    4,
                    vec![0b1111_0000],
                )),
                Value::Bit(crate::include::nodes::datum::BitString::new(
                    1,
                    vec![0b1000_0000],
                )),
            ],
        ],
    );
}

#[test]
fn prepared_insert_uses_defaults_for_omitted_columns() {
    let base = temp_dir("prepared_insert_defaults");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let mut catalog = Catalog::default();
    let mut desc = RelationDesc {
        columns: vec![
            crate::backend::catalog::catalog::column_desc(
                "id",
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4),
                false,
            ),
            crate::backend::catalog::catalog::column_desc(
                "note",
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Text),
                true,
            ),
        ],
    };
    desc.columns[1].default_expr = Some("'default note'".into());
    catalog.insert(
        "prepared_defaults",
        test_catalog_entry(
            crate::RelFileLocator {
                spc_oid: 0,
                db_oid: 1,
                rel_number: 15007,
            },
            desc.clone(),
        ),
    );
    crate::backend::catalog::store::sync_catalog_heaps_for_tests(&base, &catalog).unwrap();

    let smgr = MdStorageManager::new(&base);
    let pool = std::sync::Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 8));
    for name in catalog.table_names().collect::<Vec<_>>() {
        if let Some(entry) = catalog.get(&name) {
            create_fork(&*pool, entry.rel);
        }
    }
    let txns_arc = std::sync::Arc::new(parking_lot::RwLock::new(txns.clone()));
    let mut ctx = ExecutorContext {
        pool,
        txns: txns_arc,
        txn_waiter: None,
        sequences: Some(std::sync::Arc::new(
            crate::pgrust::database::SequenceRuntime::new_ephemeral(),
        )),
        large_objects: Some(std::sync::Arc::new(
            crate::pgrust::database::LargeObjectRuntime::new_ephemeral(),
        )),
        async_notify_runtime: None,
        advisory_locks: std::sync::Arc::new(
            crate::backend::storage::lmgr::AdvisoryLockManager::new(),
        ),
        row_locks: std::sync::Arc::new(crate::backend::storage::lmgr::RowLockManager::new()),
        checkpoint_stats: crate::backend::utils::misc::checkpoint::CheckpointStatsSnapshot::default(
        ),
        datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        interrupts: std::sync::Arc::new(
            crate::backend::utils::misc::interrupts::InterruptState::new(),
        ),
        stats: std::sync::Arc::new(parking_lot::RwLock::new(
            crate::pgrust::database::DatabaseStatsStore::with_default_io_rows(),
        )),
        session_stats: std::sync::Arc::new(parking_lot::RwLock::new(
            crate::pgrust::database::SessionStatsState::default(),
        )),
        snapshot: txns.snapshot(INVALID_TRANSACTION_ID).unwrap(),
        transaction_state: None,
        client_id: 77,
        current_database_name: "postgres".to_string(),
        session_user_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
        current_user_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
        active_role_oid: None,
        session_replication_role: Default::default(),
        statement_lock_scope_id: None,
        transaction_lock_scope_id: None,
        next_command_id: 0,
        default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
        expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
        case_test_values: Vec::new(),
        system_bindings: Vec::new(),
        subplans: Vec::new(),
        timed: false,
        allow_side_effects: true,
        pending_async_notifications: Vec::new(),
        catalog: catalog.materialize_visible_catalog(),
        compiled_functions: std::collections::HashMap::new(),
        cte_tables: std::collections::HashMap::new(),
        cte_producers: std::collections::HashMap::new(),
        recursive_worktables: std::collections::HashMap::new(),
        deferred_foreign_keys: None,
        trigger_depth: 0,
    };

    let prepared = crate::backend::parser::bind_insert_prepared(
        "prepared_defaults",
        Some(&["id".to_string()]),
        1,
        &catalog,
    )
    .unwrap();
    execute_prepared_insert_row(
        &prepared,
        &[Value::Int32(7)],
        &mut ctx,
        INVALID_TRANSACTION_ID,
        0,
    )
    .unwrap();

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "table prepared_defaults",
            catalog,
        )
        .unwrap(),
        vec![vec![Value::Int32(7), Value::Text("default note".into())]],
    );
}

#[test]
fn md5_supports_text_and_bytea_vectors() {
    let base = temp_dir("md5_vectors");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select md5(''), md5('abc'), md5(''::bytea), md5('abc'::bytea)",
        )
        .unwrap(),
        vec![vec![
            Value::Text("d41d8cd98f00b204e9800998ecf8427e".into()),
            Value::Text("900150983cd24fb0d6963f7d28e17f72".into()),
            Value::Text("d41d8cd98f00b204e9800998ecf8427e".into()),
            Value::Text("900150983cd24fb0d6963f7d28e17f72".into()),
        ]],
    );
}

#[test]
fn qualified_star_target_expands_relation_columns() {
    let mut harness = seed_people_and_pets("qualified_star_target");

    assert_query_rows(
        harness
            .execute(
                INVALID_TRANSACTION_ID,
                "select p.* from people p order by p.id",
            )
            .unwrap(),
        vec![
            vec![
                Value::Int32(1),
                Value::Text("alice".into()),
                Value::Text("a".into()),
            ],
            vec![
                Value::Int32(2),
                Value::Text("bob".into()),
                Value::Text("b".into()),
            ],
            vec![Value::Int32(3), Value::Text("carol".into()), Value::Null],
        ],
    );
}

#[test]
fn row_constructor_comparisons_expand_star_fields() {
    let mut harness = seed_people_and_pets("row_constructor_star_comparisons");

    assert_query_rows(
        harness
            .execute(
                INVALID_TRANSACTION_ID,
                "select row(p.*) = row(p.*), \
                        row(p.*) is distinct from row(p.*), \
                        row(p.*) is not distinct from row(p.*) \
                 from people p where p.id = 3",
            )
            .unwrap(),
        vec![vec![Value::Null, Value::Bool(false), Value::Bool(true)]],
    );
}

#[test]
fn comparison_operators_work_for_extended_numeric_types() {
    let base = temp_dir("extended_numeric_comparisons");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 1 <= 2, 2 >= 2, 3 != 4, 3 <> 3, 1.5 <= 1.5, 2.5 >= 3.5",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Bool(true),
                    Value::Bool(false),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn scientific_notation_literal_binds_as_float_value() {
    let base = temp_dir("scientific_notation_literal");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 1e2, 2.5e1").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Numeric("1e2".into()),
                    Value::Numeric("2.5e1".into())
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn numeric_literals_and_arithmetic_bind_as_numeric_values() {
    let base = temp_dir("numeric_literal_arithmetic");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 1.5, 2.5 + 2, 1e2 - 5",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Numeric("1.5".into()),
                    Value::Numeric("4.5".into()),
                    Value::Numeric("95".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn numeric_cast_typmod_rounds_to_scale() {
    let base = temp_dir("numeric_cast_typmod_rounds");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '12.345'::numeric(5,2)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, columns, .. } => {
            assert_eq!(
                columns[0].sql_type,
                crate::backend::parser::SqlType::with_numeric_precision_scale(5, 2)
            );
            assert_eq!(rows, vec![vec![Value::Numeric("12.35".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn numeric_cast_typmod_rejects_precision_overflow() {
    let base = temp_dir("numeric_cast_typmod_overflow");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '1234.56'::numeric(5,2)",
    )
    .unwrap_err();
    match err {
        ExecError::DetailedError {
            message,
            detail,
            sqlstate,
            ..
        } => {
            assert_eq!(message, "numeric field overflow");
            assert_eq!(
                detail.as_deref(),
                Some(
                    "A field with precision 5, scale 2 must round to an absolute value less than 10^3."
                )
            );
            assert_eq!(sqlstate, "22003");
        }
        other => panic!("expected detailed numeric typmod error, got {other:?}"),
    }
}

#[test]
fn numeric_typmod_insert_errors_include_postgres_details() {
    for (value_sql, expected_detail) in [
        (
            "'1.0'",
            "A field with precision 4, scale 4 must round to an absolute value less than 1.",
        ),
        (
            "'0.99995'",
            "A field with precision 4, scale 4 must round to an absolute value less than 1.",
        ),
        (
            "'Inf'",
            "A field with precision 4, scale 4 cannot hold an infinite value.",
        ),
        (
            "'-Inf'",
            "A field with precision 4, scale 4 cannot hold an infinite value.",
        ),
    ] {
        let value = Value::Text(value_sql.trim_matches('\'').into());
        match expr_casts::cast_value(
            value,
            crate::backend::parser::SqlType::with_numeric_precision_scale(4, 4),
        )
        .unwrap_err()
        {
            ExecError::DetailedError {
                message,
                detail,
                sqlstate,
                ..
            } => {
                assert_eq!(message, "numeric field overflow");
                assert_eq!(detail.as_deref(), Some(expected_detail));
                assert_eq!(sqlstate, "22003");
            }
            other => panic!("expected detailed numeric typmod error, got {other:?}"),
        }
    }
}

#[test]
fn sum_and_avg_bigint_promote_to_numeric() {
    let base = temp_dir("sum_avg_bigint_numeric");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select sum(x), avg(x) from unnest(ARRAY[1::int8, 2::int8, 3::int8]::int8[]) as u(x)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, columns, .. } => {
            assert_eq!(
                columns[0].sql_type,
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Numeric)
            );
            assert_eq!(
                columns[1].sql_type,
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Numeric)
            );
            assert_eq!(
                rows,
                vec![vec![Value::Numeric("6".into()), Value::Numeric("2".into())]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn sum_and_avg_numeric_preserve_postgres_display_scale() {
    let base = temp_dir("sum_avg_numeric");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select sum(x), avg(x) from unnest(ARRAY[1.5::numeric, 2.5::numeric]::numeric[]) as u(x)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, columns, .. } => {
            assert_eq!(
                columns[0].sql_type,
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Numeric)
            );
            assert_eq!(
                columns[1].sql_type,
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Numeric)
            );
            assert_eq!(
                rows,
                vec![vec![
                    Value::Numeric("4.0".into()),
                    Value::Numeric("2.0000000000000000".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn avg_numeric_preserves_postgres_display_scale() {
    let base = temp_dir("avg_numeric_display_scale");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select avg(x) from unnest(ARRAY[1.1000::numeric, 1.2000::numeric]::numeric[]) as u(x)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Numeric("1.15000000000000000000".into())]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn sum_real_and_avg_real_follow_postgres_result_types() {
    let base = temp_dir("sum_avg_real");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select sum(x), avg(x) from unnest(ARRAY[1.25::real, 2.5::real]::real[]) as u(x)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, columns, .. } => {
            assert_eq!(
                columns[0].sql_type,
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Float4)
            );
            assert_eq!(
                columns[1].sql_type,
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Float8)
            );
            assert_eq!(
                rows,
                vec![vec![Value::Float64(3.75), Value::Float64(1.875)]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn numeric_arithmetic_stays_exact_for_simple_decimals() {
    let base = temp_dir("numeric_exact_decimal_math");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select 0.1::numeric + 0.2::numeric, 1.2::numeric * 3::numeric, 1.25::numeric - 0.5::numeric",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![
                        Value::Numeric("0.3".into()),
                        Value::Numeric("3.6".into()),
                        Value::Numeric("0.75".into()),
                    ]]
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn numeric_ordering_compares_decimal_values_exactly() {
    let base = temp_dir("numeric_exact_ordering");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select 0.3::numeric = 0.30::numeric, 0.3::numeric > 0.29::numeric, 0.1::numeric + 0.2::numeric = 0.3::numeric",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Bool(true), Value::Bool(true), Value::Bool(true)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn integer_literals_widen_from_int4_to_int8_to_numeric() {
    let base = temp_dir("integer_literal_widening");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 2147483647, 2147483648, 9223372036854775808",
    )
    .unwrap()
    {
        StatementResult::Query { rows, columns, .. } => {
            assert_eq!(
                columns[0].sql_type,
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int4)
            );
            assert_eq!(
                columns[1].sql_type,
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Int8)
            );
            assert_eq!(
                columns[2].sql_type,
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Numeric)
            );
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int32(2147483647),
                    Value::Int64(2147483648),
                    Value::Numeric("9223372036854775808".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn float_and_numeric_special_values_parse() {
    let base = temp_dir("float_numeric_special_values");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'Infinity'::float8, '-Infinity'::float8, 'NaN'::numeric",
    )
    .unwrap()
    {
        StatementResult::Query { rows, columns, .. } => {
            assert_eq!(
                columns[0].sql_type,
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Float8)
            );
            assert_eq!(
                columns[1].sql_type,
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Float8)
            );
            assert_eq!(
                columns[2].sql_type,
                crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Numeric)
            );
            match &rows[0][0] {
                Value::Float64(v) => assert!(v.is_infinite() && *v > 0.0),
                other => panic!("expected positive infinity, got {:?}", other),
            }
            match &rows[0][1] {
                Value::Float64(v) => assert!(v.is_infinite() && *v < 0.0),
                other => panic!("expected negative infinity, got {:?}", other),
            }
            assert_eq!(rows[0][2], Value::Numeric("NaN".into()));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn numeric_special_values_and_extended_input_forms_parse() {
    let base = temp_dir("numeric_special_values_extended");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select 'Infinity'::numeric, '-inf'::numeric, 'NaN '::numeric, '0xFF'::numeric, '.000_000_000_123e1_0'::numeric",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows[0][0], Value::Numeric(crate::include::nodes::datum::NumericValue::PosInf));
                assert_eq!(rows[0][1], Value::Numeric(crate::include::nodes::datum::NumericValue::NegInf));
                assert_eq!(rows[0][2], Value::Numeric("NaN".into()));
                assert_eq!(rows[0][3], Value::Numeric("255".into()));
                assert_eq!(rows[0][4], Value::Numeric("1.23".into()));
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn numeric_prefixed_literals_allow_space_and_prefix_underscore() {
    let base = temp_dir("numeric_prefixed_literals_spaced");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '  -0B_1010  '::numeric, '  +0X_FF  '::numeric",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Numeric("-10".into()),
                    Value::Numeric("255".into())
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn values_can_reconcile_numeric_and_string_numeric_literals() {
    let base = temp_dir("values_numeric_string_literals");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "with v(x) as (values ('0'::numeric), ('inf'), ('-inf'), ('nan')) select x::text from v order by 1",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![
                        vec![Value::Text("-Infinity".into())],
                        vec![Value::Text("0".into())],
                        vec![Value::Text("Infinity".into())],
                        vec![Value::Text("NaN".into())],
                    ]
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn numeric_negative_scale_typmods_round_on_insert() {
    let base = temp_dir("numeric_negative_scale_typmod");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '123456'::numeric(3,-3), '123456789'::numeric(3,-6)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, columns, .. } => {
            assert_eq!(
                columns[0].sql_type,
                crate::backend::parser::SqlType::with_numeric_precision_scale(3, -3)
            );
            assert_eq!(
                columns[1].sql_type,
                crate::backend::parser::SqlType::with_numeric_precision_scale(3, -6)
            );
            assert_eq!(
                rows,
                vec![vec![
                    Value::Numeric("123000".into()),
                    Value::Numeric("123000000".into())
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn numeric_typmod_table_insert_rounds_values() {
    let db = Database::open(temp_dir("numeric_typmod_table_insert"), 16).unwrap();
    let mut session = Session::new(1);
    db.execute(
        1,
        "create table t (millions numeric(3, -6), thousands numeric(3, -3), units numeric(3, 0), thousandths numeric(3, 3), millionths numeric(3, 6))",
    )
    .unwrap();
    db.execute(
        1,
        "insert into t values (123456789, 123456, 123.456, 0.123456, 0.000123456)",
    )
    .unwrap();
    match session
        .execute(
            &db,
            "select millions, thousands, units, thousandths, millionths from t",
        )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Numeric("123000000".into()),
                    Value::Numeric("123000".into()),
                    Value::Numeric("123".into()),
                    Value::Numeric("0.123".into()),
                    Value::Numeric("0.000123".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn cte_self_join_aliases_keep_distinct_columns() {
    let base = temp_dir("cte_self_join_aliases");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "with v(x) as (values (1::numeric), (2::numeric), (3::numeric)) select x1, x2 from v as v1(x1), v as v2(x2) order by x1, x2",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Numeric("1".into()), Value::Numeric("1".into())],
                    vec![Value::Numeric("1".into()), Value::Numeric("2".into())],
                    vec![Value::Numeric("1".into()), Value::Numeric("3".into())],
                    vec![Value::Numeric("2".into()), Value::Numeric("1".into())],
                    vec![Value::Numeric("2".into()), Value::Numeric("2".into())],
                    vec![Value::Numeric("2".into()), Value::Numeric("3".into())],
                    vec![Value::Numeric("3".into()), Value::Numeric("1".into())],
                    vec![Value::Numeric("3".into()), Value::Numeric("2".into())],
                    vec![Value::Numeric("3".into()), Value::Numeric("3".into())],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn cte_filtered_self_join_aliases_keep_distinct_columns() {
    let base = temp_dir("cte_filtered_self_join_aliases");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "with v(x) as (values (0::numeric), (1::numeric), (2::numeric)) select x1, x2 from v as v1(x1), v as v2(x2) where x2 != 0 order by x1, x2",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Numeric("0".into()), Value::Numeric("1".into())],
                    vec![Value::Numeric("0".into()), Value::Numeric("2".into())],
                    vec![Value::Numeric("1".into()), Value::Numeric("1".into())],
                    vec![Value::Numeric("1".into()), Value::Numeric("2".into())],
                    vec![Value::Numeric("2".into()), Value::Numeric("1".into())],
                    vec![Value::Numeric("2".into()), Value::Numeric("2".into())],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn cte_filtered_self_join_uses_filtered_side_as_outer_order() {
    let base = temp_dir("cte_filtered_self_join_outer_order");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "with v(x) as (values (0::numeric), (1::numeric), (2::numeric)) select x1, x2 from v as v1(x1), v as v2(x2) where x2 != 0",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Numeric("0".into()), Value::Numeric("1".into())],
                    vec![Value::Numeric("1".into()), Value::Numeric("1".into())],
                    vec![Value::Numeric("2".into()), Value::Numeric("1".into())],
                    vec![Value::Numeric("0".into()), Value::Numeric("2".into())],
                    vec![Value::Numeric("1".into()), Value::Numeric("2".into())],
                    vec![Value::Numeric("2".into()), Value::Numeric("2".into())],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn values_filtered_self_join_keeps_distinct_columns() {
    let base = temp_dir("values_filtered_self_join");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select x, y from (values (0),(1),(2)) a(x), (values (0),(1),(2)) b(y) where y != 0 order by x, y",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(0), Value::Int32(1)],
                    vec![Value::Int32(0), Value::Int32(2)],
                    vec![Value::Int32(1), Value::Int32(1)],
                    vec![Value::Int32(1), Value::Int32(2)],
                    vec![Value::Int32(2), Value::Int32(1)],
                    vec![Value::Int32(2), Value::Int32(2)],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn from_functions_are_implicitly_lateral() {
    let base = temp_dir("from_functions_implicitly_lateral");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from generate_series(1::numeric, 3::numeric) i, generate_series(i, 3::numeric) j order by 1, 2",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Numeric("1".into()), Value::Numeric("1".into())],
                    vec![Value::Numeric("1".into()), Value::Numeric("2".into())],
                    vec![Value::Numeric("1".into()), Value::Numeric("3".into())],
                    vec![Value::Numeric("2".into()), Value::Numeric("2".into())],
                    vec![Value::Numeric("2".into()), Value::Numeric("3".into())],
                    vec![Value::Numeric("3".into()), Value::Numeric("3".into())],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn lateral_values_can_reference_left_columns() {
    let base = temp_dir("lateral_values_outer_columns");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select x, y from (values (1),(2)) a(x), lateral (values (x), (x + 1)) b(y) order by x, y",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1), Value::Int32(1)],
                    vec![Value::Int32(1), Value::Int32(2)],
                    vec![Value::Int32(2), Value::Int32(2)],
                    vec![Value::Int32(2), Value::Int32(3)],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn numeric_nan_division_by_zero_returns_nan() {
    let base = temp_dir("numeric_nan_division_by_zero");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'nan'::numeric / 0::numeric, 'nan'::numeric % 0::numeric, div('nan'::numeric, 0::numeric)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Numeric("NaN".into()),
                    Value::Numeric("NaN".into()),
                    Value::Numeric("NaN".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn numeric_scalar_helpers_follow_postgres_basics() {
    let base = temp_dir("numeric_scalar_helpers");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select round(42.4382::numeric, 2), trunc(42.4382::numeric, 2), div(4.2::numeric, 1::numeric), scale(0.00::numeric), scale(-13.000000000000000::numeric), min_scale(1.1000::numeric), trim_scale(1.120::numeric), mod(70.0::numeric, 70::numeric)",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![
                        Value::Numeric("42.44".into()),
                        Value::Numeric("42.43".into()),
                        Value::Numeric("4".into()),
                        Value::Int32(2),
                        Value::Int32(15),
                        Value::Int32(1),
                        Value::Numeric("1.12".into()),
                        Value::Numeric("0.0".into()),
                    ]]
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn numeric_gcd_and_lcm_preserve_postgres_display_scale() {
    let base = temp_dir("numeric_gcd_lcm_display_scale");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select gcd(4331.250::numeric, 463.75000::numeric), lcm(4232.820::numeric, 132.72000::numeric)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Numeric("8.75000".into()),
                    Value::Numeric("118518.96000".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn to_char_numeric_ignores_display_only_trailing_zeros() {
    let base = temp_dir("to_char_numeric_display_scale");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select to_char(4.31::numeric(210,10), 'FM9999999999999999.999999999999999'), to_char((-34338492.215397047)::numeric(210,10), 'FM9999999999999999.999999999999999PR')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("4.31".into()),
                    Value::Text("<34338492.215397047>".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn to_char_numeric_fill_mode_respects_integer_zero_masks() {
    let base = temp_dir("to_char_numeric_fill_mode_zero_masks");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select to_char(0::numeric(210,10), 'FM0999999999999999.999999999999999'), to_char(0::numeric(210,10), 'FM9999999999990999.990999999999999'), to_char(0::numeric(210,10), 'FM9999999999999999.099999999999999')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("0000000000000000.".into()),
                    Value::Text("0000.000".into()),
                    Value::Text(".0".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn width_bucket_supports_numeric_and_float_special_cases() {
    let base = temp_dir("width_bucket_numeric_float");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select width_bucket('Infinity'::numeric, 1::numeric, 10::numeric, 10), width_bucket('-Infinity'::numeric, 1::numeric, 10::numeric, 10), width_bucket(5.0::float8, 3.0::float8, 4.0::float8, 10), width_bucket(5.0::numeric, 3.0::numeric, 4.0::numeric, 10)",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![
                        Value::Int32(11),
                        Value::Int32(0),
                        Value::Int32(11),
                        Value::Int32(11),
                    ]]
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn width_bucket_rejects_invalid_numeric_domains() {
    let base = temp_dir("width_bucket_numeric_errors");
    let txns = TransactionManager::new_durable(&base).unwrap();
    for sql in [
        "select width_bucket(5.0::numeric, 3.0::numeric, 4.0::numeric, 0)",
        "select width_bucket('NaN'::numeric, 3.0::numeric, 4.0::numeric, 10)",
        "select width_bucket(0.0::numeric, 'Infinity'::numeric, 4.0::numeric, 10)",
    ] {
        assert!(matches!(
            run_sql(&base, &txns, INVALID_TRANSACTION_ID, sql).unwrap_err(),
            ExecError::InvalidStorageValue { .. }
        ));
    }
}

#[test]
fn width_bucket_float_handles_huge_range_boundaries() {
    let base = temp_dir("width_bucket_float_huge_ranges");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select width_bucket(0, -1e100::float8, 1, 10), width_bucket(1, 1e100::float8, 0, 10), width_bucket(10.5::float8, -1.797e308::float8, 1.797e308::float8, 2), width_bucket(10.5::float8, -1.797e308::float8, 1.797e308::float8, 3), width_bucket(10.5::float8, 1.797e308::float8, -1.797e308::float8, 2), width_bucket(10.5::float8, 1.797e308::float8, -1.797e308::float8, 3)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int32(10),
                    Value::Int32(10),
                    Value::Int32(2),
                    Value::Int32(2),
                    Value::Int32(2),
                    Value::Int32(2),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn numeric_math_misc_helpers_cover_log_factorial_and_pg_lsn() {
    let base = temp_dir("numeric_misc_helpers");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select numeric_inc(4.2::numeric), log(10::numeric), log10(10::numeric), log(2::numeric, 4.2::numeric), factorial(4::numeric), pg_lsn(23783416::numeric), ceil(-7.777::numeric), floor(-7.777::numeric), sign('-Infinity'::numeric)",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![
                        Value::Numeric("5.2".into()),
                        Value::Numeric("1.0000000000000000".into()),
                        Value::Numeric("1.0000000000000000".into()),
                        Value::Numeric("2.0703893278913979".into()),
                        Value::Numeric("24".into()),
                        Value::Text("0/16AE7F8".into()),
                        Value::Numeric("-7".into()),
                        Value::Numeric("-8".into()),
                        Value::Numeric("-1".into()),
                    ]]
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn numeric_transcendentals_match_postgres_reference_values() {
    let base = temp_dir("numeric_transcendental_reference_values");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select \
                exp(1.0::numeric(71,70)), \
                ln(0.99949452::numeric), \
                log(1.23e-89::numeric, 6.4689e45::numeric), \
                power(4.2::numeric, 4.2::numeric)",
        )
        .unwrap(),
        vec![vec![
            Value::Numeric(
                "2.7182818284590452353602874713526624977572470936999595749669676277240766"
                    .into(),
            ),
            Value::Numeric("-0.00050560779808326467".into()),
            Value::Numeric(
                "-0.5152489207781856983977054971756484879653568168479201885425588841094788842469115325262329756"
                    .into(),
            ),
            Value::Numeric("414.61691860129675".into()),
        ]],
    );
}

#[test]
fn numeric_exp_underflow_matches_postgres_zero_semantics() {
    let base = temp_dir("numeric_exp_underflow");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select \
                exp(-5000::numeric) = 0, \
                scale(exp(-5000::numeric)), \
                exp(-10000::numeric) = 0, \
                scale(exp(-10000::numeric)), \
                coalesce(nullif(exp(-5000::numeric), 0), 0), \
                coalesce(nullif(exp(-10000::numeric), 0), 0), \
                exp(32.999::numeric), \
                exp(-32.999::numeric)",
        )
        .unwrap(),
        vec![vec![
            Value::Bool(true),
            Value::Int32(1000),
            Value::Bool(true),
            Value::Int32(1000),
            Value::Numeric("0".into()),
            Value::Numeric("0".into()),
            Value::Numeric("214429043492155.053".into()),
            Value::Numeric("0.000000000000004663547361468248".into()),
        ]],
    );
}

#[test]
fn numeric_power_special_values_follow_postgres() {
    let base = temp_dir("numeric_power_special_values");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select \
                power('-1'::numeric, 'inf'::numeric), \
                power('-2'::numeric, 'inf'::numeric), \
                power('-2'::numeric, '-inf'::numeric), \
                power('-inf'::numeric, '3'::numeric), \
                power('inf'::numeric, '-2'::numeric), \
                power(1::numeric, 'nan'::numeric), \
                power('nan'::numeric, 0::numeric)",
        )
        .unwrap(),
        vec![vec![
            Value::Numeric("1".into()),
            Value::Numeric("Infinity".into()),
            Value::Numeric("0".into()),
            Value::Numeric("-Infinity".into()),
            Value::Numeric("0".into()),
            Value::Numeric("1".into()),
            Value::Numeric("1".into()),
        ]],
    );
}

#[test]
fn numeric_power_zero_exponents_with_fractional_scale_follow_postgres() {
    let base = temp_dir("numeric_power_zero_exponents");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select 0.0::numeric ^ 0.0::numeric, (-12.34)::numeric ^ 0.0::numeric, 12.34::numeric ^ 0.0::numeric, 0.0::numeric ^ 12.34::numeric",
        )
        .unwrap(),
        vec![vec![
            Value::Numeric("1.0000000000000000".into()),
            Value::Numeric("1.0000000000000000".into()),
            Value::Numeric("1.0000000000000000".into()),
            Value::Numeric("0.0000000000000000".into()),
        ]],
    );
}

#[test]
fn numeric_log_special_values_follow_postgres() {
    let base = temp_dir("numeric_log_special_values");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select \
                log('inf'::numeric, 2::numeric), \
                log(2::numeric, 'inf'::numeric), \
                log('inf'::numeric, 'inf'::numeric)",
        )
        .unwrap(),
        vec![vec![
            Value::Numeric("0".into()),
            Value::Numeric("Infinity".into()),
            Value::Numeric("NaN".into()),
        ]],
    );
}

#[test]
fn float_nan_comparisons_follow_postgres_ordering() {
    let base = temp_dir("float_nan_comparisons");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select 'NaN'::float8 = 'NaN'::float8, 'NaN'::float8 > 1.0::float8, 1.0::float8 < 'NaN'::float8",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Bool(true), Value::Bool(true), Value::Bool(true)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn order_by_places_float_nan_after_finite_values() {
    let base = temp_dir("float_nan_order_by");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select x from unnest(ARRAY[1.0::float8, 'NaN'::float8, 2.0::float8]::float8[]) as u(x) order by x",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows[0], vec![Value::Float64(1.0)]);
                assert_eq!(rows[1], vec![Value::Float64(2.0)]);
                match rows[2][0] {
                    Value::Float64(v) => assert!(v.is_nan()),
                    ref other => panic!("expected NaN float row, got {:?}", other),
                }
            }
            other => panic!("expected query result, got {:?}", other),
        }
}
#[test]
fn all_array_semantics_match_empty_false_and_null_cases() {
    let base = temp_dir("all_array_semantics");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 1 < all(ARRAY[2, 3]), 1 < all(ARRAY[]::int4[]), 3 < all(ARRAY[2, null]::int4[]), 1 < all(ARRAY[2, null]::int4[])").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(true), Value::Bool(true), Value::Bool(false), Value::Null]]); } other => panic!("expected query result, got {:?}", other), }
}
#[test]
fn any_array_empty_and_null_array_cases() {
    let base = temp_dir("any_array_empty_null");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 1 = any(ARRAY[]::int4[]), 1 = any((null)::int4[]), (null)::int4 = any(ARRAY[1]::int4[])").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(false), Value::Null, Value::Null]]); } other => panic!("expected query result, got {:?}", other), }
}
#[test]
fn array_overlap_false_and_null_cases() {
    let base = temp_dir("array_overlap_false_null");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select ARRAY['a']::varchar[] && ARRAY['b']::varchar[], ARRAY['a', null]::varchar[] && ARRAY['b', null]::varchar[], ARRAY['a']::varchar[] && (null)::varchar[]").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(false), Value::Bool(false), Value::Null]]); } other => panic!("expected query result, got {:?}", other), }
}

#[test]
fn array_contains_and_contained_match_postgres_cases() {
    let base = temp_dir("array_contains_contained");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select ARRAY[1,2,3] @> ARRAY[2], ARRAY[1,2,3] @> ARRAY[4], ARRAY[1,2,3] @> ARRAY[]::int4[], ARRAY[1,null]::int4[] @> ARRAY[null]::int4[], ARRAY[2] <@ ARRAY[1,2,3], ARRAY[]::int4[] <@ ARRAY[null]::int4[]",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Bool(true),
                    Value::Bool(true),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn array_slice_omitted_upper_and_mixed_slice_shape_work() {
    let base = temp_dir("array_slice_shape");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select ('{1,2,3}'::int[])[2:], ('{1,2,3}'::int[])[:], ('{{1,2,3},{4,5,6},{7,8,9}}'::int[])[1:2][2]",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::PgArray(crate::include::nodes::datum::ArrayValue::from_dimensions(
                        vec![crate::include::nodes::datum::ArrayDimension {
                            lower_bound: 1,
                            length: 2,
                        }],
                        vec![Value::Int32(2), Value::Int32(3)],
                    )),
                    Value::PgArray(crate::include::nodes::datum::ArrayValue::from_dimensions(
                        vec![crate::include::nodes::datum::ArrayDimension {
                            lower_bound: 1,
                            length: 3,
                        }],
                        vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)],
                    )),
                    Value::PgArray(crate::include::nodes::datum::ArrayValue::from_dimensions(
                        vec![
                            crate::include::nodes::datum::ArrayDimension {
                                lower_bound: 1,
                                length: 2,
                            },
                            crate::include::nodes::datum::ArrayDimension {
                                lower_bound: 1,
                                length: 2,
                            },
                        ],
                        vec![
                            Value::Int32(1),
                            Value::Int32(2),
                            Value::Int32(4),
                            Value::Int32(5),
                        ],
                    )),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn bound_aware_array_comparison_and_overlap_follow_array_ordering() {
    let base = temp_dir("array_literal_compare");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select ARRAY[1,2] = '{1,2}'::int[], ARRAY[1,2] && '{2,3}'::int[], ARRAY[1] < '[2:2]={1}'::int[]",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Bool(true), Value::Bool(true), Value::Bool(true)]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn array_equality_and_inequality_work_for_same_type_arrays() {
    let base = temp_dir("array_equality_ops");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select ARRAY[1, 2] = ARRAY[1, 2], ARRAY[1, 2] <> ARRAY[2, 1], ARRAY['a']::varchar[] = ARRAY['a']::varchar[]").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(true), Value::Bool(true), Value::Bool(true)]]); } other => panic!("expected query result, got {:?}", other), }
}

#[test]
fn unknown_string_literals_coerce_to_array_types_in_comparisons() {
    let base = temp_dir("array_unknown_literal_compare");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select ARRAY[1,2] = '{}', ARRAY[NULL]::int[] = '{NULL}', 2 = any ('{1,2,3}')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Bool(false),
                    Value::Bool(true),
                    Value::Bool(true)
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn malformed_array_literals_report_array_input_errors() {
    let base = temp_dir("array_malformed_input");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '{1,}'::text[]",
    )
    .unwrap_err();
    assert_eq!(format_exec_error(&err), "malformed array literal: \"{1,}\"");

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '[2]={1}'::int[]",
    )
    .unwrap_err();
    assert_eq!(
        format_exec_error(&err),
        "malformed array literal: \"[2]={1}\""
    );
}

#[test]
fn typed_empty_array_selects_as_empty_value() {
    let base = temp_dir("typed_empty_array");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select ARRAY[]::varchar[]",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::PgArray(
                    crate::include::nodes::datum::ArrayValue::empty()
                )]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn unnest_null_and_empty_arrays_return_no_rows() {
    let base = temp_dir("unnest_null_empty");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from unnest((null)::int4[])",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => assert!(rows.is_empty()),
        other => panic!("expected query result, got {:?}", other),
    }
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from unnest(ARRAY[]::int4[])",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => assert!(rows.is_empty()),
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn unnest_null_array_zips_with_longer_input() {
    let base = temp_dir("unnest_null_zip");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from unnest((null)::int4[], ARRAY['x', 'y']::varchar[]) as u(a, b)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Null, Value::Text("x".into())],
                    vec![Value::Null, Value::Text("y".into())]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn generate_series_column_alias() {
    let base = temp_dir("gen_series_alias");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select val from generate_series(1, 3) as g(val)",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["val"]);
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1)],
                    vec![Value::Int32(2)],
                    vec![Value::Int32(3)]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn generate_series_column_alias_in_where() {
    let base = temp_dir("gen_series_alias_where");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select val from generate_series(1, 5) as g(val) where val > 3",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["val"]);
            assert_eq!(rows, vec![vec![Value::Int32(4)], vec![Value::Int32(5)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn generate_series_table_alias_only() {
    let base = temp_dir("gen_series_table_alias");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from generate_series(1, 3) as g",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["g"]);
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1)],
                    vec![Value::Int32(2)],
                    vec![Value::Int32(3)]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn generate_series_alias_without_as_keyword() {
    let base = temp_dir("gen_series_no_as");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select i from generate_series(1, 3) g(i)",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["i"]);
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1)],
                    vec![Value::Int32(2)],
                    vec![Value::Int32(3)]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn generate_series_table_alias_qualifies_column() {
    let base = temp_dir("gen_series_qualify");
    let txns = TransactionManager::new_durable(&base).unwrap();
    // Use the table alias to qualify the column reference: g.val
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select g.val from generate_series(1, 3) as g(val)",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["val"]);
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1)],
                    vec![Value::Int32(2)],
                    vec![Value::Int32(3)]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn select_from_derived_table() {
    let base = temp_dir("derived_table_basic");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'alpha'), (2, 'bob', null)",
    )
    .unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select p.id from (select id from people) p order by p.id",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["id"]);
            assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn select_from_derived_table_with_bare_target_alias() {
    let base = temp_dir("derived_table_bare_target_alias");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'alpha'), (2, 'bob', null)",
    )
    .unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select p.user_id from (select id user_id from people) p order by p.user_id",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["user_id"]);
            assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(2)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn select_from_aliasless_derived_table() {
    let base = temp_dir("derived_table_no_alias");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'alpha')",
    )
    .unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select id from (select id from people)",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["id"]);
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn derived_table_column_aliases_rename_output() {
    let base = temp_dir("derived_table_alias_cols");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'alpha')",
    )
    .unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select p.x from (select id from people) p(x)",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["x"]);
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn derived_table_partial_column_aliases_preserve_remaining_names() {
    let base = temp_dir("derived_table_alias_partial");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'alpha')",
    )
    .unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select p.x, p.name from (select id, name from people) p(x) order by p.x",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["x", "name"]);
            assert_eq!(
                rows,
                vec![vec![Value::Int32(1), Value::Text("alice".into())]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn derived_table_cross_join_column_aliases_lower_without_setrefs_panic() {
    let base = temp_dir("derived_table_cross_join_alias_cols");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let pool = test_pool_with_pets(&base);
    let xid = txns.begin();
    for row in [
        tuple(1, "alice", Some("alpha")),
        tuple(2, "bob", None),
        tuple(3, "carol", Some("storage")),
    ] {
        let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &row).unwrap();
        heap_flush(&*pool, 1, rel(), tid.block_number).unwrap();
    }
    for row in [pet_tuple(10, "Kitchen", 2), pet_tuple(11, "Mocha", 3)] {
        let tid = heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &row).unwrap();
        heap_flush(&*pool, 1, pets_rel(), tid.block_number).unwrap();
    }
    txns.commit(xid).unwrap();
    match run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select ii, tt, kk from (people cross join pets) as tx (ii, jj, tt, ii2, kk) order by ii, kk",
        catalog_with_pets(),
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["ii", "tt", "kk"]);
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Int32(1),
                        Value::Text("alpha".into()),
                        Value::Text("Kitchen".into()),
                    ],
                    vec![
                        Value::Int32(1),
                        Value::Text("alpha".into()),
                        Value::Text("Mocha".into()),
                    ],
                    vec![Value::Int32(2), Value::Null, Value::Text("Kitchen".into())],
                    vec![Value::Int32(2), Value::Null, Value::Text("Mocha".into())],
                    vec![
                        Value::Int32(3),
                        Value::Text("storage".into()),
                        Value::Text("Kitchen".into()),
                    ],
                    vec![
                        Value::Int32(3),
                        Value::Text("storage".into()),
                        Value::Text("Mocha".into()),
                    ],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn join_against_derived_table_returns_matching_rows() {
    let base = temp_dir("join_derived_table");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let pool = test_pool_with_pets(&base);
    let xid = txns.begin();
    for row in [
        tuple(1, "alice", Some("alpha")),
        tuple(2, "bob", None),
        tuple(3, "carol", Some("storage")),
    ] {
        let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &row).unwrap();
        heap_flush(&*pool, 1, rel(), tid.block_number).unwrap();
    }
    for row in [pet_tuple(10, "Kitchen", 2), pet_tuple(11, "Mocha", 3)] {
        let tid = heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &row).unwrap();
        heap_flush(&*pool, 1, pets_rel(), tid.block_number).unwrap();
    }
    txns.commit(xid).unwrap();
    match run_sql_with_catalog(&base, &txns, INVALID_TRANSACTION_ID, "select p.name, q.owner_id from people p join (select owner_id from pets) q on p.id = q.owner_id order by q.owner_id", catalog_with_pets()).unwrap() {
            StatementResult::Query { column_names, rows, .. } => {
                assert_eq!(column_names, vec!["name", "owner_id"]);
                assert_eq!(rows, vec![
                    vec![Value::Text("bob".into()), Value::Int32(2)],
                    vec![Value::Text("carol".into()), Value::Int32(3)],
                ]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
}
#[test]
fn derived_table_can_cross_join_with_generate_series() {
    let base = temp_dir("derived_table_cross_srf");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'alpha'), (2, 'bob', null)",
    )
    .unwrap();
    txns.commit(xid).unwrap();
    match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select p.id, g.g from (select id from people) p, generate_series(1, 2) g order by p.id, g.g").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![
                    vec![Value::Int32(1), Value::Int32(1)],
                    vec![Value::Int32(1), Value::Int32(2)],
                    vec![Value::Int32(2), Value::Int32(1)],
                    vec![Value::Int32(2), Value::Int32(2)],
                ]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
}
#[test]
fn generate_series_sources_can_cross_join_each_other() {
    let base = temp_dir("srf_cross_join");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select g.g, h.h from generate_series(1, 2) g, generate_series(5, 6) h order by g.g, h.h",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1), Value::Int32(5)],
                    vec![Value::Int32(1), Value::Int32(6)],
                    vec![Value::Int32(2), Value::Int32(5)],
                    vec![Value::Int32(2), Value::Int32(6)],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn select_list_generate_series_expands_rows() {
    let base = temp_dir("project_set_generate_series");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select generate_series(1, 3)",
        )
        .unwrap(),
        vec![
            vec![Value::Int32(1)],
            vec![Value::Int32(2)],
            vec![Value::Int32(3)],
        ],
    );
}

#[test]
fn select_list_generate_series_promotes_integer_bounds_with_numeric_step() {
    let base = temp_dir("project_set_generate_series_numeric_step");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select generate_series(0, 1, 0.3)",
        )
        .unwrap(),
        vec![
            vec![Value::Numeric("0".into())],
            vec![Value::Numeric("0.3".into())],
            vec![Value::Numeric("0.6".into())],
            vec![Value::Numeric("0.9".into())],
        ],
    );
}

#[test]
fn select_list_unnest_expands_rows() {
    let base = temp_dir("project_set_unnest");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select unnest(ARRAY[1, 2, 3])",
        )
        .unwrap(),
        vec![
            vec![Value::Int32(1)],
            vec![Value::Int32(2)],
            vec![Value::Int32(3)],
        ],
    );
}

#[test]
fn select_list_multi_arg_unnest_is_rejected() {
    let base = temp_dir("project_set_multi_arg_unnest");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select unnest(ARRAY[1, 2], ARRAY['x', 'y']::varchar[])",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Parse(ParseError::UnexpectedToken { expected, .. })
            if expected == "single-argument unnest(array_expr) in select list"
    ));
}

#[test]
fn select_list_json_scalar_srfs_work() {
    let base = temp_dir("project_set_json");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select json_object_keys('{\"a\":1,\"b\":2}'::json)",
        )
        .unwrap(),
        vec![vec![Value::Text("a".into())], vec![Value::Text("b".into())]],
    );
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select json_array_elements_text('[1,true,null]'::json)",
        )
        .unwrap(),
        vec![
            vec![Value::Text("1".into())],
            vec![Value::Text("true".into())],
            vec![Value::Null],
        ],
    );
}

#[test]
fn select_list_srfs_run_in_lockstep() {
    let base = temp_dir("project_set_lockstep");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select generate_series(1, 2), unnest(ARRAY['a', 'b', 'c']::varchar[]) order by 1, 2",
        )
        .unwrap(),
        vec![
            vec![Value::Int32(1), Value::Text("a".into())],
            vec![Value::Int32(2), Value::Text("b".into())],
            vec![Value::Null, Value::Text("c".into())],
        ],
    );
}

#[test]
fn select_list_json_each_returns_record_value() {
    let base = temp_dir("project_set_composite_json");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select json_each('{\"a\":1}'::json)",
        )
        .unwrap(),
        vec![vec![Value::Record(RecordValue::anonymous(vec![
            ("key".into(), Value::Text("a".into())),
            ("value".into(), Value::Json("1".into())),
        ]))]],
    );
}

#[test]
fn select_list_jsonb_each_field_select_projects_column() {
    let base = temp_dir("project_set_composite_json_field");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select (jsonb_each('{\"a\":1,\"b\":null}')).key order by 1",
        )
        .unwrap(),
        vec![vec![Value::Text("a".into())], vec![Value::Text("b".into())]],
    );
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select key, count(*) from (select (jsonb_each('{\"a\":1,\"b\":null}')).key) wow group by key order by key",
        )
        .unwrap(),
        vec![
            vec![Value::Text("a".into()), Value::Int64(1)],
            vec![Value::Text("b".into()), Value::Int64(1)],
        ],
    );
}

#[test]
fn jsonb_record_expansion_functions_work() {
    let base = temp_dir("jsonb_record_expansion");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from jsonb_populate_record(null::record, '{\"x\":776}') as q(x int, y int)",
        )
        .unwrap(),
        vec![vec![Value::Int32(776), Value::Null]],
    );
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from jsonb_to_record('{\"a\":1,\"b\":\"foo\"}') as x(a int, b text)",
        )
        .unwrap(),
        vec![vec![Value::Int32(1), Value::Text("foo".into())]],
    );
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select jsonb_populate_record(row(1,2), '{\"f1\":0,\"f2\":1}')",
        )
        .unwrap(),
        vec![vec![Value::Record(RecordValue::anonymous(vec![
            ("f1".into(), Value::Int32(0)),
            ("f2".into(), Value::Int32(1)),
        ]))]],
    );
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select jsonb_populate_recordset(row(1,2), '[{\"f1\":0},{\"f2\":3}]')",
        )
        .unwrap(),
        vec![
            vec![Value::Record(RecordValue::anonymous(vec![
                ("f1".into(), Value::Int32(0)),
                ("f2".into(), Value::Int32(2)),
            ]))],
            vec![Value::Record(RecordValue::anonymous(vec![
                ("f1".into(), Value::Int32(1)),
                ("f2".into(), Value::Int32(3)),
            ]))],
        ],
    );
}

#[test]
fn jsonb_populate_record_valid_checks_conversion_errors() {
    let base = temp_dir("jsonb_populate_record_valid");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select jsonb_populate_record_valid(row(1,2), '{\"f1\":0,\"f2\":1}'), \
                    jsonb_populate_record_valid(row(1,2), '{\"f1\":[1]}')",
        )
        .unwrap(),
        vec![vec![Value::Bool(true), Value::Bool(false)]],
    );
}
#[test]
fn join_alias_hides_inner_relation_names() {
    let base = temp_dir("join_alias_hides_inner");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let pool = test_pool_with_pets(&base);
    let xid = txns.begin();
    let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &tuple(1, "alice", Some("alpha"))).unwrap();
    heap_flush(&*pool, 1, rel(), tid.block_number).unwrap();
    let tid = heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &pet_tuple(10, "Kitchen", 1)).unwrap();
    heap_flush(&*pool, 1, pets_rel(), tid.block_number).unwrap();
    txns.commit(xid).unwrap();
    let err = run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select p.name from (people p join pets q on p.id = q.owner_id) j",
        catalog_with_pets(),
    )
    .unwrap_err();
    assert!(
        matches!(err, ExecError::Parse(ParseError::InvalidFromClauseReference(name)) if name == "p")
    );
}

#[test]
fn ambiguous_cross_join_column_reports_ambiguity() {
    let mut harness = seed_people_and_pets("ambiguous_cross_join_column");
    let err = harness
        .execute(
            INVALID_TRANSACTION_ID,
            "select id from people cross join pets",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Parse(ParseError::AmbiguousColumn(name)) if name == "id"
    ));
}

#[test]
fn join_using_alias_preserves_base_table_visibility() {
    let base = temp_dir("join_using_alias_base_visibility");
    let txns = TransactionManager::new_durable(&base).unwrap();
    run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'b')",
        catalog_with_pets(),
    )
    .unwrap();
    run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "insert into pets (id, name, owner_id) values (1, 'mocha', 9), (2, 'pixel', 8)",
        catalog_with_pets(),
    )
    .unwrap();
    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select people.name, x.id from people join pets using (id) as x where people.name = 'alice'",
            catalog_with_pets(),
        )
        .unwrap(),
        vec![vec![Value::Text("alice".into()), Value::Int32(1)]],
    );
}

#[test]
fn join_using_alias_hides_non_merged_columns() {
    let mut harness = seed_people_and_pets("join_using_alias_hides_non_merged");
    let err = harness
        .execute(
            INVALID_TRANSACTION_ID,
            "select x.name from people join pets using (id) as x",
        )
        .unwrap_err();
    assert!(matches!(err, ExecError::Parse(ParseError::UnknownColumn(name)) if name == "x.name"));
}

#[test]
fn parenthesized_join_alias_reports_invalid_from_clause_reference() {
    let mut harness = seed_people_and_pets("parenthesized_join_alias_invalid_ref");
    let err = harness
        .execute(
            INVALID_TRANSACTION_ID,
            "select * from (people p join pets q on p.id = q.owner_id) j where p.id = 1",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Parse(ParseError::InvalidFromClauseReference(name)) if name == "p"
    ));
}

#[test]
fn wrapped_join_alias_reports_missing_from_clause_entry() {
    let mut harness = seed_people_and_pets("wrapped_join_alias_missing_ref");
    let err = harness
        .execute(
            INVALID_TRANSACTION_ID,
            "select * from (people join pets on people.id = pets.owner_id as x) xx where x.id = 1",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Parse(ParseError::MissingFromClauseEntry(name)) if name == "x"
    ));
}

#[test]
fn join_alias_rejects_duplicate_table_name() {
    let mut harness = seed_people_and_pets("join_alias_duplicate_name");
    let err = harness
        .execute(
            INVALID_TRANSACTION_ID,
            "select * from people a1 join pets a2 using (id) as a1",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ExecError::Parse(ParseError::DuplicateTableName(name)) if name == "a1"
    ));
}

#[test]
fn join_using_projects_merged_column_once() {
    let base = temp_dir("join_using_projection");
    let txns = TransactionManager::new_durable(&base).unwrap();
    run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'b')",
        catalog_with_pets(),
    )
    .unwrap();
    run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "insert into pets (id, name, owner_id) values (1, 'mocha', 1), (3, 'pixel', 2)",
        catalog_with_pets(),
    )
    .unwrap();

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from people join pets using (id) order by 1, 2, 3, 4, 5",
            catalog_with_pets(),
        )
        .unwrap(),
        vec![vec![
            Value::Int32(1),
            Value::Text("alice".into()),
            Value::Text("a".into()),
            Value::Text("mocha".into()),
            Value::Int32(1),
        ]],
    );
}

#[test]
fn grouped_join_using_counts_rhs_values() {
    let base = temp_dir("grouped_join_using_counts_rhs_values");
    let txns = TransactionManager::new_durable(&base).unwrap();
    run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'b')",
        catalog_with_pets(),
    )
    .unwrap();
    run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "insert into pets (id, name, owner_id) values (1, 'mocha', 1), (3, 'pixel', 2)",
        catalog_with_pets(),
    )
    .unwrap();

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select id, count(owner_id) from people left join pets using (id) group by id order by id",
            catalog_with_pets(),
        )
        .unwrap(),
        vec![
            vec![Value::Int32(1), Value::Int64(1)],
            vec![Value::Int32(2), Value::Int64(0)],
        ],
    );
}

#[test]
fn full_join_using_coalesces_join_column() {
    let base = temp_dir("full_join_using");
    let txns = TransactionManager::new_durable(&base).unwrap();
    run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'b')",
        catalog_with_pets(),
    )
    .unwrap();
    run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "insert into pets (id, name, owner_id) values (1, 'mocha', 1), (3, 'pixel', 2)",
        catalog_with_pets(),
    )
    .unwrap();

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select id, people.name, pets.name from people full join pets using (id) order by 1, 2, 3",
            catalog_with_pets(),
        )
        .unwrap(),
        vec![
            vec![
                Value::Int32(1),
                Value::Text("alice".into()),
                Value::Text("mocha".into()),
            ],
            vec![Value::Int32(2), Value::Text("bob".into()), Value::Null],
            vec![Value::Int32(3), Value::Null, Value::Text("pixel".into())],
        ],
    );
}

fn seed_join_chain_tables(base: &PathBuf, txns: &mut TransactionManager) {
    let xid = txns.begin();
    run_sql_with_catalog(
        base,
        txns,
        xid,
        "insert into t1 (name, n) values ('bb', 11)",
        join_chain_catalog(),
    )
    .unwrap();
    run_sql_with_catalog(
        base,
        txns,
        xid,
        "insert into t2 (name, n) values ('bb', 12), ('cc', 22), ('ee', 42)",
        join_chain_catalog(),
    )
    .unwrap();
    run_sql_with_catalog(
        base,
        txns,
        xid,
        "insert into t3 (name, n) values ('bb', 13), ('cc', 23), ('dd', 33)",
        join_chain_catalog(),
    )
    .unwrap();
    txns.commit(xid).unwrap();
}

#[test]
fn chained_full_join_using_keeps_merged_name_identity() {
    let base = temp_dir("chained_full_join_using");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    seed_join_chain_tables(&base, &mut txns);

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from t1 full join t2 using (name) full join t3 using (name) order by name",
            join_chain_catalog(),
        )
        .unwrap(),
        vec![
            vec![
                Value::Text("bb".into()),
                Value::Int32(11),
                Value::Int32(12),
                Value::Int32(13),
            ],
            vec![
                Value::Text("cc".into()),
                Value::Null,
                Value::Int32(22),
                Value::Int32(23),
            ],
            vec![
                Value::Text("dd".into()),
                Value::Null,
                Value::Null,
                Value::Int32(33),
            ],
            vec![
                Value::Text("ee".into()),
                Value::Null,
                Value::Int32(42),
                Value::Null,
            ],
        ],
    );
}

#[test]
fn derived_table_inner_join_using_rebinds_to_distinct_inputs() {
    let base = temp_dir("derived_table_inner_join_using");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    seed_join_chain_tables(&base, &mut txns);

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from (select * from t2) as s2 inner join (select * from t3) s3 using (name) order by name, 2, 3",
            join_chain_catalog(),
        )
        .unwrap(),
        vec![
            vec![
                Value::Text("bb".into()),
                Value::Int32(12),
                Value::Int32(13),
            ],
            vec![
                Value::Text("cc".into()),
                Value::Int32(22),
                Value::Int32(23),
            ],
        ],
    );
}

#[test]
fn chained_natural_full_join_over_subqueries_keeps_join_outputs() {
    let base = temp_dir("chained_natural_full_join_subqueries");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    seed_join_chain_tables(&base, &mut txns);

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from (select name, n as s1_n, 1 as s1_1 from t1) as s1 natural full join (select name, n as s2_n, 2 as s2_2 from t2) as s2 natural full join (select name, n as s3_n, 3 as s3_2 from t3) s3 order by name",
            join_chain_catalog(),
        )
        .unwrap(),
        vec![
            vec![
                Value::Text("bb".into()),
                Value::Int32(11),
                Value::Int32(1),
                Value::Int32(12),
                Value::Int32(2),
                Value::Int32(13),
                Value::Int32(3),
            ],
            vec![
                Value::Text("cc".into()),
                Value::Null,
                Value::Null,
                Value::Int32(22),
                Value::Int32(2),
                Value::Int32(23),
                Value::Int32(3),
            ],
            vec![
                Value::Text("dd".into()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Int32(33),
                Value::Int32(3),
            ],
            vec![
                Value::Text("ee".into()),
                Value::Null,
                Value::Null,
                Value::Int32(42),
                Value::Int32(2),
                Value::Null,
                Value::Null,
            ],
        ],
    );
}

#[test]
fn sql_visible_coalesce_returns_first_non_null_value() {
    let base = temp_dir("sql_visible_coalesce");
    let txns = TransactionManager::new_durable(&base).unwrap();
    run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "insert into people (id, name, note) values (1, 'alice', null), (2, 'bob', 'b')",
        catalog_with_pets(),
    )
    .unwrap();

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select coalesce(note, name, 'fallback') from people order by id",
            catalog_with_pets(),
        )
        .unwrap(),
        vec![
            vec![Value::Text("alice".into())],
            vec![Value::Text("b".into())],
        ],
    );
}

#[test]
fn sql_visible_coalesce_supports_common_numeric_type() {
    let base = temp_dir("sql_visible_coalesce_numeric");
    let txns = TransactionManager::new_durable(&base).unwrap();
    run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'b')",
        catalog_with_pets(),
    )
    .unwrap();

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select coalesce(null, id, 7) from people order by id",
            catalog_with_pets(),
        )
        .unwrap(),
        vec![vec![Value::Int32(1)], vec![Value::Int32(2)]],
    );
}

#[test]
fn sql_visible_coalesce_accepts_single_argument() {
    let base = temp_dir("sql_visible_coalesce_single_arg");
    let txns = TransactionManager::new_durable(&base).unwrap();
    run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "insert into people (id, name, note) values (1, 'alice', null), (2, 'bob', 'b')",
        catalog_with_pets(),
    )
    .unwrap();

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select coalesce(note) from people order by id",
            catalog_with_pets(),
        )
        .unwrap(),
        vec![vec![Value::Null], vec![Value::Text("b".into())]],
    );
}

#[test]
fn left_join_on_emits_null_extended_rows() {
    let mut harness = seed_people_and_pets("left_join_on");

    assert_query_rows(
        harness
            .execute(
                INVALID_TRANSACTION_ID,
                "select people.id, pets.id from people left join pets on people.id = pets.owner_id order by 1, 2",
            )
            .unwrap(),
        vec![
            vec![Value::Int32(1), Value::Int32(10)],
            vec![Value::Int32(1), Value::Int32(11)],
            vec![Value::Int32(2), Value::Int32(12)],
            vec![Value::Int32(3), Value::Null],
        ],
    );
}

#[test]
fn cross_join_limit_respects_order_by_after_reordering() {
    let mut harness = seed_people_and_pets("cross_join_row_order");

    assert_query_rows(
        harness
            .execute(
                INVALID_TRANSACTION_ID,
                "select people.id, pets.id from people, pets order by pets.id, people.id limit 6",
            )
            .unwrap(),
        vec![
            vec![Value::Int32(1), Value::Int32(10)],
            vec![Value::Int32(2), Value::Int32(10)],
            vec![Value::Int32(3), Value::Int32(10)],
            vec![Value::Int32(1), Value::Int32(11)],
            vec![Value::Int32(2), Value::Int32(11)],
            vec![Value::Int32(3), Value::Int32(11)],
        ],
    );
}
#[test]
fn non_lateral_derived_table_rejects_outer_refs() {
    let base = temp_dir("derived_table_outer_ref");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from people p, (select p.id from people) q",
    )
    .unwrap_err();
    assert!(matches!(err, ExecError::Parse(ParseError::UnknownColumn(name)) if name == "p.id"));
}

#[test]
fn lateral_full_join_with_multiple_outer_refs_rangefuncs_shape() {
    let base = temp_dir("rangefuncs_lateral_full_join_multi_outer_refs");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let sql = r#"
select *
from (values (1),(2)) v1(r1)
    left join lateral (
        select *
        from generate_series(1, v1.r1) as gs1
        left join lateral (
            select *
            from generate_series(1, gs1) as gs2
            left join generate_series(1, gs2) as gs3 on true
        ) as ss1 on true
        full join generate_series(1, v1.r1) as gs4 on false
    ) as ss0 on true
"#;
    match run_sql(&base, &txns, INVALID_TRANSACTION_ID, sql).unwrap() {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["r1", "gs1", "gs2", "gs3", "gs4"]);
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Int32(1),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Int32(1),
                    ],
                    vec![
                        Value::Int32(1),
                        Value::Int32(1),
                        Value::Int32(1),
                        Value::Int32(1),
                        Value::Null,
                    ],
                    vec![
                        Value::Int32(2),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Int32(1),
                    ],
                    vec![
                        Value::Int32(2),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Int32(2),
                    ],
                    vec![
                        Value::Int32(2),
                        Value::Int32(1),
                        Value::Int32(1),
                        Value::Int32(1),
                        Value::Null,
                    ],
                    vec![
                        Value::Int32(2),
                        Value::Int32(2),
                        Value::Int32(1),
                        Value::Int32(1),
                        Value::Null,
                    ],
                    vec![
                        Value::Int32(2),
                        Value::Int32(2),
                        Value::Int32(2),
                        Value::Int32(1),
                        Value::Null,
                    ],
                    vec![
                        Value::Int32(2),
                        Value::Int32(2),
                        Value::Int32(2),
                        Value::Int32(2),
                        Value::Null,
                    ],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn derived_table_alias_preserved_for_empty_result() {
    let base = temp_dir("derived_table_empty_alias");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select p.x from (select id from people where id > 10) p(x)",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["x"]);
            assert!(rows.is_empty());
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn parenthesized_join_alias_can_be_selected_from() {
    let base = temp_dir("parenthesized_join_alias");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let pool = test_pool_with_pets(&base);
    let xid = txns.begin();
    let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &tuple(1, "alice", Some("alpha"))).unwrap();
    heap_flush(&*pool, 1, rel(), tid.block_number).unwrap();
    let tid = heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &pet_tuple(10, "Kitchen", 1)).unwrap();
    heap_flush(&*pool, 1, pets_rel(), tid.block_number).unwrap();
    txns.commit(xid).unwrap();
    match run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select j.note, j.owner_id from (people p join pets q on p.id = q.owner_id) j",
        catalog_with_pets(),
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["note", "owner_id"]);
            assert_eq!(
                rows,
                vec![vec![Value::Text("alpha".into()), Value::Int32(1)]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn regex_basic_match() {
    let base = temp_dir("regex_basic_match");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'foobar' ~ 'foo'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(true)]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_basic_no_match() {
    let base = temp_dir("regex_basic_no_match");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'foobar' ~ 'baz'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(false)]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_start_anchor_match() {
    let base = temp_dir("regex_start_anchor_match");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'foobar' ~ '^foo'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(true)]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_start_anchor_no_match() {
    let base = temp_dir("regex_start_anchor_no_match");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'foobar' ~ '^bar'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(false)]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_end_anchor_match() {
    let base = temp_dir("regex_end_anchor_match");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'foobar' ~ 'bar$'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(true)]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_end_anchor_no_match() {
    let base = temp_dir("regex_end_anchor_no_match");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'foobar' ~ 'foo$'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(false)]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_full_anchor_match() {
    let base = temp_dir("regex_full_anchor");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'foobar' ~ '^foobar$'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(true)]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_dot_matches_any() {
    let base = temp_dir("regex_dot");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'foobar' ~ 'f.obar'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(true)]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_plus_quantifier() {
    let base = temp_dir("regex_plus");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'fooooo' ~ 'fo+'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(true)]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_star_quantifier() {
    let base = temp_dir("regex_star");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'f' ~ 'fo*'").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(true)]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_digit_class_match() {
    let base = temp_dir("regex_digit_match");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'abc123' ~ '[0-9]+'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(true)]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_digit_class_no_match() {
    let base = temp_dir("regex_digit_no_match");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'abc' ~ '[0-9]+'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(false)]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_alternation_first_branch() {
    let base = temp_dir("regex_alt_first");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'foo' ~ '(foo|bar)'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(true)]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_alternation_second_branch() {
    let base = temp_dir("regex_alt_second");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'bar' ~ '(foo|bar)'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(true)]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_is_case_sensitive() {
    let base = temp_dir("regex_case_sensitive");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'FOO' ~ 'foo'").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(false)]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_null_text_returns_null() {
    let base = temp_dir("regex_null_text_returns_null");
    let mut ctx = empty_executor_context(&base);
    let mut slot = TupleSlot::virtual_row(vec![Value::Null]);
    assert_eq!(
        eval_expr(
            &Expr::op_auto(
                crate::include::nodes::primnodes::OpExprKind::RegexMatch,
                vec![local_var(0), Expr::Const(Value::Text("foo".into()))]
            ),
            &mut slot,
            &mut ctx
        )
        .unwrap(),
        Value::Null
    );
}
#[test]
fn regex_null_pattern_returns_null() {
    let base = temp_dir("regex_null_pattern_returns_null");
    let mut ctx = empty_executor_context(&base);
    let mut slot = TupleSlot::virtual_row(vec![Value::Text("foobar".into())]);
    assert_eq!(
        eval_expr(
            &Expr::op_auto(
                crate::include::nodes::primnodes::OpExprKind::RegexMatch,
                vec![local_var(0), Expr::Const(Value::Null)]
            ),
            &mut slot,
            &mut ctx
        )
        .unwrap(),
        Value::Null
    );
}

#[test]
fn array_subscript_null_slice_bounds_return_null() {
    let base = temp_dir("array_subscript_null_slice_bounds");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select ('{{{1},{2},{3}},{{4},{5},{6}}}'::int[])[1][NULL:1][1]",
        )
        .unwrap(),
        vec![vec![Value::Null]],
    );

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select ('{{{1},{2},{3}},{{4},{5},{6}}}'::int[])[1][1:NULL][1]",
        )
        .unwrap(),
        vec![vec![Value::Null]],
    );
}

#[test]
fn array_subscript_rejects_more_than_max_dimensions() {
    let base = temp_dir("array_subscript_max_dimensions");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select ('{}'::int[])[1][2][3][4][5][6][7]",
    ) {
        Err(ExecError::DetailedError {
            message, sqlstate, ..
        }) => {
            assert_eq!(
                message,
                "number of array dimensions (7) exceeds the maximum allowed (6)"
            );
            assert_eq!(sqlstate, "54000");
        }
        other => panic!("expected max-dimension error, got {other:?}"),
    }
}

#[test]
fn array_subscript_partial_slices_on_zero_based_arrays_match_postgres() {
    let base = temp_dir("array_subscript_zero_based_partial_slices");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select ('[0:4]={1,2,3,4,5}'::int[])[:3]",
        )
        .unwrap(),
        vec![vec![Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound: 1,
                length: 4,
            }],
            vec![
                Value::Int32(1),
                Value::Int32(2),
                Value::Int32(3),
                Value::Int32(4),
            ],
        ))]],
    );

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select ('[0:4]={1,2,3,4,5}'::int[])[2:]",
        )
        .unwrap(),
        vec![vec![Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound: 1,
                length: 3,
            }],
            vec![Value::Int32(3), Value::Int32(4), Value::Int32(5)],
        ))]],
    );

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select ('[0:4]={1,2,3,4,5}'::int[])[:]",
        )
        .unwrap(),
        vec![vec![Value::PgArray(ArrayValue::from_dimensions(
            vec![ArrayDimension {
                lower_bound: 1,
                length: 5,
            }],
            vec![
                Value::Int32(1),
                Value::Int32(2),
                Value::Int32(3),
                Value::Int32(4),
                Value::Int32(5),
            ],
        ))]],
    );
}

#[test]
fn array_subscript_on_unsubscriptable_type_uses_postgres_error() {
    let base = temp_dir("array_subscript_unsubscriptable_error");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select (now())[1]") {
        Err(ExecError::Parse(ParseError::DetailedError {
            message, sqlstate, ..
        })) => {
            assert_eq!(
                message,
                "cannot subscript type timestamp with time zone because it does not support subscripting"
            );
            assert_eq!(sqlstate, "42804");
        }
        other => panic!("expected unsubscriptable-type error, got {other:?}"),
    }
}

#[test]
fn point_slice_subscript_uses_fixed_length_array_error() {
    let base = temp_dir("point_slice_subscript_error");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select ('(1,2)'::point)[0:1]",
    ) {
        Err(ExecError::Parse(ParseError::DetailedError {
            message, sqlstate, ..
        })) => {
            assert_eq!(message, "slices of fixed-length arrays not implemented");
            assert_eq!(sqlstate, "0A000");
        }
        other => panic!("expected fixed-length array slice error, got {other:?}"),
    }
}

#[test]
fn point_coordinate_subscripts_return_float8_values() {
    let base = temp_dir("point_coordinate_subscripts");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select ('(1.5,2.5)'::point)[0], ('(1.5,2.5)'::point)[1]",
        )
        .unwrap(),
        vec![vec![Value::Float64(1.5), Value::Float64(2.5)]],
    );
}

#[test]
fn legacy_executor_rejects_drop_table_cascade() {
    let base = temp_dir("legacy_drop_table_cascade_rejected");
    let txns = TransactionManager::new_durable(&base).unwrap();

    run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "create table items (id int4)",
    )
    .unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "drop table items cascade",
    ) {
        Err(ExecError::Parse(ParseError::UnexpectedToken { expected, actual })) => {
            assert_eq!(
                expected,
                "DROP TABLE CASCADE handled by database/session layer"
            );
            assert_eq!(actual, "DROP TABLE ... CASCADE");
        }
        other => panic!("expected DROP TABLE CASCADE rejection, got {other:?}"),
    }
}

#[test]
fn array_subscript_mixed_slice_scalar_queries_match_postgres() {
    let base = temp_dir("array_subscript_mixed_slice_scalar_queries");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select ('{{1,2,3},{4,5,6},{7,8,9}}'::int[])[1:2][2]",
        )
        .unwrap(),
        vec![vec![Value::PgArray(ArrayValue::from_dimensions(
            vec![
                ArrayDimension {
                    lower_bound: 1,
                    length: 2,
                },
                ArrayDimension {
                    lower_bound: 1,
                    length: 2,
                },
            ],
            vec![
                Value::Int32(1),
                Value::Int32(2),
                Value::Int32(4),
                Value::Int32(5),
            ],
        ))]],
    );

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select ('{{3,4},{4,5}}'::int[])[1:1][1:2][1:2]",
        )
        .unwrap(),
        vec![vec![Value::PgArray(ArrayValue::empty())]],
    );

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select ('{{3,4},{4,5}}'::int[])[1:1][2][2]",
        )
        .unwrap(),
        vec![vec![Value::PgArray(ArrayValue::empty())]],
    );

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select ('[0:2][0:2]={{1,2,3},{4,5,6},{7,8,9}}'::int[])[1:2][2]",
        )
        .unwrap(),
        vec![vec![Value::PgArray(ArrayValue::from_dimensions(
            vec![
                ArrayDimension {
                    lower_bound: 1,
                    length: 2,
                },
                ArrayDimension {
                    lower_bound: 1,
                    length: 2,
                },
            ],
            vec![
                Value::Int32(5),
                Value::Int32(6),
                Value::Int32(8),
                Value::Int32(9),
            ],
        ))]],
    );
}

#[test]
fn array_subscript_null_scalar_index_returns_null() {
    let base = temp_dir("array_subscript_null_scalar_index_returns_null");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select ('{{{1},{2},{3}},{{4},{5},{6}}}'::int[])[1][NULL][1]",
        )
        .unwrap(),
        vec![vec![Value::Null]],
    );
}

#[test]
fn nested_array_constructor_select_executes() {
    let base = temp_dir("nested_array_constructor_select_executes");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select ARRAY[[[111,112],[121,122]],[[211,212],[221,222]]]",
        )
        .unwrap(),
        vec![vec![Value::PgArray(ArrayValue::from_dimensions(
            vec![
                ArrayDimension {
                    lower_bound: 1,
                    length: 2,
                },
                ArrayDimension {
                    lower_bound: 1,
                    length: 2,
                },
                ArrayDimension {
                    lower_bound: 1,
                    length: 2,
                },
            ],
            vec![
                Value::Int32(111),
                Value::Int32(112),
                Value::Int32(121),
                Value::Int32(122),
                Value::Int32(211),
                Value::Int32(212),
                Value::Int32(221),
                Value::Int32(222),
            ],
        ))]],
    );
}

#[test]
fn array_select_subquery_executes() {
    let base = temp_dir("array_select_subquery_executes");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (4, 'dave', null), (2, 'bob', null), (3, 'carol', null)",
    )
    .unwrap();
    txns.commit(xid).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select ARRAY(select id from people order by id)",
        )
        .unwrap(),
        vec![vec![Value::PgArray(
            ArrayValue::from_1d(vec![Value::Int32(2), Value::Int32(3), Value::Int32(4)])
                .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
        )]],
    );
}

#[test]
fn array_select_subquery_empty_result_returns_empty_array() {
    let base = temp_dir("array_select_subquery_empty_result_returns_empty_array");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select ARRAY(select id from people)",
        )
        .unwrap(),
        vec![vec![Value::PgArray(
            ArrayValue::empty().with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
        )]],
    );
}

#[test]
fn regex_filters_rows_in_where_clause() {
    let base = temp_dir("regex_filter_where");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'b'), (3, 'charlie', 'c')").unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select name from people where name ~ '^a'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("alice".into())]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_filter_matches_multiple_rows() {
    let base = temp_dir("regex_filter_multi");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'arnold', 'b'), (3, 'bob', 'c')").unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select name from people where name ~ '^a' order by name",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Text("alice".into())],
                    vec![Value::Text("arnold".into())]
                ]
            );
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_combined_with_and() {
    let base = temp_dir("regex_and");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'albert', 'b'), (3, 'bob', 'c')").unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select name from people where name ~ '^al' and id > 1",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("albert".into())]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn regex_null_column_excluded_from_results() {
    let base = temp_dir("regex_null_col");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'keep'), (2, 'bob', null)",
    )
    .unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select id from people where note ~ 'keep'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("{:?}", other),
    }
}
#[test]
fn ungrouped_column_is_rejected() {
    let base = temp_dir("ungrouped_column");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let result = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select name, count(*) from people",
    );
    assert!(result.is_err());
}
#[test]
fn aggregate_in_where_is_rejected() {
    let base = temp_dir("agg_in_where");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let result = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select name from people where count(*) > 1",
    );
    assert!(result.is_err());
}
#[test]
fn explain_shows_aggregate_node() {
    let base = temp_dir("explain_agg");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "explain select note, count(*) from people group by note",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            let rendered = rows
                .into_iter()
                .map(|row| match &row[0] {
                    Value::Text(text) => text.clone(),
                    other => panic!("expected text, got {:?}", other),
                })
                .collect::<Vec<_>>();
            assert!(rendered.iter().any(|line| line.contains("Aggregate")));
            assert!(rendered.iter().any(|line| line.contains("Seq Scan")));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn explain_verbose_lateral_aggregate_renders_pg_style_details() {
    let base = temp_dir("explain_verbose_lateral_agg");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "explain (verbose, costs off)
         select s1, s2, sm
         from generate_series(1, 3) s1,
              lateral (
                  select s2, sum(s1 + s2) sm
                  from generate_series(1, 3) s2
                  group by s2
              ) ss
         order by 1, 2",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            let rendered = rows
                .into_iter()
                .map(|row| match &row[0] {
                    Value::Text(text) => text.to_string(),
                    other => panic!("expected text, got {:?}", other),
                })
                .collect::<Vec<_>>();
            assert!(
                rendered.iter().any(|line| {
                    line.trim()
                        == "Output: generate_series.generate_series, sum((generate_series.generate_series + generate_series.generate_series))"
                }),
                "{}",
                rendered.join("\n")
            );
            assert!(
                rendered
                    .iter()
                    .any(|line| line.trim() == "Group Key: generate_series.generate_series"),
                "{}",
                rendered.join("\n")
            );
            assert!(
                rendered
                    .iter()
                    .any(|line| { line.trim() == "Function Call: generate_series(1, 3)" }),
                "{}",
                rendered.join("\n")
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn window_builtin_functions_handle_peer_groups() {
    let base = temp_dir("window_builtin_peer_groups");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values
            (1, 'alice', 'x'),
            (2, 'bob', 'x'),
            (3, 'carol', 'y'),
            (4, 'dave', 'x')",
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select id,
                row_number() over (order by note, id),
                rank() over (order by note),
                dense_rank() over (order by note)
         from people
         order by id",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Int32(1),
                        Value::Int64(1),
                        Value::Int64(1),
                        Value::Int64(1)
                    ],
                    vec![
                        Value::Int32(2),
                        Value::Int64(2),
                        Value::Int64(1),
                        Value::Int64(1)
                    ],
                    vec![
                        Value::Int32(3),
                        Value::Int64(4),
                        Value::Int64(4),
                        Value::Int64(2)
                    ],
                    vec![
                        Value::Int32(4),
                        Value::Int64(3),
                        Value::Int64(1),
                        Value::Int64(1)
                    ],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn window_distribution_functions_handle_peer_groups() {
    let base = temp_dir("window_distribution_peer_groups");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values
            (1, 'alice', 'x'),
            (2, 'bob', 'x'),
            (3, 'carol', 'y'),
            (4, 'dave', 'x')",
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select id,
                percent_rank() over (order by note),
                cume_dist() over (order by note),
                ntile(3) over (order by note, id)
         from people
         order by id",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 4);
            assert_eq!(rows[0][0], Value::Int32(1));
            assert_eq!(rows[1][0], Value::Int32(2));
            assert_eq!(rows[2][0], Value::Int32(3));
            assert_eq!(rows[3][0], Value::Int32(4));

            for index in [0usize, 1, 3] {
                match rows[index][1] {
                    Value::Float64(value) => assert_eq!(value, 0.0),
                    ref other => panic!("expected Float64, got {other:?}"),
                }
                match rows[index][2] {
                    Value::Float64(value) => assert!((value - 0.75).abs() < 1e-12),
                    ref other => panic!("expected Float64, got {other:?}"),
                }
            }
            match rows[2][1] {
                Value::Float64(value) => assert_eq!(value, 1.0),
                ref other => panic!("expected Float64, got {other:?}"),
            }
            match rows[2][2] {
                Value::Float64(value) => assert_eq!(value, 1.0),
                ref other => panic!("expected Float64, got {other:?}"),
            }

            assert_eq!(rows[0][3], Value::Int32(1));
            assert_eq!(rows[1][3], Value::Int32(1));
            assert_eq!(rows[2][3], Value::Int32(3));
            assert_eq!(rows[3][3], Value::Int32(2));
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select ntile(null) over (order by id) from people order by id limit 2",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Null], vec![Value::Null]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn window_ntile_rejects_nonpositive_bucket_count() {
    let base = temp_dir("window_ntile_invalid_bucket_count");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values
            (1, 'alice', 'x'),
            (2, 'bob', 'y')",
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select ntile(0) over (order by id) from people",
    )
    .unwrap_err()
    {
        ExecError::DetailedError {
            message, sqlstate, ..
        } => {
            assert_eq!(message, "argument of ntile must be greater than zero");
            assert_eq!(sqlstate, "22023");
        }
        other => panic!("expected detailed error, got {other:?}"),
    }
}

#[test]
fn window_value_functions_follow_default_frame_semantics() {
    let base = temp_dir("window_value_default_frame");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values
            (1, 'alice', 'x'),
            (2, 'bob', 'x'),
            (3, 'carol', 'y'),
            (4, 'dave', 'x')",
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select id,
                first_value(id) over (partition by note order by id),
                last_value(id) over (partition by note order by id),
                nth_value(id, 2) over (partition by note order by id),
                nth_value(id, null) over (partition by note order by id)
         from people
         order by id",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Int32(1),
                        Value::Int32(1),
                        Value::Int32(1),
                        Value::Null,
                        Value::Null,
                    ],
                    vec![
                        Value::Int32(2),
                        Value::Int32(1),
                        Value::Int32(2),
                        Value::Int32(2),
                        Value::Null,
                    ],
                    vec![
                        Value::Int32(3),
                        Value::Int32(3),
                        Value::Int32(3),
                        Value::Null,
                        Value::Null,
                    ],
                    vec![
                        Value::Int32(4),
                        Value::Int32(1),
                        Value::Int32(4),
                        Value::Int32(2),
                        Value::Null,
                    ],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn window_rows_range_and_groups_frames_are_respected() {
    let base = temp_dir("window_explicit_frames");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values
            (1, 'alice', 'x'),
            (2, 'bob', 'x'),
            (3, 'carol', 'x'),
            (4, 'dave', 'y')",
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select id,
                sum(id) over (order by note, id rows between 1 preceding and 1 following),
                sum(id) over (order by note range between current row and unbounded following),
                sum(id) over (order by note groups between current row and 1 following),
                first_value(id) over (order by note groups between 1 preceding and current row),
                last_value(id) over (order by note range between current row and unbounded following)
         from people
         order by id",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Int32(1),
                        Value::Int64(3),
                        Value::Int64(10),
                        Value::Int64(10),
                        Value::Int32(1),
                        Value::Int32(4),
                    ],
                    vec![
                        Value::Int32(2),
                        Value::Int64(6),
                        Value::Int64(10),
                        Value::Int64(10),
                        Value::Int32(1),
                        Value::Int32(4),
                    ],
                    vec![
                        Value::Int32(3),
                        Value::Int64(9),
                        Value::Int64(10),
                        Value::Int64(10),
                        Value::Int32(1),
                        Value::Int32(4),
                    ],
                    vec![
                        Value::Int32(4),
                        Value::Int64(7),
                        Value::Int64(4),
                        Value::Int64(4),
                        Value::Int32(1),
                        Value::Int32(4),
                    ],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn window_range_offset_frame_supports_numeric_order_keys() {
    let base = temp_dir("window_range_offset_frame");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values
            (1, 'alice', 'x'),
            (3, 'bob', 'x'),
            (4, 'carol', 'x'),
            (8, 'dave', 'x')",
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select id,
                sum(id) over (order by id range between 2 preceding and 2 following),
                first_value(id) over (order by id range between 2 preceding and 2 following),
                last_value(id) over (order by id range between 2 preceding and 2 following)
         from people
         order by id",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Int32(1),
                        Value::Int64(4),
                        Value::Int32(1),
                        Value::Int32(3)
                    ],
                    vec![
                        Value::Int32(3),
                        Value::Int64(8),
                        Value::Int32(1),
                        Value::Int32(4)
                    ],
                    vec![
                        Value::Int32(4),
                        Value::Int64(7),
                        Value::Int32(3),
                        Value::Int32(4)
                    ],
                    vec![
                        Value::Int32(8),
                        Value::Int64(8),
                        Value::Int32(8),
                        Value::Int32(8)
                    ],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn window_lag_and_lead_support_offsets_defaults_and_nulls() {
    let base = temp_dir("window_lag_lead_offsets_defaults");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values
            (1, 'alice', 'x'),
            (2, 'bob', 'x'),
            (3, 'carol', 'y'),
            (4, 'dave', 'x')",
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select id,
                lag(id) over (partition by note order by id),
                lag(id, 2, 99) over (partition by note order by id),
                lead(id * 2, 1, -1.4) over (partition by note order by id),
                lead(id, null) over (partition by note order by id)
         from people
         order by id",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Int32(1),
                        Value::Null,
                        Value::Int32(99),
                        Value::Numeric("4".into()),
                        Value::Null,
                    ],
                    vec![
                        Value::Int32(2),
                        Value::Int32(1),
                        Value::Int32(99),
                        Value::Numeric("8".into()),
                        Value::Null,
                    ],
                    vec![
                        Value::Int32(3),
                        Value::Null,
                        Value::Int32(99),
                        Value::Numeric("-1.4".into()),
                        Value::Null,
                    ],
                    vec![
                        Value::Int32(4),
                        Value::Int32(2),
                        Value::Int32(1),
                        Value::Numeric("-1.4".into()),
                        Value::Null,
                    ],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn window_nth_value_rejects_nonpositive_offset() {
    let base = temp_dir("window_nth_value_invalid_offset");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values
            (1, 'alice', 'x'),
            (2, 'bob', 'y')",
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select nth_value(id, 0) over (order by id) from people",
    )
    .unwrap_err()
    {
        ExecError::DetailedError {
            message, sqlstate, ..
        } => {
            assert_eq!(message, "argument of nth_value must be greater than zero");
            assert_eq!(sqlstate, "22023");
        }
        other => panic!("expected detailed error, got {other:?}"),
    }
}

#[test]
fn window_ntile_supports_join_bucket_expression() {
    let base = temp_dir("window_ntile_join_bucket_expression");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values
            (1, 'alice', 'x'),
            (2, 'bob', 'x'),
            (3, 'carol', 'y')",
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select c
         from (
             select ntile(r.id) over (partition by l.note order by l.id) as c
             from people l
             left join people r on true
             where l.id = r.id
         ) s
         where c = 1
         order by c",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1)],
                    vec![Value::Int32(1)],
                    vec![Value::Int32(1)]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn window_aggregate_supports_partitioning_and_running_totals() {
    let base = temp_dir("window_partition_running_sum");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values
            (1, 'alice', 'x'),
            (2, 'bob', 'x'),
            (3, 'carol', 'y'),
            (4, 'dave', 'x')",
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select id,
                count(*) over (),
                sum(id) over (partition by note order by id),
                sum(id) over (order by note)
         from people
         order by id",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Int32(1),
                        Value::Int64(4),
                        Value::Int64(1),
                        Value::Int64(7)
                    ],
                    vec![
                        Value::Int32(2),
                        Value::Int64(4),
                        Value::Int64(3),
                        Value::Int64(7)
                    ],
                    vec![
                        Value::Int32(3),
                        Value::Int64(4),
                        Value::Int64(3),
                        Value::Int64(10)
                    ],
                    vec![
                        Value::Int32(4),
                        Value::Int64(4),
                        Value::Int64(7),
                        Value::Int64(7)
                    ],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn window_duplicate_running_aggregates_in_subquery_match() {
    let base = temp_dir("window_duplicate_running_aggregates_in_subquery_match");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    let mut values = String::new();
    for id in 1..=200 {
        if !values.is_empty() {
            values.push_str(", ");
        }
        let note = if id % 2 == 0 { "x" } else { "y" };
        values.push_str(&format!("({id}, 'p{id}', '{note}')"));
    }
    run_sql(
        &base,
        &txns,
        xid,
        &format!("insert into people (id, name, note) values {values}"),
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select *
         from (
             select count(*) over (partition by id % 4 order by id % 10) +
                        sum(id) over (partition by note order by id % 10) as total,
                    count(*) over (partition by id % 4 order by id % 10) as fourcount,
                    sum(id) over (partition by note order by id % 10) as notesum
             from people
         ) s
         where total <> fourcount + notesum",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert!(rows.is_empty(), "unexpected rows: {rows:?}")
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn explain_shows_windowagg_node_details() {
    let base = temp_dir("explain_windowagg");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "explain select row_number() over (partition by note order by id) from people",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            let rendered = rows
                .into_iter()
                .map(|row| match &row[0] {
                    Value::Text(text) => text.clone(),
                    other => panic!("expected text, got {:?}", other),
                })
                .collect::<Vec<_>>();
            assert!(rendered.iter().any(|line| line.contains("WindowAgg")));
            assert!(rendered.iter().any(|line| line.contains("Partition By:")));
            assert!(rendered.iter().any(|line| line.contains("Order By:")));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn group_by_with_order_by_and_limit() {
    let base = temp_dir("group_by_order_limit");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'x'), (2, 'bob', 'y'), (3, 'carol', 'x'), (4, 'dave', 'y'), (5, 'eve', 'z')").unwrap();
    txns.commit(xid).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select note, count(*) from people group by note order by count(*) desc limit 2",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Text("x".into()), Value::Int64(2)],
                    vec![Value::Text("y".into()), Value::Int64(2)]
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
#[test]
fn random_returns_float_in_range() {
    let base = temp_dir("random_func");
    let txns = TransactionManager::new_durable(&base).unwrap();
    for _ in 0..10 {
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select random()").unwrap() {
            StatementResult::Query {
                column_names, rows, ..
            } => {
                assert_eq!(column_names, vec!["random".to_string()]);
                assert_eq!(rows.len(), 1);
                match &rows[0][0] {
                    Value::Float64(v) => {
                        assert!(*v >= 0.0 && *v < 1.0, "random() must be in [0,1), got {v}")
                    }
                    other => panic!("expected Float64, got {:?}", other),
                }
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
}

#[test]
fn bounded_random_uses_requested_result_types_and_ranges() {
    let base = temp_dir("bounded_random_ranges");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select random(1, 2), random(1000000000001, 1000000000002), random(-0.5, 0.49), random(101, 101), random(1000000000001, 1000000000001), random(3.14, 3.14)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert!(matches!(rows[0][0], Value::Int32(v) if (1..=2).contains(&v)));
            assert!(matches!(
                rows[0][1],
                Value::Int64(v) if (1_000_000_000_001..=1_000_000_000_002).contains(&v)
            ));
            assert!(matches!(rows[0][2], Value::Numeric(_)));
            assert_eq!(rows[0][3], Value::Int32(101));
            assert_eq!(rows[0][4], Value::Int64(1_000_000_000_001));
            assert_eq!(rows[0][5], Value::Numeric("3.14".into()));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn bounded_random_reports_invalid_ranges() {
    let base = temp_dir("bounded_random_errors");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let err = run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select random(1, 0)").unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, sqlstate, .. }
            if message == "lower bound must be less than or equal to upper bound"
                && sqlstate == "22023"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select random('NaN'::numeric, 10)",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, sqlstate, .. }
            if message == "lower bound cannot be NaN" && sqlstate == "22023"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select random(0, 'Inf'::numeric)",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, sqlstate, .. }
            if message == "upper bound cannot be infinity" && sqlstate == "22023"
    ));
}

#[test]
fn random_normal_supports_defaults_named_args_and_zero_stddev() {
    let base = temp_dir("random_normal_func");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select random_normal(), random_normal(10, 0), random_normal(mean => 1, stddev => 0)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert!(matches!(rows[0][0], Value::Float64(_)));
            assert_eq!(rows[0][1], Value::Float64(10.0));
            assert_eq!(rows[0][2], Value::Float64(1.0));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn json_cast_and_extract_operators_work() {
    let base = temp_dir("json_extract_ops");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select '{\"a\":[1,null],\"b\":{\"c\":\"x\"}}'::json -> 'a', '{\"a\":[1,null],\"b\":{\"c\":\"x\"}}'::json ->> 'a', '{\"a\":[1,null],\"b\":{\"c\":\"x\"}}'::json #> ARRAY['b','c']::varchar[], '{\"a\":[1,null],\"b\":{\"c\":\"x\"}}'::json #>> ARRAY['b','c']::varchar[]",
        ).unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![
                    Value::Json("[1,null]".into()),
                    Value::Text("[1,null]".into()),
                    Value::Json("\"x\"".into()),
                    Value::Text("x".into()),
                ]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn json_scalar_functions_work() {
    let base = temp_dir("json_scalar_functions");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select to_json(ARRAY[1,2]), array_to_json(ARRAY['a','b']::varchar[]), json_typeof('{\"a\":1}'::json), json_array_length('[1,2,3]'::json), json_extract_path('{\"a\":{\"b\":2}}'::json, 'a', 'b'), json_extract_path_text('{\"a\":{\"b\":2}}'::json, 'a', 'b')",
        ).unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![
                    Value::Json("[1,2]".into()),
                    Value::Json("[\"a\",\"b\"]".into()),
                    Value::Text("object".into()),
                    Value::Int32(3),
                    Value::Json("2".into()),
                    Value::Text("2".into()),
                ]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn array_to_json_preserves_nested_jsonb_spacing() {
    let base = temp_dir("array_to_json_nested_jsonb_spacing");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select array_to_json(ARRAY [jsonb '{\"a\":1}', jsonb '{\"b\":[2,3]}'])",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Json("[{\"a\": 1},{\"b\": [2, 3]}]".into())]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn row_to_json_supports_row_constructor_and_whole_row_alias() {
    let base = temp_dir("row_to_json");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select row_to_json(row(1, 'foo')), \
                row_to_json(q), \
                row_to_json(row((select array_agg(x) from generate_series(5,10) x)), false) \
         from (select 7 as a, 'bar'::text as b) q",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Json("{\"f1\":1,\"f2\":\"foo\"}".into()),
                    Value::Json("{\"a\":7,\"b\":\"bar\"}".into()),
                    Value::Json("{\"f1\":[5,6,7,8,9,10]}".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn row_to_json_supports_qualified_star_inside_row_constructor() {
    let base = temp_dir("row_to_json_qualified_star");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "SELECT row_to_json(q) \
         FROM (SELECT $$a$$ || x AS b, \
                      y AS c, \
                      ARRAY[ROW(x.*, ARRAY[1,2,3]), ROW(y.*, ARRAY[4,5,6])] AS z \
               FROM generate_series(1,2) x, \
                    generate_series(4,5) y) q",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Json(
                        "{\"b\":\"a1\",\"c\":4,\"z\":[{\"f1\":1,\"f2\":[1,2,3]},{\"f1\":4,\"f2\":[4,5,6]}]}"
                            .into(),
                    )],
                    vec![Value::Json(
                        "{\"b\":\"a1\",\"c\":5,\"z\":[{\"f1\":1,\"f2\":[1,2,3]},{\"f1\":5,\"f2\":[4,5,6]}]}"
                            .into(),
                    )],
                    vec![Value::Json(
                        "{\"b\":\"a2\",\"c\":4,\"z\":[{\"f1\":2,\"f2\":[1,2,3]},{\"f1\":4,\"f2\":[4,5,6]}]}"
                            .into(),
                    )],
                    vec![Value::Json(
                        "{\"b\":\"a2\",\"c\":5,\"z\":[{\"f1\":2,\"f2\":[1,2,3]},{\"f1\":5,\"f2\":[4,5,6]}]}"
                            .into(),
                    )],
                ]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn jsonb_agg_supports_whole_row_alias_arguments() {
    let base = temp_dir("jsonb_agg_whole_row_alias");
    let mut txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "SELECT jsonb_agg(q) \
         FROM (SELECT $$a$$ || x AS b, \
                      y AS c, \
                      ARRAY[ROW(x.*, ARRAY[1,2,3]), ROW(y.*, ARRAY[4,5,6])] AS z \
               FROM generate_series(1,2) x, \
                    generate_series(4,5) y) q",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Jsonb(
                    crate::backend::executor::jsonb::parse_jsonb_text(
                        "[{\"b\": \"a1\", \"c\": 4, \"z\": [{\"f1\": 1, \"f2\": [1, 2, 3]}, {\"f1\": 4, \"f2\": [4, 5, 6]}]}, \
                          {\"b\": \"a1\", \"c\": 5, \"z\": [{\"f1\": 1, \"f2\": [1, 2, 3]}, {\"f1\": 5, \"f2\": [4, 5, 6]}]}, \
                          {\"b\": \"a2\", \"c\": 4, \"z\": [{\"f1\": 2, \"f2\": [1, 2, 3]}, {\"f1\": 4, \"f2\": [4, 5, 6]}]}, \
                          {\"b\": \"a2\", \"c\": 5, \"z\": [{\"f1\": 2, \"f2\": [1, 2, 3]}, {\"f1\": 5, \"f2\": [4, 5, 6]}]}]"
                    )
                    .unwrap()
                )]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_agg(q order by x, y) \
         from (values (1, 'txt1'), (2, 'txt2'), (3, 'txt3')) as q(x, y)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Jsonb(
                    crate::backend::executor::jsonb::parse_jsonb_text(
                        "[{\"x\": 1, \"y\": \"txt1\"}, {\"x\": 2, \"y\": \"txt2\"}, {\"x\": 3, \"y\": \"txt3\"}]"
                    )
                    .unwrap()
                )]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_agg(q order by x nulls first, y) \
         from (values (null::int, 'txt1'), (2, 'txt2'), (3, 'txt3')) as q(x, y)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Jsonb(
                    crate::backend::executor::jsonb::parse_jsonb_text(
                        "[{\"x\": null, \"y\": \"txt1\"}, {\"x\": 2, \"y\": \"txt2\"}, {\"x\": 3, \"y\": \"txt3\"}]"
                    )
                    .unwrap()
                )]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn json_strip_nulls_functions_work() {
    let base = temp_dir("json_strip_nulls");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select json_strip_nulls('{\"a\":1,\"b\":null,\"c\":[2,null,3]}'::json), \
                json_strip_nulls('{\"a\":1,\"b\":null,\"c\":[2,null,3]}'::json, true), \
                jsonb_strip_nulls('{\"a\":1,\"b\":null,\"c\":[2,null,3]}'::jsonb), \
                jsonb_strip_nulls('{\"a\":1,\"b\":null,\"c\":[2,null,3]}'::jsonb, true)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Json("{\"a\":1,\"c\":[2,null,3]}".into()),
                    Value::Json("{\"a\":1,\"c\":[2,3]}".into()),
                    Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text(
                            "{\"a\":1,\"c\":[2,null,3]}"
                        )
                        .unwrap()
                    ),
                    Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("{\"a\":1,\"c\":[2,3]}")
                            .unwrap()
                    ),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn json_builders_and_object_agg_work() {
    let base = temp_dir("json_builders");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'x'), (2, 'bob', 'y')",
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select json_build_array('a', 1, true), json_build_object('a', 1, 'b', true), json_object(ARRAY['a','1','b','2']::varchar[]), json_object_agg(name, note) from people",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![
                        Value::Json("[\"a\",1,true]".into()),
                        Value::Json("{\"a\":1,\"b\":true}".into()),
                        Value::Json("{\"a\":\"1\",\"b\":\"2\"}".into()),
                        Value::Json("{\"alice\":\"x\",\"bob\":\"y\"}".into()),
                    ]]
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn json_variadic_calls_match_supported_postgres_cases() {
    let base = temp_dir("json_variadic_calls");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select json_build_array(VARIADIC NULL::text[]), \
                json_build_array(VARIADIC '{{1,4},{2,5},{3,6}}'::int[][]), \
                json_build_object(VARIADIC '{}'::text[]), \
                json_build_object(VARIADIC '{{1,4},{2,5},{3,6}}'::int[][]), \
                json_extract_path('{\"a\":{\"b\":2}}'::json, VARIADIC ARRAY['a','b']::text[])",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Null,
                    Value::Json("[1,4,2,5,3,6]".into()),
                    Value::Json("{}".into()),
                    Value::Json("{\"1\":4,\"2\":5,\"3\":6}".into()),
                    Value::Json("2".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn json_table_functions_and_json_agg_work() {
    let base = temp_dir("json_table_functions");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'x'), (2, 'bob', 'y')",
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select key, value from json_each('{\"a\":1,\"b\":null}'::json) order by key",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Text("a".into()), Value::Json("1".into())],
                    vec![Value::Text("b".into()), Value::Json("null".into())],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select json_agg(id) from people",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Json("[1,2]".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn invalid_json_input_errors() {
    let base = temp_dir("json_invalid_input");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let err = run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select '{bad'::json").unwrap_err();
    assert!(matches!(
        err,
        ExecError::JsonInput { message, .. } if message == "invalid input syntax for type json"
    ));
}

#[test]
fn jsonb_operators_and_scalar_functions_work() {
    let base = temp_dir("jsonb_ops");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select '{\"a\":1,\"b\":[\"x\",\"y\"]}'::jsonb @> '{\"a\":1}'::jsonb, '{\"a\":1,\"b\":[\"x\",\"y\"]}'::jsonb ? 'a', '{\"a\":1,\"b\":[\"x\",\"y\"]}'::jsonb ?| ARRAY['z','a']::varchar[], '{\"a\":1,\"b\":[\"x\",\"y\"]}'::jsonb -> 'b', '{\"a\":1,\"b\":[\"x\",\"y\"]}'::jsonb ->> 'a', jsonb_typeof('{\"a\":1}'::jsonb), jsonb_extract_path('{\"a\":{\"b\":2}}'::jsonb, 'a', 'b'), jsonb_extract_path_text('{\"a\":{\"b\":2}}'::jsonb, 'a', 'b')",
        ).unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Jsonb(crate::backend::executor::jsonb::parse_jsonb_text("[\"x\",\"y\"]").unwrap()),
                    Value::Text("1".into()),
                    Value::Text("object".into()),
                    Value::Jsonb(crate::backend::executor::jsonb::parse_jsonb_text("2").unwrap()),
                    Value::Text("2".into()),
                ]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn jsonb_contains_and_exists_helpers_follow_postgres_semantics() {
    let base = temp_dir("jsonb_contains_exists_helpers");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select \
            jsonb_contains('{\"a\":\"b\", \"b\":1, \"c\":null}', '{\"a\":\"b\", \"c\":null}'), \
            jsonb_contained('{\"a\":\"b\"}', '{\"a\":\"b\", \"b\":1, \"c\":null}'), \
            '[1,2]'::jsonb @> '[1,2,2]'::jsonb, \
            '[1,2,2]'::jsonb <@ '[1,2]'::jsonb, \
            jsonb_exists('{\"a\":null, \"b\":\"qq\"}', 'a'), \
            jsonb_exists_any('{\"a\":null, \"b\":\"qq\"}', ARRAY['c','a']::text[]), \
            jsonb_exists_all('{\"a\":null, \"b\":\"qq\"}', ARRAY['a','b']::text[]), \
            '{\"a\":null, \"b\":\"qq\"}'::jsonb ? 'a', \
            '{\"a\":null, \"b\":\"qq\"}'::jsonb ?| ARRAY['c','a']::text[], \
            '{\"a\":null, \"b\":\"qq\"}'::jsonb ?& ARRAY['a','b']::text[]",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonb_containment_operators_coerce_string_literals_to_jsonb() {
    let base = temp_dir("jsonb_containment_literal_coercion");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select \
            '{\"a\":\"b\", \"b\":1, \"c\":null}'::jsonb @> '{\"a\":\"b\"}', \
            '{\"a\":\"b\", \"b\":1, \"c\":null}'::jsonb @> '{\"a\":\"b\", \"c\":null}', \
            '{\"a\":\"b\"}'::jsonb <@ '{\"a\":\"b\", \"b\":1, \"c\":null}', \
            '{\"a\":\"b\", \"c\":null}'::jsonb <@ '{\"a\":\"b\", \"b\":1, \"c\":null}', \
            '{\"a\":\"b\", \"b\":1, \"c\":null}'::jsonb @> '{\"a\":\"b\", \"g\":null}', \
            '{\"a\":\"b\", \"g\":null}'::jsonb <@ '{\"a\":\"b\", \"b\":1, \"c\":null}'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Bool(false),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn row_to_json_renders_regclass_fields_with_relation_names() {
    let base = temp_dir("row_to_json_regclass");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select row_to_json(r) \
         from (select relkind, oid::regclass as name from pg_class where relname = 'pg_class') r",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Json(
                    "{\"relkind\":\"r\",\"name\":\"pg_class\"}".into()
                )]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonb_object_and_pretty_functions_work() {
    let base = temp_dir("jsonb_object_and_pretty");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_object(ARRAY['a','1','b','2']::varchar[]), \
                jsonb_object(ARRAY['a','b']::varchar[], ARRAY['1','2']::varchar[]), \
                jsonb_pretty('{\"a\":[1,2],\"b\":{\"c\":3}}'::jsonb)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text(
                            "{\"a\":\"1\",\"b\":\"2\"}"
                        )
                        .unwrap()
                    ),
                    Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text(
                            "{\"a\":\"1\",\"b\":\"2\"}"
                        )
                        .unwrap()
                    ),
                    Value::Text(
                        "{\n  \"a\": [\n    1,\n    2\n  ],\n  \"b\": {\n    \"c\": 3\n  }\n}"
                            .into()
                    ),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonb_object_accepts_text_array_literals() {
    let base = temp_dir("jsonb_object_text_array_literals");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_object('{a,1,b,2,3,NULL,\"d e f\",\"a b c\"}'), \
                jsonb_object('{a,b,c,\"d e f\"}','{1,2,3,\"a b c\"}')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text(
                            "{\"3\":null,\"a\":\"1\",\"b\":\"2\",\"d e f\":\"a b c\"}"
                        )
                        .unwrap()
                    ),
                    Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text(
                            "{\"a\":\"1\",\"b\":\"2\",\"c\":\"3\",\"d e f\":\"a b c\"}"
                        )
                        .unwrap()
                    ),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonb_object_matches_postgres_multidimensional_text_array_behavior() {
    let base = temp_dir("jsonb_object_multidimensional_arrays");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_object('{{a,1},{b,2},{3,NULL},{\"d e f\",\"a b c\"}}')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Jsonb(
                    crate::backend::executor::jsonb::parse_jsonb_text(
                        "{\"3\":null,\"a\":\"1\",\"b\":\"2\",\"d e f\":\"a b c\"}"
                    )
                    .unwrap()
                )]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    for (sql, message) in [
        (
            "select jsonb_object('{{a},{b}}')",
            "array must have two columns",
        ),
        (
            "select jsonb_object('{{a,b,c},{b,c,d}}')",
            "array must have two columns",
        ),
        (
            "select jsonb_object('{{{a,b},{c,d}},{{b,c},{d,e}}}')",
            "wrong number of array subscripts",
        ),
        (
            "select jsonb_object('{{a,1},{b,2}}', '{{a,1},{b,2}}')",
            "wrong number of array subscripts",
        ),
    ] {
        let err = run_sql(&base, &txns, INVALID_TRANSACTION_ID, sql).unwrap_err();
        assert!(matches!(
            err,
            ExecError::InvalidStorageValue { details, .. } if details == message
        ));
    }
}

#[test]
fn jsonb_object_and_builder_report_postgres_style_errors() {
    let base = temp_dir("jsonb_object_errors");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_build_object('a', 'b', 'c')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError {
            message,
            hint,
            sqlstate,
            ..
        } if message == "argument list must have even number of elements"
            && hint.as_deref() == Some(
                "The arguments of jsonb_build_object() must consist of alternating keys and values."
            )
            && sqlstate == "22023"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_build_object(NULL, 'a')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, sqlstate, .. }
            if message == "argument 1: key must not be null" && sqlstate == "22004"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_build_object('{1,2,3}'::int[], 3)",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, sqlstate, .. }
            if message == "key value must be scalar, not array, composite, or json"
                && sqlstate == "22023"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_build_object(r, 2) from (select 1 as a, 2 as b) r",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, sqlstate, .. }
            if message == "key value must be scalar, not array, composite, or json"
                && sqlstate == "22023"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_object(ARRAY['a','b',NULL,'d e f']::text[], ARRAY['1','2','3','a b c']::text[])",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, sqlstate, .. }
            if message == "null value not allowed for object key" && sqlstate == "22004"
    ));
}

#[test]
fn jsonb_object_keys_and_object_agg_reject_invalid_keys() {
    let base = temp_dir("jsonb_object_keys_errors");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_object_keys('\"scalar\"'::jsonb)",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, sqlstate, .. }
            if message == "cannot call jsonb_object_keys on a scalar" && sqlstate == "22023"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_object_keys('[1,2,3]'::jsonb)",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, sqlstate, .. }
            if message == "cannot call jsonb_object_keys on an array" && sqlstate == "22023"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_object_agg(NULL, '{\"a\":1}'::jsonb)",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, sqlstate, .. }
            if message == "field name must not be null" && sqlstate == "22004"
    ));
}

#[test]
fn jsonb_delete_and_delete_path_functions_work() {
    let base = temp_dir("jsonb_delete");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_delete('{\"a\":1,\"b\":2,\"c\":3}'::jsonb, 'b'), \
                jsonb_delete('[\"a\",\"b\",\"c\"]'::jsonb, 'b'), \
                jsonb_delete('[10,20,30]'::jsonb, 1), \
                jsonb_delete_path('{\"a\":{\"b\":[1,2,3]}}'::jsonb, ARRAY['a','b','1']::varchar[])",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("{\"a\":1,\"c\":3}")
                            .unwrap()
                    ),
                    Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("[\"a\",\"c\"]").unwrap()
                    ),
                    Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("[10,30]").unwrap()
                    ),
                    Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("{\"a\":{\"b\":[1,3]}}")
                            .unwrap()
                    ),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonb_set_insert_and_set_lax_functions_work() {
    let base = temp_dir("jsonb_set_insert");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_set('{\"a\":[1,2,3],\"b\":{\"c\":4}}'::jsonb, ARRAY['a','1']::varchar[], '9'::jsonb), \
                jsonb_set('{\"a\":[1,2,3],\"b\":{\"c\":4}}'::jsonb, ARRAY['b','d']::varchar[], '5'::jsonb, true), \
                jsonb_insert('{\"a\":[1,2,3]}'::jsonb, ARRAY['a','1']::varchar[], '9'::jsonb), \
                jsonb_set_lax('{\"a\":1,\"b\":2}'::jsonb, ARRAY['b']::varchar[], null, true, 'delete_key')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("{\"a\":[1,9,3],\"b\":{\"c\":4}}")
                            .unwrap()
                    ),
                    Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("{\"a\":[1,2,3],\"b\":{\"c\":4,\"d\":5}}")
                            .unwrap()
                    ),
                    Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("{\"a\":[1,9,2,3]}")
                            .unwrap()
                    ),
                    Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("{\"a\":1}").unwrap()
                    ),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonb_set_and_delete_path_validate_path_elements() {
    let base = temp_dir("jsonb_set_path_validation");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_set('{\"d\":{\"1\":[2,3]}}'::jsonb, '{d,NULL,0}', '[1,2,3]')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidStorageValue { details, .. }
            if details == "path element at position 2 is null"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_set('{\"a\":{\"b\":[1,2,3]}}'::jsonb, '{a,b,non_integer}', '\"new_value\"')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidStorageValue { details, .. }
            if details == "path element at position 3 is not an integer: \"non_integer\""
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_delete_path('{\"a\":[1,2,3]}'::jsonb, '{a,NULL}')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidStorageValue { details, .. }
            if details == "path element at position 2 is null"
    ));
}

#[test]
fn jsonb_delete_and_set_lax_report_postgres_style_errors() {
    let base = temp_dir("jsonb_delete_set_lax_errors");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '\"a\"'::jsonb - 'a'",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidStorageValue { details, .. } if details == "cannot delete from scalar"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '{}'::jsonb - 1",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidStorageValue { details, .. }
            if details == "cannot delete from object using integer index"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_set_lax('{\"a\":1,\"b\":2}', '{b}', null, true, null)",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidStorageValue { details, .. }
            if details
                == "null_value_treatment must be \"delete_key\", \"return_target\", \"use_json_null\", or \"raise_exception\""
    ));
}

#[test]
fn jsonpath_operators_and_functions_work() {
    let base = temp_dir("jsonpath_ops");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select '{\"a\":1}'::jsonb @? '$.a', '[{\"a\":1},{\"a\":2}]'::jsonb @@ '$[*].a > 1', jsonb_path_exists('[{\"a\":1},{\"a\":2},{\"a\":3}]'::jsonb, '$[*] ? (@.a > $min && @.a < $max)', '{\"min\":1,\"max\":3}'::jsonb), jsonb_path_query_first('[{\"a\":1},{\"a\":2}]'::jsonb, '$[*].a ? (@ > 1)'), jsonb_path_query_array('[{\"a\":1},{\"a\":2}]'::jsonb, '$[*].a')",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Jsonb(crate::backend::executor::jsonb::parse_jsonb_text("2").unwrap()),
                    Value::Jsonb(crate::backend::executor::jsonb::parse_jsonb_text("[1,2]").unwrap()),
                ]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn jsonpath_cast_and_silent_behavior_work() {
    let base = temp_dir("jsonpath_cast");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select 'strict $.a'::jsonpath, jsonb_path_query_array('{}'::jsonb, 'strict $.a', '{}'::jsonb, true), jsonb_path_match('1'::jsonb, '$', '{}'::jsonb, true)",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![
                    Value::JsonPath("strict $.\"a\"".into()),
                    Value::Jsonb(crate::backend::executor::jsonb::parse_jsonb_text("[]").unwrap()),
                    Value::Null,
                ]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'strict $['::jsonpath",
    )
    .unwrap_err();
    assert!(matches!(err, ExecError::InvalidStorageValue { column, .. } if column == "jsonpath"));
}

#[test]
fn jsonpath_large_subscript_uses_pg_error_text() {
    let base = temp_dir("jsonpath_large_subscript_error");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('[1]', 'lax $[10000000000000000]')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidStorageValue { details, .. }
            if details == "jsonpath array subscript is out of integer range"
    ));
}

#[test]
fn jsonpath_functions_accept_named_sql_args() {
    let base = temp_dir("jsonpath_named_args");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_exists(target => '[{\"a\":1},{\"a\":2},{\"a\":3}]'::jsonb, path => '$[*] ? (@.a > $min && @.a < $max)', vars => '{\"min\":1,\"max\":3}'::jsonb, silent => false)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(true)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonb_set_lax_accepts_named_sql_args() {
    let base = temp_dir("jsonb_set_lax_named_args");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_set_lax(target => '{\"a\":1,\"b\":2}'::jsonb, path => ARRAY['b']::varchar[], new_value => null, null_value_treatment => 'delete_key')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Jsonb(
                    crate::backend::executor::jsonb::parse_jsonb_text("{\"a\":1}").unwrap()
                )]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn trim_like_and_regexp_string_functions_work() {
    let base = temp_dir("strings_trim_like_regexp");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select trim(leading 'x' from 'xxxabc'), \
                trim(trailing 'x' from 'abcxxx'), \
                'hawkeye' like 'h%eye', \
                'hawkeye' ilike 'H%', \
                'ro_view1' like E'r_\\\\_view%', \
                'h%' like 'h#%' escape '#', \
                regexp_like('Steven', '^Ste(v|ph)en$'), \
                regexp_replace('AAA aaa', 'A+', 'Z', 'gi')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("abc".into()),
                    Value::Text("abc".into()),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Text("Z Z".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn trim_without_explicit_trim_chars_and_text_substring_work() {
    let base = temp_dir("strings_trim_substring_text");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
            "select trim(both from '  bunch o blanks  '), trim(leading from '  bunch o blanks  '), trim(trailing from '  bunch o blanks  '), substring('1234567890' from 3), substring('1234567890' from 4 for 3), substring('string' from -10 for 2147483646)",
        )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Text("bunch o blanks".into()));
            assert_eq!(rows[0][1], Value::Text("bunch o blanks  ".into()));
            assert_eq!(rows[0][2], Value::Text("  bunch o blanks".into()));
            assert_eq!(rows[0][3], Value::Text("34567890".into()));
            assert_eq!(rows[0][4], Value::Text("456".into()));
            assert_eq!(rows[0][5], Value::Text("string".into()));
        }
        other => panic!("expected query result, got {:?}", other),
    }

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select substring('string' from -10 for -2147483646)",
    )
    .expect_err("negative length should error");
    assert!(matches!(err, ExecError::NegativeSubstringLength));

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select substring('1234567890' for 3), substring('1234567890' for 0)",
        )
        .unwrap(),
        vec![vec![Value::Text("123".into()), Value::Text("".into())]],
    );

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select substr('WS.001.1a'::char(20), 1, 2), \
                    substring('WS.001.1a'::char(20) from 1 for 2), \
                    substring('WS.001.1a'::varchar(20) from 4), \
                    substring('abcdef'::char(6) similar 'a#\"(b_d)#\"%' escape '#')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("WS".into()),
            Value::Text("WS".into()),
            Value::Text("001.1a".into()),
            Value::Text("bcd".into()),
        ]],
    );
}

#[test]
fn regexp_scalar_functions_work() {
    let base = temp_dir("regexp_scalar_functions");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select regexp_count('123123123123', '123', 3), \
                regexp_instr('abcabcabc', 'a.c', 1, 3), \
                regexp_substr('1234567890', '(123)(4(56)(78))', 1, 1, 'i', 3), \
                regexp_match('foobarbequebaz', '(bar)(.*)(baz)'), \
                regexp_split_to_array('the quick brown fox', '\\s+')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int32(3),
                    Value::Int32(7),
                    Value::Text("56".into()),
                    Value::Array(vec![
                        Value::Text("bar".into()),
                        Value::Text("beque".into()),
                        Value::Text("baz".into()),
                    ]),
                    Value::Array(vec![
                        Value::Text("the".into()),
                        Value::Text("quick".into()),
                        Value::Text("brown".into()),
                        Value::Text("fox".into()),
                    ]),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn regexp_match_edge_cases_work() {
    let base = temp_dir("regexp_match_edge_cases");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select regexp_match('abc', 'd') is null, regexp_match(null, 'a') is null",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(true), Value::Bool(true)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select regexp_match('abc', 'a', 'g')",
    )
    .expect_err("global regexp_match flag should error");
    assert!(matches!(
        err,
        ExecError::Regex(RegexError { sqlstate, message, hint, .. })
            if sqlstate == "22023"
                && message == "regexp_match() does not support the \"global\" option"
                && hint.as_deref() == Some("Use the regexp_matches function instead.")
    ));
}

#[test]
fn sql_regex_substring_forms_work() {
    let base = temp_dir("sql_regex_substring_forms");
    let txns = TransactionManager::new_durable(&base).unwrap();

    for (sql, expected) in [
        (
            "select substring('abcdefg' similar 'a#\"(b_d)#\"%' escape '#')",
            Value::Text("bcd".into()),
        ),
        (
            "select substring('abcdefg' from 'a#\"(b_d)#\"%' for '#')",
            Value::Text("bcd".into()),
        ),
        (
            "select substring('abcdefg' similar 'a#\"%#\"g' escape '#')",
            Value::Text("bcdef".into()),
        ),
        (
            "select substring('abcdefg' from 'c.e')",
            Value::Text("cde".into()),
        ),
        (
            "select substring('abcdefg' from 'b(.*)f')",
            Value::Text("cde".into()),
        ),
    ] {
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, sql).unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![expected.clone()]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    for sql in [
        "select substring('foo' from 'foo(bar)?') is null",
        "select substring('abcdefg' similar '%' escape null) is null",
    ] {
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, sql).unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Bool(true)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
}

#[test]
fn sql_regex_substring_errors_include_substring_context() {
    let base = temp_dir("sql_regex_substring_errors_include_context");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select substring('abcdefg' similar 'a*#\"%#\"g*#\"x' escape '#')",
    )
    .expect_err("substring similar with too many separators should error");

    assert!(matches!(
        err,
        ExecError::Regex(RegexError {
            message,
            context,
            ..
        }) if message
            == "SQL regular expression may not contain more than two escape-double-quote separators"
            && context.as_deref() == Some("SQL function \"substring\" statement 1")
    ));
}

#[test]
fn regexp_set_returning_functions_work() {
    let base = temp_dir("regexp_set_returning_functions");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select regexp_matches('foobarbequebaz', '(bar)(.*)(baz)'), regexp_split_to_table('a b  c', '\\s+')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Array(vec![
                            Value::Text("bar".into()),
                            Value::Text("beque".into()),
                            Value::Text("baz".into()),
                        ]),
                        Value::Text("a".into()),
                    ],
                    vec![Value::Null, Value::Text("b".into())],
                    vec![Value::Null, Value::Text("c".into())],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn string_to_table_works_in_from_clause() {
    let base = temp_dir("string_to_table_from");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select v, v is null as is_null from string_to_table('1,2,*,4', ',', '*') as t(v)",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["v", "is_null"]);
            assert_eq!(
                rows,
                vec![
                    vec![Value::Text("1".into()), Value::Bool(false)],
                    vec![Value::Text("2".into()), Value::Bool(false)],
                    vec![Value::Null, Value::Bool(true)],
                    vec![Value::Text("4".into()), Value::Bool(false)],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn string_to_table_select_list_expands_rows() {
    let base = temp_dir("string_to_table_select_list");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select string_to_table('ab', null)",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, vec!["string_to_table"]);
            assert_eq!(
                rows,
                vec![vec![Value::Text("a".into())], vec![Value::Text("b".into())],]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn integer_base_rendering_matches_postgres() {
    let base = temp_dir("strings_integer_base_rendering");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select to_bin(-1234), to_bin(-1234::bigint), to_bin(256*256*256 - 1), to_bin(256::bigint*256::bigint*256::bigint*256::bigint - 1), to_oct(-1234), to_oct(-1234::bigint), to_oct(256*256*256 - 1), to_oct(256::bigint*256::bigint*256::bigint*256::bigint - 1), to_hex(-1234), to_hex(-1234::bigint), to_hex(256*256*256 - 1), to_hex(256::bigint*256::bigint*256::bigint*256::bigint - 1)",
        )
        .unwrap(),
        vec![vec![
            Value::Text("11111111111111111111101100101110".into()),
            Value::Text(
                "1111111111111111111111111111111111111111111111111111101100101110".into(),
            ),
            Value::Text("111111111111111111111111".into()),
            Value::Text("11111111111111111111111111111111".into()),
            Value::Text("37777775456".into()),
            Value::Text("1777777777777777775456".into()),
            Value::Text("77777777".into()),
            Value::Text("37777777777".into()),
            Value::Text("fffffb2e".into()),
            Value::Text("fffffffffffffb2e".into()),
            Value::Text("ffffff".into()),
            Value::Text("ffffffff".into()),
        ]],
    );
}

#[test]
fn similar_to_predicates_work() {
    let base = temp_dir("similar_to_predicates");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'abcdefg' similar to '_bcd%', \
                'abcdefg' similar to '_bcd#%' escape '#', \
                'abcd%' similar to '_bcd#%' escape '#', \
                'abcdefg' not similar to 'bcd%'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Bool(true),
                    Value::Bool(true),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn trim_supports_bytea_arguments() {
    let base = temp_dir("strings_trim_bytea");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select trim(E'\\\\000'::bytea from E'\\\\000Tom\\\\000'::bytea)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bytea(b"Tom".to_vec())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn text_helper_functions_work() {
    let base = temp_dir("strings_text_helper_functions");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select strpos('high', 'ig'), replace('abcdef', 'cd', 'XX'), split_part('a,b,c', ',', 2), initcap('hi THOMAS'), lpad('hi', 5, 'xy'), rpad('hi', 5, 'xy'), translate('12345', '143', 'ax'), ascii('x'), chr(120)",
        )
        .unwrap(),
        vec![vec![
            Value::Int32(2),
            Value::Text("abXXef".into()),
            Value::Text("b".into()),
            Value::Text("Hi Thomas".into()),
            Value::Text("xyxhi".into()),
            Value::Text("hixyx".into()),
            Value::Text("a2x5".into()),
            Value::Int32(120),
            Value::Text("x".into()),
        ]],
    );
}

#[test]
fn text_overlay_follows_postgres_rules() {
    let base = temp_dir("strings_text_overlay");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select overlay('abcdef' placing '45' from 4), overlay('yabadoo' placing 'daba' from 5), overlay('yabadoo' placing 'daba' from 5 for 0), overlay('babosa' placing 'ubb' from 2 for 4)",
        )
        .unwrap(),
        vec![vec![
            Value::Text("abc45f".into()),
            Value::Text("yabadaba".into()),
            Value::Text("yabadabadoo".into()),
            Value::Text("bubba".into()),
        ]],
    );

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select overlay('abcdef' placing '45' from 0)",
    )
    .unwrap_err();
    assert!(matches!(err, ExecError::NegativeSubstringLength));
}

#[test]
fn unistr_function_decodes_and_validates_unicode_escapes() {
    let base = temp_dir("strings_unistr");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            r"select unistr('\0064at\+0000610'), unistr('d\u0061t\U000000610'), unistr('a\\b')",
        )
        .unwrap(),
        vec![vec![
            Value::Text("data0".into()),
            Value::Text("data0".into()),
            Value::Text(r"a\b".into()),
        ]],
    );

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        r"select unistr('wrong: \db99')",
    )
    .unwrap_err();
    assert!(format!("{err:?}").contains("invalid Unicode surrogate pair"));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        r"select unistr('wrong: \U002FFFFF')",
    )
    .unwrap_err();
    assert!(format!("{err:?}").contains("invalid Unicode code point: 2FFFFF"));
}

#[test]
fn bytea_hash_and_encoding_functions_work() {
    let base = temp_dir("strings_bytea_helper_functions");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select encode(E'\\\\336\\\\255\\\\276\\\\357'::bytea, 'hex'), decode('deadbeef', 'hex'), sha256('abc'), crc32('abc'), crc32c('abc'), reverse(E'\\\\001\\\\002\\\\003'::bytea), position(E'\\\\002\\\\003'::bytea in E'\\\\001\\\\002\\\\003\\\\002'::bytea), substring(E'\\\\001\\\\002\\\\003\\\\004'::bytea from 2 for 2), overlay(E'\\\\001\\\\002\\\\003\\\\004'::bytea placing E'\\\\252\\\\273'::bytea from 2 for 2), get_bit(E'\\\\200'::bytea, 0), set_bit(E'\\\\000'::bytea, 0, 1), get_byte(E'\\\\001\\\\002'::bytea, 1), set_byte(E'\\\\001\\\\002'::bytea, 1, 255), bit_count(E'\\\\360'::bytea)",
        )
        .unwrap(),
        vec![vec![
            Value::Text("deadbeef".into()),
            Value::Bytea(vec![0xde, 0xad, 0xbe, 0xef]),
            Value::Bytea(Sha256::digest(b"abc").to_vec()),
            Value::Int64(crc32fast::hash(b"abc") as i64),
            Value::Int64(crc32c::crc32c(b"abc") as i64),
            Value::Bytea(vec![3, 2, 1]),
            Value::Int32(2),
            Value::Bytea(vec![2, 3]),
            Value::Bytea(vec![1, 0xaa, 0xbb, 4]),
            Value::Int32(1),
            Value::Bytea(vec![0x80]),
            Value::Int32(2),
            Value::Bytea(vec![1, 255]),
            Value::Int64(4),
        ]],
    );
}

#[test]
fn bytea_concat_operator_concatenates_buffers() {
    let base = temp_dir("bytea_concat_operator");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select E'\\\\001\\\\002'::bytea || E'\\\\003\\\\004'::bytea",
        )
        .unwrap(),
        vec![vec![Value::Bytea(vec![1, 2, 3, 4])]],
    );
}

#[test]
fn length_accepts_bytea_argument() {
    let base = temp_dir("length_bytea");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select length(E'\\\\001\\\\002\\\\003'::bytea)",
        )
        .unwrap(),
        vec![vec![Value::Int32(3)]],
    );
}

#[test]
fn generate_series_accepts_named_sql_args_in_from() {
    let base = temp_dir("generate_series_named_args");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from generate_series(stop => 3, start => 1)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Int32(1)],
                    vec![Value::Int32(2)],
                    vec![Value::Int32(3)],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn json_jsonb_table_functions_accept_named_sql_args() {
    let base = temp_dir("json_table_functions_named_args");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select json_object_keys(from_json => '{\"a\":1,\"b\":2}'::json)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Text("a".into())], vec![Value::Text("b".into())],]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_array_elements_text(from_json => '[1,true,null]'::jsonb)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Text("1".into())],
                    vec![Value::Text("true".into())],
                    vec![Value::Null],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from json_each_text(from_json => '{\"a\":1,\"b\":null}'::json) order by key",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Text("a".into()), Value::Text("1".into())],
                    vec![Value::Text("b".into()), Value::Null],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonb_table_functions_coerce_unknown_literal_inputs() {
    let base = temp_dir("jsonb_table_functions_unknown_literals");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_array_elements_text('[1,true,null]')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Text("1".into())],
                    vec![Value::Text("true".into())],
                    vec![Value::Null],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonb_path_operators_coerce_unknown_path_literals() {
    let base = temp_dir("jsonb_path_literal_coercion");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '{\"a\":{\"b\":2}}'::jsonb #> '{a,b}', '{\"a\":{\"b\":2}}'::jsonb #>> '{a,b}'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Jsonb(crate::backend::executor::jsonb::parse_jsonb_text("2").unwrap()),
                    Value::Text("2".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonb_scalar_casts_match_pg_scalar_rules() {
    let base = temp_dir("jsonb_scalar_casts");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 'true'::jsonb::bool, '12345'::jsonb::int4, '1.0'::jsonb::float8, '12345.05'::jsonb::numeric, 'null'::jsonb::int4",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Bool(true),
                    Value::Int32(12345),
                    Value::Float64(1.0),
                    Value::Numeric(crate::backend::executor::exec_expr::parse_numeric_text("12345.05").unwrap()),
                    Value::Null,
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '[1.0]'::jsonb::float8",
    )
    .unwrap_err()
    {
        ExecError::DetailedError {
            message, sqlstate, ..
        } => {
            assert_eq!(message, "cannot cast jsonb array to type double precision");
            assert_eq!(sqlstate, "22023");
        }
        other => panic!("expected cast failure, got {:?}", other),
    }
}

#[test]
fn jsonb_subscript_reads_match_basic_pg_cases() {
    let base = temp_dir("jsonb_subscript_reads");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select ('123'::jsonb)['a'], ('123'::jsonb)[0], ('123'::jsonb)[NULL], ('{\"a\":1}'::jsonb)['a'], ('{\"a\":1}'::jsonb)[0], ('[10,20,30]'::jsonb)[1]",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    crate::backend::executor::jsonb::jsonb_to_value(
                        &crate::backend::executor::jsonb::JsonbValue::Numeric(
                            crate::backend::executor::exec_expr::parse_numeric_text("1").unwrap(),
                        ),
                    ),
                    Value::Null,
                    crate::backend::executor::jsonb::jsonb_to_value(
                        &crate::backend::executor::jsonb::JsonbValue::Numeric(
                            crate::backend::executor::exec_expr::parse_numeric_text("20").unwrap(),
                        ),
                    ),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select ('[1,2,3]'::jsonb)[1:]",
    )
    .unwrap_err()
    {
        ExecError::Parse(ParseError::DetailedError {
            message, sqlstate, ..
        }) => {
            assert_eq!(message, "jsonb subscript does not support slices");
            assert_eq!(sqlstate, "0A000");
        }
        other => panic!("expected slice failure, got {:?}", other),
    }
}

#[test]
fn jsonb_subscript_assignment_updates_objects_arrays_and_nulls() {
    let db = Database::open(temp_dir("jsonb_subscript_assignment"), 16).unwrap();
    let mut session = Session::new(1);

    db.execute(1, "create temp table t (id int4, test_json jsonb)")
        .unwrap();
    db.execute(
        1,
        "insert into t values (1, '{}'), (2, '{\"key\":\"value\"}'), (3, null)",
    )
    .unwrap();

    db.execute(1, "update t set test_json['a'] = '1' where id = 1")
        .unwrap();
    db.execute(
        1,
        "update t set test_json['a'] = '[1, 2, 3]'::jsonb where id = 2",
    )
    .unwrap();
    db.execute(1, "update t set test_json['a'] = '1' where id = 3")
        .unwrap();

    match session
        .execute(&db, "select test_json from t order by id")
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("{\"a\":1}").unwrap()
                    )],
                    vec![Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text(
                            "{\"a\":[1,2,3],\"key\":\"value\"}"
                        )
                        .unwrap()
                    )],
                    vec![Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("{\"a\":1}").unwrap()
                    )],
                ]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }

    db.execute(1, "delete from t").unwrap();
    db.execute(1, "insert into t values (1, '[0]')").unwrap();
    db.execute(1, "update t set test_json[5] = '1'").unwrap();
    db.execute(1, "update t set test_json[-4] = '1'").unwrap();

    match session.execute(&db, "select test_json from t").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Jsonb(
                    crate::backend::executor::jsonb::parse_jsonb_text("[0,null,1,null,null,1]")
                        .unwrap()
                )]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }

    db.execute(1, "delete from t").unwrap();
    db.execute(1, "insert into t values (1, '{}')").unwrap();
    db.execute(1, "update t set test_json['a'][0]['b'][0]['c'] = '1'")
        .unwrap();

    match session.execute(&db, "select test_json from t").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Jsonb(
                    crate::backend::executor::jsonb::parse_jsonb_text(
                        "{\"a\":[{\"b\":[{\"c\":1}]}]}"
                    )
                    .unwrap()
                )]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn jsonb_path_query_works_in_select_list_and_from() {
    let base = temp_dir("jsonb_path_query_srf");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('[{\"a\":1},{\"a\":2},{\"a\":3}]'::jsonb, '$[*].a')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("1").unwrap()
                    )],
                    vec![Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("2").unwrap()
                    )],
                    vec![Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("3").unwrap()
                    )],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from jsonb_path_query(target => '[{\"a\":1},{\"a\":2},{\"a\":3}]'::jsonb, path => '$[*] ? (@.a > $min).a', vars => '{\"min\":1}'::jsonb)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("2").unwrap()
                    )],
                    vec![Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("3").unwrap()
                    )],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_arithmetic_recursive_and_subscripts_work() {
    let base = temp_dir("jsonpath_extended");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '$.a.**{2}.b'::jsonpath, '$ ? ((@ == 1) is unknown)'::jsonpath, '$[last]'::jsonpath, '$[0.5]'::jsonpath, '{\"a\":{\"b\":1}}'::jsonb @? 'lax $.**{2}', '{\"a\":12}'::jsonb @? '$.a + 2', '[1]'::jsonb @? '$[0.5]'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::JsonPath("$.\"a\".**{2}.\"b\"".into()),
                    Value::JsonPath("$ ? ((@ == 1) is unknown)".into()),
                    Value::JsonPath("$[last]".into()),
                    Value::JsonPath("$[0.5]".into()),
                    Value::Bool(true),
                    Value::Bool(true),
                    Value::Bool(true),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_exists_returns_false_for_silent_errors() {
    let base = temp_dir("jsonpath_exists_silent_errors");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '{\"a\":12}'::jsonb @? '$.b + 2', jsonb_path_exists('[{\"a\":1},{\"a\":2},3]'::jsonb, 'strict $[*].a', silent => true)",
    )
        .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(false), Value::Bool(false)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_exists_propagates_non_silent_errors() {
    let base = temp_dir("jsonpath_exists_errors");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_exists('[{\"a\":1},{\"a\":2},3]'::jsonb, 'strict $[*].a', silent => false)",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidStorageValue { column, details }
            if column == "jsonpath" && details == "jsonpath member access requires object"
    ));
}

#[test]
fn jsonpath_lax_scalar_index_zero_returns_scalar() {
    let base = temp_dir("jsonpath_lax_scalar_index");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('1', 'lax $[0]')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Jsonb(
                    crate::backend::executor::jsonb::parse_jsonb_text("1").unwrap()
                )]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_lax_scalar_wildcard_returns_scalar() {
    let base = temp_dir("jsonpath_lax_scalar_wildcard");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('1', 'lax $[*]')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Jsonb(
                    crate::backend::executor::jsonb::parse_jsonb_text("1").unwrap()
                )]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_recursive_descent_includes_current_item_at_depth_zero() {
    let base = temp_dir("jsonpath_recursive_depth");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('{\"a\":{\"b\":1}}', 'lax $.**')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("{\"a\":{\"b\":1}}")
                            .unwrap()
                    )],
                    vec![Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("{\"b\":1}").unwrap()
                    )],
                    vec![Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("1").unwrap()
                    )],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('{\"a\":{\"b\":1}}', 'lax $.**{0}')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Jsonb(
                    crate::backend::executor::jsonb::parse_jsonb_text("{\"a\":{\"b\":1}}").unwrap()
                )]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('{\"a\":{\"b\":1}}', 'lax $.**{0 to last}')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("{\"a\":{\"b\":1}}")
                            .unwrap()
                    )],
                    vec![Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("{\"b\":1}").unwrap()
                    )],
                    vec![Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("1").unwrap()
                    )],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_is_unknown_treats_mixed_type_compare_as_unknown() {
    let base = temp_dir("jsonpath_is_unknown_compare");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb '1' @? '$ ? ((@ == \"1\") is unknown)'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(true)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_is_unknown_treats_predicate_arithmetic_errors_as_unknown() {
    let base = temp_dir("jsonpath_is_unknown_arithmetic");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb '[1,2,0,3]' @? '$[*] ? ((2 / @ > 0) is unknown)'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(true)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_strict_mixed_type_sequence_compare_returns_false() {
    let base = temp_dir("jsonpath_strict_mixed_type_compare");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb '{\"a\":[1,2,3],\"b\":[3,4,\"5\"]}' @? 'strict $ ? (@.a[*] >= @.b[*])'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(false)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_extended_subscripts_parse() {
    let base = temp_dir("jsonpath_extended_subscripts_parse");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '$[0,1]'::jsonpath, '$[last - 1]'::jsonpath, '$[2.5 - 1 to $.size() - 2]'::jsonpath, '$[last ? (@.type() == \"number\")]'::jsonpath",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::JsonPath("$[0, 1]".into()),
                    Value::JsonPath("$[last - 1]".into()),
                    Value::JsonPath("$[2.5 - 1 to $.size() - 2]".into()),
                    Value::JsonPath("$[last ? (@.type() == \"number\")]".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_extended_subscripts_work() {
    let base = temp_dir("jsonpath_extended_subscripts");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('[12,{\"a\":13},{\"b\":14}]', 'lax $[0,1].a'), jsonb_path_query('[1,2,3]', '$[last - 1]'), jsonb_path_query('[12,{\"a\":13},{\"b\":14},\"ccc\",true]', '$[2.5 - 1 to $.size() - 2]'), jsonb_path_query('[1,2,3]', '$[last ? (@.type() == \"number\")]')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Jsonb(
                            crate::backend::executor::jsonb::parse_jsonb_text("13").unwrap()
                        ),
                        Value::Jsonb(
                            crate::backend::executor::jsonb::parse_jsonb_text("2").unwrap()
                        ),
                        Value::Jsonb(
                            crate::backend::executor::jsonb::parse_jsonb_text("{\"a\":13}")
                                .unwrap()
                        ),
                        Value::Jsonb(
                            crate::backend::executor::jsonb::parse_jsonb_text("3").unwrap()
                        ),
                    ],
                    vec![
                        Value::Null,
                        Value::Null,
                        Value::Jsonb(
                            crate::backend::executor::jsonb::parse_jsonb_text("{\"b\":14}")
                                .unwrap()
                        ),
                        Value::Null,
                    ],
                    vec![
                        Value::Null,
                        Value::Null,
                        Value::Jsonb(
                            crate::backend::executor::jsonb::parse_jsonb_text("\"ccc\"").unwrap()
                        ),
                        Value::Null,
                    ],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('[]', '$[last ? (exists(last))]')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => assert!(rows.is_empty()),
        other => panic!("expected query result, got {:?}", other),
    }

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('[1,2,3]', '$[last ? (@.type() == \"string\")]')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidStorageValue { column, details }
            if column == "jsonpath"
                && details == "jsonpath array subscript is not a single numeric value"
    ));

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('[1,2,3]', '$[last ? (@.type() == \"string\")]', silent => true)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => assert!(rows.is_empty()),
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_expression_method_calls_parse() {
    let base = temp_dir("jsonpath_expression_method_calls_parse");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '(($.a - 5).abs() + 10)'::jsonpath, '-($.a * $.a).floor() % 4.3'::jsonpath",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::JsonPath("($.\"a\" - 5).abs() + 10".into()),
                    Value::JsonPath("-($.\"a\" * $.\"a\").floor() % 4.3".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_builtin_method_calls_parse() {
    let base = temp_dir("jsonpath_builtin_method_calls_parse");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '$.double().floor().ceiling().abs()'::jsonpath, '$.boolean()'::jsonpath, '$.string()'::jsonpath",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::JsonPath("$.double().floor().ceiling().abs()".into()),
                    Value::JsonPath("$.boolean()".into()),
                    Value::JsonPath("$.string()".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_numeric_method_calls_parse() {
    let base = temp_dir("jsonpath_numeric_method_calls_parse");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '$.number()'::jsonpath, '$.integer()'::jsonpath, '$.decimal(4,2)'::jsonpath",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::JsonPath("$.number()".into()),
                    Value::JsonPath("$.integer()".into()),
                    Value::JsonPath("$.decimal(4, 2)".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_datetime_method_calls_parse() {
    let base = temp_dir("jsonpath_datetime_method_calls_parse");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '$.bigint()'::jsonpath, '$.date()'::jsonpath, '$.time(6)'::jsonpath, '$.time_tz(4)'::jsonpath, '$.timestamp(2)'::jsonpath, '$.timestamp_tz()'::jsonpath, '$.datetime()'::jsonpath",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::JsonPath("$.bigint()".into()),
                    Value::JsonPath("$.date()".into()),
                    Value::JsonPath("$.time(6)".into()),
                    Value::JsonPath("$.time_tz(4)".into()),
                    Value::JsonPath("$.timestamp(2)".into()),
                    Value::JsonPath("$.timestamp_tz()".into()),
                    Value::JsonPath("$.datetime()".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_datetime_template_method_parse() {
    let base = temp_dir("jsonpath_datetime_template_method_parse");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '$.datetime(\"datetime template\")'::jsonpath, '$.datetime(\"dd-mm-yyyy\\\"T\\\"HH24:MI:SS\")'::jsonpath",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::JsonPath("$.datetime(\"datetime template\")".into()),
                    Value::JsonPath("$.datetime(\"dd-mm-yyyy\\\"T\\\"HH24:MI:SS\")".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_string_predicates_parse() {
    let base = temp_dir("jsonpath_string_predicates_parse");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '$ ? (@ starts with \"abc\")'::jsonpath, '$ ? (@ starts with $var)'::jsonpath, '$ ? (@ like_regex \"pattern\" flag \"iq\")'::jsonpath",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::JsonPath("$ ? (@ starts with \"abc\")".into()),
                    Value::JsonPath("$ ? (@ starts with $var)".into()),
                    Value::JsonPath("$ ? (@ like_regex \"pattern\" flag \"iq\")".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_expression_method_calls_work() {
    let base = temp_dir("jsonpath_expression_method_calls");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('{\"a\":2}', '($.a - 5).abs() + 10'), jsonb_path_query('{\"a\":2.5}', '-($.a * $.a).floor() % 4.3'), jsonb_path_query('[0,1,-2,-3.4,5.6]', '$[*].ceiling().abs().type()')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 5);
            match &rows[0][0] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "13"
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
            match &rows[0][1] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "-1.7"
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
            for row in rows {
                match &row[2] {
                    Value::Jsonb(bytes) => assert_eq!(
                        crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                        "\"number\""
                    ),
                    other => panic!("expected jsonb, got {:?}", other),
                }
            }
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_numeric_method_calls_work() {
    let base = temp_dir("jsonpath_numeric_method_calls");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('1e1000', '$.number()'), jsonb_path_query('1.83', '$.integer()'), jsonb_path_query('1234.5678', '$.decimal(+6, -2)')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            match &rows[0][0] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
            match &rows[0][1] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "2"
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
            match &rows[0][2] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "1200"
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_datetime_method_calls_work() {
    let base = temp_dir("jsonpath_datetime_method_calls");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('1.83', '$.bigint()'), jsonb_path_query('\"2023-08-15\"', '$.date().type()'), jsonb_path_query('\"12:34:56.789\"', '$.time(2).string()'), jsonb_path_query('\"12:34:56+05:20\"', '$.time_tz().type()'), jsonb_path_query('\"2023-08-15 12:34:56.789\"', '$.timestamp(2).string()'), jsonb_path_query('\"2023-08-15 12:34:56 +05:20\"', '$.timestamp_tz().type()'), jsonb_path_query('\"2023-08-15 12:34:56\"', '$.datetime().type()'), jsonb_path_query_array('[\"1\", \"2\"]', '$.bigint()')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            match &rows[0][0] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "2"
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
            match &rows[0][1] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "\"date\""
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
            match &rows[0][2] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "\"12:34:56.78\""
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
            match &rows[0][3] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "\"time with time zone\""
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
            match &rows[0][4] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "\"2023-08-15T12:34:56.78\""
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
            match &rows[0][5] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "\"timestamp with time zone\""
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
            match &rows[0][6] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "\"timestamp without time zone\""
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
            match &rows[0][7] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "[1, 2]"
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_datetime_template_method_work() {
    let base = temp_dir("jsonpath_datetime_template_method_work");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('\"10-03-2017\"', '$.datetime(\"dd-mm-yyyy\")'), jsonb_path_query('\"10-03-2017 12:34\"', '$.datetime(\"dd-mm-yyyy HH24:MI\")'), jsonb_path_query('\"10-03-2017 12:34\"', '$.datetime(\"dd-mm-yyyy HH24:MI\").type()'), jsonb_path_query('\"12:34:56 +05:20\"', '$.datetime(\"HH24:MI:SS TZH:TZM\").type()'), jsonb_path_query('\"10-03-2017T12:34:56\"', '$.datetime(\"dd-mm-yyyy\\\"T\\\"HH24:MI:SS\")')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            match &rows[0][0] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "\"2017-03-10\""
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
            match &rows[0][1] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "\"2017-03-10T12:34:00\""
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
            match &rows[0][2] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "\"timestamp without time zone\""
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
            match &rows[0][3] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "\"time with time zone\""
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
            match &rows[0][4] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "\"2017-03-10T12:34:56\""
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_string_predicates_work() {
    let base = temp_dir("jsonpath_string_predicates_work");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb '\"abcdef\"' @? '$ ? (@ starts with \"abc\")', jsonb '\"AbCdEf\"' @? '$ ? (@ like_regex \"^abc\" flag \"i\")', jsonb_path_exists('\"abcdef\"', '$ ? (@ starts with $prefix)', '{\"prefix\":\"abc\"}')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Bool(true), Value::Bool(true), Value::Bool(true)]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb '123' @? '$ ? (@ starts with \"1\")', jsonb '123' @? '$ ? (@ like_regex \"1\")'",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Bool(false), Value::Bool(false)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_builtin_method_calls_work() {
    let base = temp_dir("jsonpath_builtin_method_calls");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('1.23', '$.double()'), jsonb_path_query_array('[1, \"yes\", false]', '$[*].boolean()'), jsonb_path_query_array('[1.23, \"yes\", false]', '$[*].string().type()')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            match &rows[0][0] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "1.23"
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
            match &rows[0][1] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "[true, true, false]"
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
            match &rows[0][2] {
                Value::Jsonb(bytes) => assert_eq!(
                    crate::backend::executor::jsonb::render_jsonb_bytes(bytes).unwrap(),
                    "[\"string\", \"string\", \"string\"]"
                ),
                other => panic!("expected jsonb, got {:?}", other),
            }
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonpath_datetime_method_calls_errors() {
    let base = temp_dir("jsonpath_datetime_method_calls_errors");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('true', '$.bigint()')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidStorageValue { column, details }
            if column == "jsonpath"
                && details == "jsonpath item method .bigint() can only be applied to a string or numeric value"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('\"bogus\"', '$.datetime()')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, hint, .. }
            if message == "datetime format is not recognized: \"bogus\""
                && hint.as_deref() == Some("Use a datetime template argument to specify the input data format.")
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('\"12:34:56\"', '$.time(12345678901)')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidStorageValue { column, details }
            if column == "jsonpath"
                && details == "time precision of jsonpath item method .time() is out of range for type integer"
    ));
}

#[test]
fn jsonpath_datetime_template_method_errors() {
    let base = temp_dir("jsonpath_datetime_template_method_errors");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('\"12:34\"', '$.datetime(\"aaa\")')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidStorageValue { column, details }
            if column == "jsonpath"
                && details == "invalid datetime format separator: \"a\""
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('\"aaaa\"', '$.datetime(\"HH24\")')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, detail, .. }
            if message == "invalid value \"aa\" for \"HH24\""
                && detail.as_deref() == Some("Value must be an integer.")
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('\"10-03-2017 12:34\"', '$.datetime(\"dd-mm-yyyy\")')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, .. }
            if message == "trailing characters remain in input string after datetime format"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('\"10-03-2017t12:34:56\"', '$.datetime(\"dd-mm-yyyy\\\"T\\\"HH24:MI:SS\")')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, .. }
            if message == "unmatched format character \"T\""
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('\"10-03-2017 12:34:56\"', '$.datetime(\"dd-mm-yyyy\\\"T\\\"HH24:MI:SS\")')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::DetailedError { message, .. }
            if message == "unmatched format character \"T\""
    ));
}

#[test]
fn jsonpath_string_predicates_errors() {
    let base = temp_dir("jsonpath_string_predicates_errors");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '$ ? (@ like_regex \"pattern\" flag \"a\")'::jsonpath",
    )
    .unwrap_err();
    assert!(
        matches!(
            &err,
            ExecError::InvalidStorageValue { column, details }
                if column == "jsonpath"
                    && details == "invalid input syntax for type jsonpath: \"$ ? (@ like_regex \"pattern\" flag \"a\")\""
        ),
        "{err:?}"
    );
}

#[test]
fn jsonpath_numeric_method_calls_errors() {
    let base = temp_dir("jsonpath_numeric_method_calls_errors");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('\"1.23aaa\"', '$.number()')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidStorageValue { column, details }
            if column == "jsonpath"
                && details == "argument \"1.23aaa\" of jsonpath item method .number() is invalid for type numeric"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('12345678901', '$.integer()')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidStorageValue { column, details }
            if column == "jsonpath"
                && details == "argument \"12345678901\" of jsonpath item method .integer() is invalid for type integer"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('12.3', '$.decimal(12345678901,1)')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidStorageValue { column, details }
            if column == "jsonpath"
                && details == "precision of jsonpath item method .decimal() is out of range for type integer"
    ));
}

#[test]
fn jsonpath_builtin_method_calls_errors() {
    let base = temp_dir("jsonpath_builtin_method_calls_errors");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('\"nan\"', '$.double()')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidStorageValue { column, details }
            if column == "jsonpath"
                && details == "NaN or Infinity is not allowed for jsonpath item method .double()"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_path_query('1.23', '$.boolean()')",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::InvalidStorageValue { column, details }
            if column == "jsonpath"
                && details == "argument \"1.23\" of jsonpath item method .boolean() is invalid for type boolean"
    ));
}

#[test]
fn getdatabaseencoding_and_jsonpath_unicode_work() {
    let base = temp_dir("jsonpath_unicode");
    let txns = TransactionManager::new_durable(&base).unwrap();

    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select getdatabaseencoding(), '{\"ꯍ\":1,\"😄\":2}'::jsonb @? '$.\"\\uaBcD\"', '{\"ꯍ\":1,\"😄\":2}'::jsonb @? '$.\"\\ud83d\\ude04\"'",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![
                        Value::Text("UTF8".into()),
                        Value::Bool(true),
                        Value::Bool(true),
                    ]]
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '\"\\u\"'::jsonpath",
    )
    .unwrap_err();
    assert!(matches!(err, ExecError::InvalidStorageValue { column, .. } if column == "jsonpath"));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '\"\\ud83dX\"'::jsonpath",
    )
    .unwrap_err();
    assert!(matches!(err, ExecError::InvalidStorageValue { column, .. } if column == "jsonpath"));
}

#[test]
fn concat_text_array_and_jsonb_work() {
    let base = temp_dir("concat_ops");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select 'four: ' || 2 + 2, ARRAY[1,2] || 3, 0 || ARRAY[1,2], ARRAY[1,2] || ARRAY[3,4], '{\"a\":1}'::jsonb || '{\"b\":2,\"a\":9}'::jsonb",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![
                    Value::Text("four: 4".into()),
                    Value::Array(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]),
                    Value::Array(vec![Value::Int32(0), Value::Int32(1), Value::Int32(2)]),
                    Value::Array(vec![
                        Value::Int32(1),
                        Value::Int32(2),
                        Value::Int32(3),
                        Value::Int32(4)
                    ]),
                    Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text(
                            "{\"a\": 9, \"b\": 2}"
                        )
                        .unwrap()
                    ),
                ]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn concat_accepts_jsonb_delete_rhs() {
    let base = temp_dir("concat_jsonb_delete_rhs");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_build_array('x') || '[10,20,30]'::jsonb - 0, jsonb_build_array('x') || ('[10,20,30]'::jsonb - 0)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            let expected = Value::Jsonb(
                crate::backend::executor::jsonb::parse_jsonb_text("[\"x\",20,30]").unwrap(),
            );
            assert_eq!(rows, vec![vec![expected.clone(), expected]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn concat_rejects_non_text_nonarray_non_jsonb_operands() {
    let base = temp_dir("concat_rejects");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let err = run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 3 || 4.0").unwrap_err();
    assert!(matches!(
        err,
        ExecError::Parse(ParseError::UndefinedOperator { op: "||", .. })
    ));
}

#[test]
fn left_and_repeat_follow_postgres_text_semantics() {
    let base = temp_dir("left_repeat");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select left('ahoj', 2), left('ahoj', -1), repeat('ab', 3), repeat('ab', -1)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("ah".into()),
                    Value::Text("aho".into()),
                    Value::Text("ababab".into()),
                    Value::Text("".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn concat_right_and_quote_literal_are_available_to_sql() {
    let base = temp_dir("text_builtins");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select concat('one', 2, true), concat_ws('#', 'one', 2, null, false), right('ahoj', 2), quote_literal(E'\\\\')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("one2t".into()),
                    Value::Text("one#2#f".into()),
                    Value::Text("oj".into()),
                    Value::Text("E'\\\\'".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn format_supports_common_postgres_specifiers() {
    let base = temp_dir("format_builtin");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select format('Hello %s', 'World'), format('INSERT INTO %I VALUES(%L,%L)', 'mytab', 10, 'Hello'), format('%1$s %3$s', 1, 2, 3), format('>>%10s<<', 'Hello')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("Hello World".into()),
                    Value::Text("INSERT INTO mytab VALUES('10','Hello')".into()),
                    Value::Text("1 3".into()),
                    Value::Text(">>     Hello<<".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn reverse_supports_text_and_bytea() {
    let base = temp_dir("reverse_text_and_bytea");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select reverse('abcde'), encode(reverse(E'\\\\001\\\\002\\\\003'::bytea), 'hex')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Text("edcba".into()),
                    Value::Text("030201".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn format_star_with_explicit_width_uses_next_value_argument() {
    let base = temp_dir("format_star_explicit_width");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select format('>>%*1$s<<', 10, 'Hello')",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text(">>     Hello<<".into())]]);
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn lower_supports_grouped_queries() {
    let base = temp_dir("lower_supports_grouped_queries");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
            &base,
            &txns,
            xid,
            "insert into people (id, name, note) values (1, 'alice', 'AAAA'), (2, 'bob', 'AAAA'), (3, 'carol', 'bbbb'), (4, 'dave', 'cccc'), (5, 'eve', 'cccc'), (6, 'frank', 'CCCC')",
        )
        .unwrap();
    txns.commit(xid).unwrap();
    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select lower(note), count(note) from people group by lower(note) having count(*) > 2 or min(id) = max(id) order by lower(note)",
            )
            .unwrap(),
            vec![
                vec![Value::Text("bbbb".into()), Value::Int64(1)],
                vec![Value::Text("cccc".into()), Value::Int64(3)],
            ],
        );
}

#[test]
fn jsonb_table_functions_and_agg_work() {
    let base = temp_dir("jsonb_table_functions");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'x'), (2, 'bob', 'y')",
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select key, value from jsonb_each('{\"a\":1,\"b\":null}'::jsonb) order by key",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Text("a".into()),
                        Value::Jsonb(
                            crate::backend::executor::jsonb::parse_jsonb_text("1").unwrap()
                        ),
                    ],
                    vec![
                        Value::Text("b".into()),
                        Value::Jsonb(
                            crate::backend::executor::jsonb::parse_jsonb_text("null").unwrap()
                        ),
                    ],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select key, value from jsonb_each('{\"a\":1,\"b\":null}') order by key",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![
                        Value::Text("a".into()),
                        Value::Jsonb(
                            crate::backend::executor::jsonb::parse_jsonb_text("1").unwrap()
                        ),
                    ],
                    vec![
                        Value::Text("b".into()),
                        Value::Jsonb(
                            crate::backend::executor::jsonb::parse_jsonb_text("null").unwrap()
                        ),
                    ],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_agg(id) from people",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Jsonb(
                    crate::backend::executor::jsonb::parse_jsonb_text("[1,2]").unwrap()
                )]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonb_array_length_and_each_errors_match_postgres() {
    let base = temp_dir("jsonb_array_length_and_each_errors");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_array_length('{\"f1\":1,\"f2\":[5,6]}')",
    )
    .unwrap_err();
    assert_eq!(
        format_exec_error(&err),
        "cannot get array length of a non-array"
    );
    assert!(matches!(
        err,
        ExecError::DetailedError { sqlstate, .. } if sqlstate == "22023"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_array_length('4')",
    )
    .unwrap_err();
    assert_eq!(
        format_exec_error(&err),
        "cannot get array length of a scalar"
    );
    assert!(matches!(
        err,
        ExecError::DetailedError { sqlstate, .. } if sqlstate == "22023"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from jsonb_each('[]')",
    )
    .unwrap_err();
    assert_eq!(
        format_exec_error(&err),
        "cannot call jsonb_each on a non-object"
    );
    assert!(matches!(
        err,
        ExecError::DetailedError { sqlstate, .. } if sqlstate == "22023"
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from jsonb_each_text('null')",
    )
    .unwrap_err();
    assert_eq!(
        format_exec_error(&err),
        "cannot call jsonb_each_text on a non-object"
    );
    assert!(matches!(
        err,
        ExecError::DetailedError { sqlstate, .. } if sqlstate == "22023"
    ));
}

#[test]
fn jsonb_builders_and_object_agg_work() {
    let base = temp_dir("jsonb_builders");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'x'), (2, 'bob', 'y')",
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select jsonb_build_array('a', 1, true), jsonb_build_object('a', 1, 'b', true), jsonb_object_agg(name, note) from people",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![
                        Value::Jsonb(
                            crate::backend::executor::jsonb::parse_jsonb_text("[\"a\",1,true]")
                                .unwrap()
                        ),
                        Value::Jsonb(
                            crate::backend::executor::jsonb::parse_jsonb_text(
                                "{\"a\":1,\"b\":true}"
                            )
                            .unwrap()
                        ),
                        Value::Jsonb(
                            crate::backend::executor::jsonb::parse_jsonb_text(
                                "{\"alice\":\"x\",\"bob\":\"y\"}"
                            )
                            .unwrap()
                        ),
                    ]]
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
}

#[test]
fn jsonb_build_object_can_wrap_object_agg() {
    let base = temp_dir("jsonb_build_object_wraps_object_agg");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql(
        &base,
        &txns,
        xid,
        "insert into people (id, name, note) values (1, 'alice', 'x'), (2, 'bob', 'y')",
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_build_object('notes', jsonb_object_agg(name, note)) from people",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Jsonb(
                    crate::backend::executor::jsonb::parse_jsonb_text(
                        "{\"notes\":{\"alice\":\"x\",\"bob\":\"y\"}}"
                    )
                    .unwrap()
                )]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn jsonb_variadic_calls_match_supported_postgres_cases() {
    let base = temp_dir("jsonb_variadic_calls");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select jsonb_build_array(VARIADIC NULL::text[]), \
                jsonb_build_array(VARIADIC '{{1,4},{2,5},{3,6}}'::int[][]), \
                jsonb_build_object(VARIADIC '{}'::text[]), \
                jsonb_build_object(VARIADIC '{{1,4},{2,5},{3,6}}'::int[][]), \
                jsonb_extract_path('{\"a\":{\"b\":2}}'::jsonb, VARIADIC ARRAY['a','b']::text[])",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Null,
                    Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text("[1,4,2,5,3,6]").unwrap()
                    ),
                    Value::Jsonb(crate::backend::executor::jsonb::parse_jsonb_text("{}").unwrap()),
                    Value::Jsonb(
                        crate::backend::executor::jsonb::parse_jsonb_text(
                            "{\"1\":4,\"2\":5,\"3\":6}"
                        )
                        .unwrap()
                    ),
                    Value::Jsonb(crate::backend::executor::jsonb::parse_jsonb_text("2").unwrap()),
                ]]
            );
        }
        other => panic!("expected query result, got {other:?}"),
    }
}

#[test]
fn insert_sql_varchar_rejects_non_space_overflow() {
    let base = temp_dir("insert_varchar_overflow");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let err = run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "insert into t (name) values ('cd')",
        varchar_catalog("t", 1),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::StringDataRightTruncation { ref ty } if ty == "character varying(1)"
    ));
}

#[test]
fn insert_sql_varchar_trims_trailing_spaces() {
    let base = temp_dir("insert_varchar_trailing_spaces");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql_with_catalog(
        &base,
        &txns,
        xid,
        "insert into t (name) values ('c     ')",
        varchar_catalog("t", 1),
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select name from t",
        varchar_catalog("t", 1),
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("c".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn insert_sql_varchar_counts_characters_not_bytes() {
    let base = temp_dir("insert_varchar_multibyte");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql_with_catalog(
        &base,
        &txns,
        xid,
        "insert into t (name) values ('éé')",
        varchar_catalog("t", 2),
    )
    .unwrap();
    txns.commit(xid).unwrap();

    let err = run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "insert into t (name) values ('ééé')",
        varchar_catalog("t", 2),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::StringDataRightTruncation { ref ty } if ty == "character varying(2)"
    ));
}

#[test]
fn insert_sql_char_pads_to_declared_length() {
    let base = temp_dir("insert_char_padding");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql_with_catalog(
        &base,
        &txns,
        xid,
        "insert into t (name) values ('bbbb')",
        char_catalog("t", 8),
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select name from t",
        char_catalog("t", 8),
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("bbbb    ".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn char_to_text_cast_trims_trailing_spaces() {
    let base = temp_dir("char_to_text_trim");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql_with_catalog(
        &base,
        &txns,
        xid,
        "insert into t (name) values ('BBBB')",
        char_catalog("t", 8),
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select lower(name) from t",
        char_catalog("t", 8),
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Text("bbbb".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn insert_sql_numeric_round_trips_through_storage() {
    let base = temp_dir("insert_numeric_roundtrip");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql_with_catalog(
        &base,
        &txns,
        xid,
        "insert into t (value) values (1.25::numeric)",
        numeric_catalog("t"),
    )
    .unwrap();
    txns.commit(xid).unwrap();

    match run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select value from t",
        numeric_catalog("t"),
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Numeric("1.25".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn scalar_subquery_target_list_returns_per_row_counts() {
    let mut harness = seed_people_and_pets("scalar_subquery_target_list");
    assert_query_rows(
            harness
                .execute(
                    INVALID_TRANSACTION_ID,
                    "select p.name, (select count(*) from pets q where q.owner_id = p.id) from people p order by p.id",
                )
                .unwrap(),
            vec![
                vec![Value::Text("alice".into()), Value::Int64(2)],
                vec![Value::Text("bob".into()), Value::Int64(1)],
                vec![Value::Text("carol".into()), Value::Int64(0)],
            ],
        );
}

#[test]
fn range_constructor_and_accessor_semantics() {
    let base = temp_dir("range_constructor_accessors");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select \
                int4range(1, 10, '[]')::text, \
                daterange('2000-01-10', '2000-01-20', '[]')::text, \
                numrange(1.7, 1.7, '[]')::text, \
                numrange(1.7, 1.7, '()')::text, \
                lower(int4range(1, 10))::text, \
                upper(int4range(1, 10))::text, \
                lower(int4range(null, 10))::text, \
                upper(int4range(1, null))::text, \
                lower_inf(int4range(null, 10)), \
                upper_inf(int4range(1, null)), \
                lower_inc('empty'::int4range), \
                upper_inf('empty'::int4range)",
        )
        .unwrap(),
        vec![vec![
            Value::Text("[1,11)".into()),
            Value::Text("[2000-01-10,2000-01-21)".into()),
            Value::Text("[1.7,1.7]".into()),
            Value::Text("empty".into()),
            Value::Text("1".into()),
            Value::Text("10".into()),
            Value::Null,
            Value::Null,
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(false),
        ]],
    );
}

#[test]
fn range_set_operators_and_aggregate_work() {
    let base = temp_dir("range_set_operators");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select \
                (int4range(1, 5) + int4range(5, 10))::text, \
                (int4range(1, 10) * int4range(5, 20))::text, \
                (int4range(1, 10) - int4range(5, 20))::text, \
                range_merge(int4range(1, 5), int4range(10, 15))::text",
        )
        .unwrap(),
        vec![vec![
            Value::Text("[1,10)".into()),
            Value::Text("[5,10)".into()),
            Value::Text("[1,5)".into()),
            Value::Text("[1,15)".into()),
        ]],
    );

    let xid = txns.begin();
    run_sql_with_catalog(
        &base,
        &txns,
        xid,
        "insert into t (id, span) values \
            (1, '[1,10)'::int4range), \
            (2, '[5,20)'::int4range), \
            (3, null)",
        range_catalog("t", crate::backend::parser::SqlTypeKind::Int4Range),
    )
    .unwrap();
    txns.commit(xid).unwrap();

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select range_intersect_agg(span)::text from t",
            range_catalog("t", crate::backend::parser::SqlTypeKind::Int4Range),
        )
        .unwrap(),
        vec![vec![Value::Text("[5,10)".into())]],
    );

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select range_intersect_agg(span)::text from (select null::int4range as span) q",
            range_catalog("t", crate::backend::parser::SqlTypeKind::Int4Range),
        )
        .unwrap(),
        vec![vec![Value::Null]],
    );
}

#[test]
fn range_storage_ordering_grouping_and_joining_work() {
    let base = temp_dir("range_storage_grouping");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let xid = txns.begin();
    run_sql_with_catalog(
        &base,
        &txns,
        xid,
        "insert into t (id, span) values \
            (1, '[5,7)'::int4range), \
            (2, 'empty'::int4range), \
            (3, '[1,3)'::int4range), \
            (4, '[1,3)'::int4range)",
        range_catalog("t", crate::backend::parser::SqlTypeKind::Int4Range),
    )
    .unwrap();
    txns.commit(xid).unwrap();

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select span::text from t order by t.span, id",
            range_catalog("t", crate::backend::parser::SqlTypeKind::Int4Range),
        )
        .unwrap(),
        vec![
            vec![Value::Text("empty".into())],
            vec![Value::Text("[1,3)".into())],
            vec![Value::Text("[1,3)".into())],
            vec![Value::Text("[5,7)".into())],
        ],
    );

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select span::text, count(*) from t group by t.span order by t.span",
            range_catalog("t", crate::backend::parser::SqlTypeKind::Int4Range),
        )
        .unwrap(),
        vec![
            vec![Value::Text("empty".into()), Value::Int64(1)],
            vec![Value::Text("[1,3)".into()), Value::Int64(2)],
            vec![Value::Text("[5,7)".into()), Value::Int64(1)],
        ],
    );

    assert_query_rows(
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select a.id, b.id from t a join t b on a.span = b.span where a.id < b.id order by a.id, b.id",
            range_catalog("t", crate::backend::parser::SqlTypeKind::Int4Range),
        )
        .unwrap(),
        vec![vec![Value::Int32(3), Value::Int32(4)]],
    );
}

#[test]
fn range_union_and_difference_errors_match_postgres() {
    let base = temp_dir("range_operator_errors");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let union_err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select int4range(1, 5) + int4range(7, 10)",
    )
    .unwrap_err();
    assert_eq!(
        format_exec_error(&union_err),
        "result of range union would not be contiguous"
    );

    let diff_err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select int4range(1, 10) - int4range(5, 7)",
    )
    .unwrap_err();
    assert_eq!(
        format_exec_error(&diff_err),
        "result of range difference would not be contiguous"
    );
}

#[test]
fn integer_shift_operators_preserve_left_type() {
    let base = temp_dir("integer_shift_operators");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select (-1::int2<<15)::text, ((-1::int2<<15)+1::int2)::text, (8::int4>>2)::text, (8::int8>>2)::text",
            )
            .unwrap(),
            vec![vec![
                Value::Text("-32768".into()),
                Value::Text("-32767".into()),
                Value::Text("2".into()),
                Value::Text("2".into()),
            ]],
        );
}

#[test]
fn top_level_values_orders_limits_and_names_columns() {
    let base = temp_dir("top_level_values");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "values (2, 'b'), (1, 'a') order by 1 limit 1",
    )
    .unwrap()
    {
        StatementResult::Query {
            column_names, rows, ..
        } => {
            assert_eq!(
                column_names,
                vec!["column1".to_string(), "column2".to_string()]
            );
            assert_eq!(rows, vec![vec![Value::Int32(1), Value::Text("a".into())]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn select_ctes_bind_values_and_shadow_catalog_tables() {
    let base = temp_dir("select_ctes_bind_values");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "with people(id) as (values (42)), q as (select id from people) select (select id from q)",
            )
            .unwrap(),
            vec![vec![Value::Int32(42)]],
        );
}

#[test]
fn select_cte_can_capture_outer_value_through_scalar_subquery() {
    let base = temp_dir("select_cte_outer_value_scalar_subquery");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select (
                with cte(foo) as (values (x))
                select (select foo from cte)
             )
             from (values (0), (123456), (-123456)) as t(x)",
        )
        .unwrap(),
        vec![
            vec![Value::Int32(0)],
            vec![Value::Int32(123456)],
            vec![Value::Int32(-123456)],
        ],
    );
}

#[test]
fn aggregate_subquery_can_reference_outer_visible_cte() {
    let base = temp_dir("aggregate_subquery_outer_cte");
    let txns = TransactionManager::new_durable(&base).unwrap();

    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "with a(id) as (values (1), (2)),
                  b as (select max((select sum(id) from a)) as agg)
             select agg from b",
        )
        .unwrap(),
        vec![vec![Value::Int64(3)]],
    );
}

#[test]
fn insert_values_can_reference_statement_ctes() {
    let mut harness = SeededSqlHarness::new("insert_values_ctes", catalog());
    let xid = harness.txns.begin();
    harness
        .execute(
            xid,
            "with q(v) as (values (7)) insert into people (id, name, note) values ((select v from q), 'alice', 'a')",
        )
        .unwrap();
    harness.txns.commit(xid).unwrap();

    assert_query_rows(
        harness
            .execute(INVALID_TRANSACTION_ID, "select id, name from people")
            .unwrap(),
        vec![vec![Value::Int32(7), Value::Text("alice".into())]],
    );
}

#[test]
fn update_can_reference_statement_ctes() {
    let mut harness = SeededSqlHarness::new("update_ctes", catalog());
    let xid = harness.txns.begin();
    harness
        .execute(
            xid,
            "insert into people (id, name, note) values (1, 'alice', 'old')",
        )
        .unwrap();
    harness.txns.commit(xid).unwrap();

    let xid = harness.txns.begin();
    harness
        .execute(
            xid,
            "with q(v) as (values ('new')) update people set note = (select v from q) where id = 1",
        )
        .unwrap();
    harness.txns.commit(xid).unwrap();

    assert_query_rows(
        harness
            .execute(INVALID_TRANSACTION_ID, "select note from people")
            .unwrap(),
        vec![vec![Value::Text("new".into())]],
    );
}

#[test]
fn delete_can_reference_statement_ctes() {
    let mut harness = SeededSqlHarness::new("delete_ctes", catalog());
    let xid = harness.txns.begin();
    harness
        .execute(
            xid,
            "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'b')",
        )
        .unwrap();
    harness.txns.commit(xid).unwrap();

    let xid = harness.txns.begin();
    harness
        .execute(
            xid,
            "with q(v) as (values (2)) delete from people where id in (select v from q)",
        )
        .unwrap();
    harness.txns.commit(xid).unwrap();

    assert_query_rows(
        harness
            .execute(INVALID_TRANSACTION_ID, "select id from people")
            .unwrap(),
        vec![vec![Value::Int32(1)]],
    );
}

#[test]
fn scalar_subquery_zero_rows_yields_null() {
    let mut harness = seed_people_and_pets("scalar_subquery_zero_rows");
    assert_query_rows(
            harness
                .execute(
                    INVALID_TRANSACTION_ID,
                    "select p.name, (select q.name from pets q where q.owner_id = p.id and q.id = 999) from people p order by p.id",
                )
                .unwrap(),
            vec![
                vec![Value::Text("alice".into()), Value::Null],
                vec![Value::Text("bob".into()), Value::Null],
                vec![Value::Text("carol".into()), Value::Null],
            ],
        );
}

#[test]
fn scalar_subquery_multiple_rows_errors() {
    let mut harness = seed_people_and_pets("scalar_subquery_multiple_rows");
    let err = harness
        .execute(
            INVALID_TRANSACTION_ID,
            "select (select q.name from pets q where q.owner_id = p.id) from people p where p.id = 1",
        )
        .unwrap_err();
    assert!(
        format!("{err:?}")
            .contains("more than one row returned by a subquery used as an expression")
    );
}

#[test]
fn exists_and_not_exists_are_correlated_per_row() {
    let mut harness = seed_people_and_pets("exists_correlated_per_row");
    assert_query_rows(
            harness
                .execute(
                    INVALID_TRANSACTION_ID,
                    "select p.id from people p where exists (select 1 from pets q where q.owner_id = p.id) order by p.id",
                )
                .unwrap(),
            vec![vec![Value::Int32(1)], vec![Value::Int32(2)]],
        );
    assert_query_rows(
            harness
                .execute(
                    INVALID_TRANSACTION_ID,
                    "select p.id from people p where not exists (select 1 from pets q where q.owner_id = p.id) order by p.id",
                )
                .unwrap(),
            vec![vec![Value::Int32(3)]],
        );
}

#[test]
fn in_subquery_truth_table_cases() {
    let base = temp_dir("in_subquery_truth_table");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select 1 in (select 1), 1 in (select 2), 1 in (select 1 where false)",
        )
        .unwrap(),
        vec![vec![
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(false),
        ]],
    );
}

#[test]
fn not_in_subquery_truth_table_cases() {
    let base = temp_dir("not_in_subquery_truth_table");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select 1 not in (select 1), 1 not in (select 2), 1 not in (select 1 where false)",
        )
        .unwrap(),
        vec![vec![
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(true),
        ]],
    );
}

#[test]
fn in_and_not_in_propagate_nulls_like_postgres() {
    let base = temp_dir("in_not_in_nulls");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select 1 in (select null), 1 not in (select null), null in (select 1), null not in (select 1)",
            )
            .unwrap(),
            vec![vec![Value::Null, Value::Null, Value::Null, Value::Null]],
        );
}

#[test]
fn any_and_all_subquery_match_postgres_empty_set_semantics() {
    let base = temp_dir("any_all_empty_set");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select 1 = any (select 1 where false), 1 < all (select 1 where false)",
        )
        .unwrap(),
        vec![vec![Value::Bool(false), Value::Bool(true)]],
    );
}

#[test]
fn any_and_all_subquery_propagate_nulls() {
    let base = temp_dir("any_all_nulls");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select 1 = any (select null), 1 < all (select null)",
        )
        .unwrap(),
        vec![vec![Value::Null, Value::Null]],
    );
}

#[test]
fn correlated_any_subquery_filters_rows() {
    let mut harness = seed_people_and_pets("correlated_any_subquery");
    assert_query_rows(
            harness
                .execute(
                    INVALID_TRANSACTION_ID,
                    "select p.id from people p where p.id = any (select q.owner_id from pets q where q.owner_id is not null) order by p.id",
                )
                .unwrap(),
            vec![vec![Value::Int32(1)], vec![Value::Int32(2)]],
        );
}

#[test]
fn grouped_query_having_can_use_correlated_exists() {
    let mut harness = seed_people_and_pets("grouped_having_correlated_exists");
    assert_query_rows(
            harness
                .execute(
                    INVALID_TRANSACTION_ID,
                    "select p.id, count(*) from people p group by p.id having exists (select 1 from pets q where q.owner_id = p.id) order by p.id",
                )
                .unwrap(),
            vec![
                vec![Value::Int32(1), Value::Int64(1)],
                vec![Value::Int32(2), Value::Int64(1)],
            ],
        );
}

#[test]
fn grouped_query_having_can_use_outer_aggregate_inside_subquery_where() {
    let mut harness = seed_people_and_pets("grouped_having_outer_aggregate_in_subquery_where");
    assert_query_rows(
        harness
            .execute(
                INVALID_TRANSACTION_ID,
                "select p.id from people p group by p.id having exists (select 1 from pets q where sum(p.id) = q.owner_id) order by p.id",
            )
            .unwrap(),
        vec![vec![Value::Int32(1)], vec![Value::Int32(2)]],
    );
}

#[test]
fn grouped_query_having_matches_outer_aggregate_when_subquery_qualifies_column() {
    let mut harness = seed_people_and_pets("grouped_having_outer_aggregate_qualified_match");
    assert_query_rows(
        harness
            .execute(
                INVALID_TRANSACTION_ID,
                "select p.note, sum(id) from people p group by p.note having exists (select 1 from pets q where sum(p.id) = q.owner_id) order by p.note",
            )
            .unwrap(),
        vec![
            vec![Value::Text("a".into()), Value::Int64(1)],
            vec![Value::Text("b".into()), Value::Int64(2)],
        ],
    );
}

#[test]
fn grouped_query_having_can_use_outer_aggregate_with_ungrouped_arg_inside_subquery_where() {
    let mut harness =
        seed_people_and_pets("grouped_having_outer_aggregate_with_ungrouped_arg_inside_subquery");
    assert_query_rows(
        harness
            .execute(
                INVALID_TRANSACTION_ID,
                "select p.note from people p group by p.note having exists (select 1 from pets q where sum(distinct p.id) = q.owner_id) order by p.note",
            )
            .unwrap(),
        vec![
            vec![Value::Text("a".into())],
            vec![Value::Text("b".into())],
        ],
    );
}

#[test]
fn degenerate_having_does_not_scan_where_clause() {
    let base = temp_dir("degenerate_having_no_scan");
    let mut txns = TransactionManager::new_durable(&base).unwrap();
    let pool = test_pool(&base);
    let xid = txns.begin();
    for row in [tuple(0, "zero", Some("z")), tuple(1, "one", Some("o"))] {
        let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &row).unwrap();
        heap_flush(&*pool, 1, rel(), tid.block_number).unwrap();
    }
    txns.commit(xid).unwrap();

    match run_sql_with_catalog(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select 1 as one from people where 1/id = 1 having 1 < 2",
        catalog(),
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn nested_outer_correlation_uses_the_correct_row() {
    let mut harness = seed_people_and_pets("nested_outer_correlation");
    assert_query_rows(
            harness
                .execute(
                    INVALID_TRANSACTION_ID,
                    "select p.id from people p where exists (select 1 from pets q where q.owner_id = p.id and exists (select 1 from people r where r.id = p.id and r.name = p.name)) order by p.id",
                )
                .unwrap(),
            vec![vec![Value::Int32(1)], vec![Value::Int32(2)]],
        );
}

#[test]
fn scalar_subquery_can_be_used_in_order_by() {
    let mut harness = seed_people_and_pets("scalar_subquery_order_by");
    assert_query_rows(
            harness
                .execute(
                    INVALID_TRANSACTION_ID,
                    "select p.name from people p order by (select count(*) from pets q where q.owner_id = p.id) desc, p.id",
                )
                .unwrap(),
            vec![
                vec![Value::Text("alice".into())],
                vec![Value::Text("bob".into())],
                vec![Value::Text("carol".into())],
            ],
        );
}

#[test]
fn numeric_typmod_accepts_zero_in_full_scale_columns() {
    let base = temp_dir("numeric_typmod_zero_full_scale");
    let txns = TransactionManager::new_durable(&base).unwrap();
    // numeric(4,4) has 0 digits before the decimal — zero must be accepted
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '0.0'::numeric(4,4), '0.1234'::numeric(4,4)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Numeric("0.0000".into()),
                    Value::Numeric("0.1234".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
    // Values >= 1.0 should still overflow
    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select '1.0'::numeric(4,4)",
    )
    .unwrap_err();
    match err {
        ExecError::DetailedError {
            message,
            detail,
            sqlstate,
            ..
        } => {
            assert_eq!(message, "numeric field overflow");
            assert_eq!(
                detail.as_deref(),
                Some(
                    "A field with precision 4, scale 4 must round to an absolute value less than 1."
                )
            );
            assert_eq!(sqlstate, "22003");
        }
        other => panic!("expected detailed numeric typmod error, got {other:?}"),
    }
}

#[test]
fn gcd_and_lcm_support_numeric_arguments() {
    let base = temp_dir("gcd_lcm_numeric");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select gcd(12.0, 8.0), lcm(12.0, 8.0), gcd(0.0, 5.0)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Numeric("4".into()),
                    Value::Numeric("24".into()),
                    Value::Numeric("5".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn generate_series_supports_numeric_arguments() {
    let base = temp_dir("generate_series_numeric");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from generate_series(0.0::numeric, 4.0::numeric)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Numeric("0.0".into())],
                    vec![Value::Numeric("1.0".into())],
                    vec![Value::Numeric("2.0".into())],
                    vec![Value::Numeric("3.0".into())],
                    vec![Value::Numeric("4.0".into())],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
    // With explicit step
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from generate_series(0.0::numeric, 1.0::numeric, 0.3::numeric)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Numeric("0.0".into())],
                    vec![Value::Numeric("0.3".into())],
                    vec![Value::Numeric("0.6".into())],
                    vec![Value::Numeric("0.9".into())],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from generate_series(0, 1, 0.3)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![
                    vec![Value::Numeric("0".into())],
                    vec![Value::Numeric("0.3".into())],
                    vec![Value::Numeric("0.6".into())],
                    vec![Value::Numeric("0.9".into())],
                ]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn generate_series_preserves_numeric_display_scale_and_descending_rows() {
    let base = temp_dir("generate_series_numeric_display_scale");
    let txns = TransactionManager::new_durable(&base).unwrap();
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from generate_series(0.1::numeric, 4.0::numeric, 1.3::numeric)",
        )
        .unwrap(),
        vec![
            vec![Value::Numeric("0.1".into())],
            vec![Value::Numeric("1.4".into())],
            vec![Value::Numeric("2.7".into())],
            vec![Value::Numeric("4.0".into())],
        ],
    );
    assert_query_rows(
        run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select * from generate_series(4.0::numeric, -1.5::numeric, -2.2::numeric)",
        )
        .unwrap(),
        vec![
            vec![Value::Numeric("4.0".into())],
            vec![Value::Numeric("1.8".into())],
            vec![Value::Numeric("-0.4".into())],
        ],
    );
}

#[test]
fn recursive_query_with_numeric_generate_series_step_executes() {
    let base = temp_dir("recursive_generate_series_numeric_step");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let sql = r#"
with recursive ps as (
  select r, c from generate_series(0, 1, 0.5) a(r)
  cross join generate_series(0, 1, 0.5) b(c)
  order by r desc, c asc
), iterations as (
  select r,
         c,
         0.0::float as zr,
         0.0::float as zc,
         0 as iteration
  from ps
  union all
  select r,
         c,
         zr*zr - zc*zc + c as zr,
         2*zr*zc + r as zc,
         iteration + 1 as iteration
  from iterations
  where zr*zr + zc*zc < 4 and iteration < 4
), final_iteration as (
  select * from iterations where iteration = 4
), marked_points as (
  select r,
         c,
         (case when exists (select 1 from final_iteration i where p.r = i.r and p.c = i.c)
               then '**'
               else '  '
          end) as marker
  from ps p
  order by r desc, c asc
), lines as (
  select r, string_agg(marker, '') as r_text
  from marked_points
  group by r
  order by r desc
)
select string_agg(r_text, E'\n') from lines
"#;
    match run_sql(&base, &txns, INVALID_TRANSACTION_ID, sql).unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 1);
            match &rows[0][0] {
                Value::Text(text) => {
                    assert!(text.contains("**"));
                    assert!(text.contains('\n'));
                }
                other => panic!("expected text result, got {:?}", other),
            }
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn recursive_lsystem_segments_query_executes() {
    let base = temp_dir("recursive_lsystem_segments");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let sql = r#"
with recursive iterations as (
  select 'FX' as path, 0 as iteration
  union all
  select replace(replace(replace(path, 'X', 'X+ZF+'), 'Y', '-FX-Y'), 'Z', 'Y'), iteration + 1
  from iterations
  where iteration < 3
), segments as (
  select 0 as start_row,
         0 as start_col,
         0 as mid_row,
         0 as mid_col,
         0 as end_row,
         0 as end_col,
         0 as row_diff,
         1 as col_diff,
         (select path from iterations order by iteration desc limit 1) as path_left
  union all
  select end_row,
         end_col,
         end_row + row_diff * step_size,
         end_col + col_diff * step_size,
         end_row + 2 * row_diff * step_size,
         end_col + 2 * col_diff * step_size,
         case when substring(path_left for 1) = '-' then -col_diff
              when substring(path_left for 1) = '+' then col_diff
              else row_diff
         end,
         case when substring(path_left for 1) = '-' then row_diff
              when substring(path_left for 1) = '+' then -row_diff
              else col_diff
         end,
         substring(path_left from 2)
  from segments,
       lateral (
         select case when substring(path_left for 1) = 'F' then 1 else 0 end as step_size
       ) sub
  where char_length(path_left) > 0
)
select count(*) from segments
"#;
    assert_query_rows(
        run_sql(&base, &txns, INVALID_TRANSACTION_ID, sql).unwrap(),
        vec![vec![Value::Int64(31)]],
    );
}

#[test]
fn recursive_lsystem_points_query_executes() {
    let base = temp_dir("recursive_lsystem_points");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let sql = r#"
with recursive iterations as (
  select 'FX' as path, 0 as iteration
  union all
  select replace(replace(replace(path, 'X', 'X+ZF+'), 'Y', '-FX-Y'), 'Z', 'Y'), iteration + 1
  from iterations where iteration < 3
), segments as (
  select
    0 as start_row,
    0 as start_col,
    0 as mid_row,
    0 as mid_col,
    0 as end_row,
    0 as end_col,
    0 as row_diff,
    1 as col_diff,
    (select path from iterations order by iteration desc limit 1) as path_left
  union all
  select
    end_row as start_row,
    end_col as start_col,
    end_row + row_diff * step_size as mid_row,
    end_col + col_diff * step_size as mid_col,
    end_row + 2 * row_diff * step_size as end_row,
    end_col + 2 * col_diff * step_size as end_col,
    case when substring(path_left for 1) = '-' then -col_diff
         when substring(path_left for 1) = '+' then col_diff
         else row_diff
    end as row_diff,
    case when substring(path_left for 1) = '-' then row_diff
         when substring(path_left for 1) = '+' then -row_diff
         else col_diff
    end as col_diff,
    substring(path_left from 2) as path_left
  from segments,
       lateral (select case when substring(path_left for 1) = 'F' then 1 else 0 end as step_size) sub
  where char_length(path_left) > 0
), end_points as (
  select start_row as r, start_col as c from segments
  union
  select end_row as r, end_col as c from segments
), points as (
  select r, c from generate_series((select min(r) from end_points), (select max(r) from end_points)) a(r)
  cross join generate_series((select min(c) from end_points), (select max(c) from end_points)) b(c)
), marked_points as (
  select r, c, (case when
    exists (select 1 from end_points e where p.r = e.r and p.c = e.c)
    then '*'

    when exists (select 1 from segments s where p.r = s.mid_row and p.c = s.mid_col and col_diff != 0)
    then '-'

    when exists (select 1 from segments s where p.r = s.mid_row and p.c = s.mid_col and row_diff != 0)
    then '|'

    else ' '
    end
    ) as marker
  from points p
), lines as (
   select r, string_agg(marker, '') as row_text
   from marked_points
   group by r
   order by r desc
) select string_agg(row_text, E'\n') from lines
"#;
    assert_query_rows(
        run_sql(&base, &txns, INVALID_TRANSACTION_ID, sql).unwrap(),
        vec![vec![Value::Text(
            "* *-*  \n| | |  \n*-* *-*\n      |\n    *-*".into(),
        )]],
    );
}

#[test]
fn explain_recursive_exists_query_uses_cte_scan() {
    let base = temp_dir("explain_recursive_exists_cte_scan");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let sql = r#"
explain
with recursive t(n) as (
  values (1)
  union all
  select n + 1 from t where n < 3
), final_t as (
  select * from t where n = 3
)
select n
from t s
where exists (select 1 from final_t f where f.n = s.n)
order by n
"#;
    match run_sql(&base, &txns, INVALID_TRANSACTION_ID, sql).unwrap() {
        StatementResult::Query { rows, .. } => {
            let rendered = rows
                .into_iter()
                .map(|row| match &row[0] {
                    Value::Text(text) => text.clone(),
                    other => panic!("expected text explain line, got {:?}", other),
                })
                .collect::<Vec<_>>();
            assert!(
                rendered.iter().any(|line| line.contains("CTE Scan")),
                "expected CTE Scan in explain output, got {rendered:?}"
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn generate_series_rejects_non_finite_numeric_bounds() {
    let base = temp_dir("generate_series_numeric_non_finite");
    let txns = TransactionManager::new_durable(&base).unwrap();

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from generate_series(-100::numeric, 100::numeric, 'nan'::numeric)",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::GenerateSeriesInvalidArg("step size", "NaN")
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from generate_series('nan'::numeric, 100::numeric, 10::numeric)",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::GenerateSeriesInvalidArg("start", "NaN")
    ));

    let err = run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select * from generate_series(0::numeric, 'nan'::numeric, 10::numeric)",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ExecError::GenerateSeriesInvalidArg("stop", "NaN")
    ));
}

#[test]
fn mod_function_works_for_numeric_values() {
    let base = temp_dir("mod_function_numeric");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select mod(10.0, 3.0), mod(12.5, 4.0)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Numeric("1.0".into()),
                    Value::Numeric("0.5".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn numeric_division_works_with_large_scale_operands() {
    let base = temp_dir("numeric_div_large_scale");
    let txns = TransactionManager::new_durable(&base).unwrap();
    // Division where lscale > out_scale + rscale should not error
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select (1.0 / 3.0) / 7.0",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            // Should produce a numeric result, not a TypeMismatch error
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 1);
            match &rows[0][0] {
                Value::Numeric(_) => {}
                other => panic!("expected numeric, got {:?}", other),
            }
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn trunc_and_round_preserve_requested_scale() {
    let base = temp_dir("trunc_round_scale");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select trunc(1.0, 1), trunc(1.999, 2), round(1.5, 0), round(1.0, 3)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Numeric("1.0".into()),
                    Value::Numeric("1.99".into()),
                    Value::Numeric("2".into()),
                    Value::Numeric("1.000".into()),
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn trunc_and_round_large_negative_scale_short_circuit_to_zero() {
    let base = temp_dir("trunc_round_large_negative_scale");
    let txns = TransactionManager::new_durable(&base).unwrap();
    match run_sql(
        &base,
        &txns,
        INVALID_TRANSACTION_ID,
        "select trunc(9.9e131071, -1000000), round(5.5e131071, -1000000)",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![Value::Numeric("0".into()), Value::Numeric("0".into())]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn large_object_metadata_tracks_create_and_unlink() {
    let base = temp_dir("large_object_metadata_tracks_create_and_unlink");
    let txns = TransactionManager::new_durable(&base).unwrap();
    let large_objects =
        std::sync::Arc::new(crate::pgrust::database::LargeObjectRuntime::new_ephemeral());
    let run_large_object_sql = |sql: &str| -> Result<StatementResult, ExecError> {
        let mut catalog = catalog();
        crate::backend::catalog::store::sync_catalog_heaps_for_tests(&base, &catalog).unwrap();
        let smgr = MdStorageManager::new(&base);
        let pool = std::sync::Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 8));
        for name in catalog.table_names().collect::<Vec<_>>() {
            if let Some(entry) = catalog.get(&name) {
                create_fork(&*pool, entry.rel);
            }
        }
        let txns_arc = std::sync::Arc::new(parking_lot::RwLock::new(txns.clone()));
        let mut ctx = ExecutorContext {
            pool,
            txns: txns_arc,
            txn_waiter: None,
            sequences: Some(std::sync::Arc::new(
                crate::pgrust::database::SequenceRuntime::new_ephemeral(),
            )),
            large_objects: Some(large_objects.clone()),
            async_notify_runtime: None,
            advisory_locks: std::sync::Arc::new(
                crate::backend::storage::lmgr::AdvisoryLockManager::new(),
            ),
            row_locks: std::sync::Arc::new(crate::backend::storage::lmgr::RowLockManager::new()),
            checkpoint_stats:
                crate::backend::utils::misc::checkpoint::CheckpointStatsSnapshot::default(),
            datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
            interrupts: std::sync::Arc::new(
                crate::backend::utils::misc::interrupts::InterruptState::new(),
            ),
            stats: std::sync::Arc::new(parking_lot::RwLock::new(
                crate::pgrust::database::DatabaseStatsStore::with_default_io_rows(),
            )),
            session_stats: std::sync::Arc::new(parking_lot::RwLock::new(
                crate::pgrust::database::SessionStatsState::default(),
            )),
            snapshot: txns.snapshot(INVALID_TRANSACTION_ID).unwrap(),
            transaction_state: None,
            client_id: 77,
            current_database_name: "postgres".to_string(),
            session_user_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
            current_user_oid: crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,
            active_role_oid: None,
            session_replication_role: Default::default(),
            statement_lock_scope_id: None,
            transaction_lock_scope_id: None,
            next_command_id: 0,
            default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
            expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
            case_test_values: Vec::new(),
            system_bindings: Vec::new(),
            subplans: Vec::new(),
            timed: false,
            allow_side_effects: true,
            pending_async_notifications: Vec::new(),
            catalog: catalog.materialize_visible_catalog(),
            compiled_functions: std::collections::HashMap::new(),
            cte_tables: std::collections::HashMap::new(),
            cte_producers: std::collections::HashMap::new(),
            recursive_worktables: std::collections::HashMap::new(),
            deferred_foreign_keys: None,
            trigger_depth: 0,
        };
        execute_sql(sql, &mut catalog, &mut ctx, INVALID_TRANSACTION_ID)
    };

    match run_large_object_sql("select lo_create(1001)").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int64(1001)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match run_large_object_sql(
        "select oid, lomowner, lomacl from pg_largeobject_metadata order by oid",
    )
    .unwrap()
    {
        StatementResult::Query { rows, .. } => {
            assert_eq!(
                rows,
                vec![vec![
                    Value::Int64(1001),
                    Value::Int64(i64::from(crate::include::catalog::BOOTSTRAP_SUPERUSER_OID,)),
                    Value::Null,
                ]]
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match run_large_object_sql("select lo_unlink(1001)").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert_eq!(rows, vec![vec![Value::Int32(1)]]);
        }
        other => panic!("expected query result, got {:?}", other),
    }

    match run_large_object_sql("select oid from pg_largeobject_metadata").unwrap() {
        StatementResult::Query { rows, .. } => {
            assert!(rows.is_empty());
        }
        other => panic!("expected query result, got {:?}", other),
    }
}
