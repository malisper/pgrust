    use super::*;
    use crate::RelFileLocator;
    use crate::backend::access::heap::heapam::{heap_flush, heap_insert_mvcc, heap_update};
    use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
    use crate::backend::parser::{Catalog, CatalogEntry};
    use crate::backend::storage::smgr::{ForkNumber, MdStorageManager, StorageManager};
    use crate::include::access::htup::TupleValue;
    use crate::include::access::htup::{AttributeDesc, HeapTuple};
    use std::fs;
    use std::path::PathBuf;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

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
            relation_oid: 50_000u32.saturating_add(rel.rel_number),
            namespace_oid: 11,
            row_type_oid: 60_000u32.saturating_add(rel.rel_number),
            relkind: 'r',
            rel,
            desc,
            index_meta: None,
        }
    }

    fn catalog() -> Catalog {
        let mut catalog = Catalog::default();
        catalog.insert("people", test_catalog_entry(rel(), relation_desc()));
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

    fn catalog_with_pets() -> Catalog {
        let mut catalog = catalog();
        catalog.insert("pets", test_catalog_entry(pets_rel(), pets_relation_desc()));
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
                        crate::backend::parser::SqlType::new(
                            crate::backend::parser::SqlTypeKind::Oid,
                        ),
                        false,
                    )],
                },
            ),
        );
        catalog
    }

    fn shipments_rel() -> RelFileLocator {
        RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: 15003,
        }
    }

    fn shipments_relation_desc() -> RelationDesc {
        use crate::backend::parser::{SqlType, SqlTypeKind};

        RelationDesc {
            columns: vec![
                crate::backend::catalog::catalog::column_desc(
                    "id",
                    SqlType::new(SqlTypeKind::Int4),
                    false,
                ),
                crate::backend::catalog::catalog::column_desc(
                    "company_id",
                    SqlType::new(SqlTypeKind::Text),
                    false,
                ),
                crate::backend::catalog::catalog::column_desc(
                    "year",
                    SqlType::new(SqlTypeKind::Text),
                    false,
                ),
                crate::backend::catalog::catalog::column_desc(
                    "container_numbers",
                    SqlType::array_of(SqlType::new(SqlTypeKind::Varchar)),
                    true,
                ),
                crate::backend::catalog::catalog::column_desc(
                    "container_types_categories",
                    SqlType::array_of(SqlType::new(SqlTypeKind::Varchar)),
                    true,
                ),
                crate::backend::catalog::catalog::column_desc(
                    "container_size_categories",
                    SqlType::array_of(SqlType::new(SqlTypeKind::Varchar)),
                    true,
                ),
            ],
        }
    }

    fn shipments_catalog() -> Catalog {
        let mut catalog = Catalog::default();
        catalog.insert(
            "om_shipments",
            test_catalog_entry(shipments_rel(), shipments_relation_desc()),
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
            snapshot,
            client_id: 1,
            next_command_id: 0,
            outer_rows: Vec::new(),
            timed: false,
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
            snapshot: txns.snapshot(INVALID_TRANSACTION_ID).unwrap(),
            client_id: 42,
            next_command_id: 0,
            outer_rows: Vec::new(),
            timed: false,
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
        crate::backend::catalog::store::sync_catalog_heaps_for_tests(base, &catalog)
            .unwrap();
        let smgr = MdStorageManager::new(base);
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
            snapshot: txns.snapshot(xid).unwrap(),
            client_id: 77,
            next_command_id: 0,
            outer_rows: Vec::new(),
            timed: false,
        };
        execute_sql(sql, &mut catalog, &mut ctx, xid)
    }

    fn assert_query_rows(result: StatementResult, expected: Vec<Vec<Value>>) {
        match result {
            StatementResult::Query { rows, .. } => assert_eq!(rows, expected),
            other => panic!("expected query result, got {:?}", other),
        }
    }

    fn seed_people_and_pets(base: &PathBuf, txns: &mut TransactionManager) {
        let xid = txns.begin();
        run_sql_with_catalog(
            base,
            txns,
            xid,
            "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'b'), (3, 'carol', null)",
            catalog_with_pets(),
        )
        .unwrap();
        run_sql_with_catalog(
            base,
            txns,
            xid,
            "insert into pets (id, name, owner_id) values (10, 'mocha', 1), (11, 'pixel', 1), (12, 'otis', 2), (13, 'stray', null)",
            catalog_with_pets(),
        )
        .unwrap();
        txns.commit(xid).unwrap();
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
                &Expr::Eq(
                    Box::new(Expr::Column(0)),
                    Box::new(Expr::Const(Value::Int32(7)))
                ),
                &mut slot,
                &mut ctx
            )
            .unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_expr(
                &Expr::Eq(
                    Box::new(Expr::Column(2)),
                    Box::new(Expr::Const(Value::Text("x".into())))
                ),
                &mut slot,
                &mut ctx
            )
            .unwrap(),
            Value::Null
        );
        assert_eq!(
            eval_expr(
                &Expr::And(
                    Box::new(Expr::Const(Value::Bool(true))),
                    Box::new(Expr::Const(Value::Null))
                ),
                &mut slot,
                &mut ctx
            )
            .unwrap(),
            Value::Null
        );
        assert_eq!(
            eval_expr(
                &Expr::IsNull(Box::new(Expr::Column(2))),
                &mut slot,
                &mut ctx
            )
            .unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_expr(
                &Expr::IsNotNull(Box::new(Expr::Column(2))),
                &mut slot,
                &mut ctx
            )
            .unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            eval_expr(
                &Expr::IsDistinctFrom(
                    Box::new(Expr::Column(2)),
                    Box::new(Expr::Const(Value::Null))
                ),
                &mut slot,
                &mut ctx
            )
            .unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            eval_expr(
                &Expr::IsDistinctFrom(
                    Box::new(Expr::Column(1)),
                    Box::new(Expr::Const(Value::Null))
                ),
                &mut slot,
                &mut ctx
            )
            .unwrap(),
            Value::Bool(true)
        );
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
            input: Box::new(Plan::Filter {
                input: Box::new(Plan::SeqScan {
                    rel: rel(),
                    desc: relation_desc(),
                }),
                predicate: Expr::Gt(
                    Box::new(Expr::Column(0)),
                    Box::new(Expr::Const(Value::Int32(1))),
                ),
            }),
            targets: vec![
                TargetEntry {
                    name: "name".into(),
                    expr: Expr::Column(1),
                    sql_type: crate::backend::parser::SqlType::new(
                        crate::backend::parser::SqlTypeKind::Text,
                    ),
                },
                TargetEntry {
                    name: "note_is_null".into(),
                    expr: Expr::IsNull(Box::new(Expr::Column(2))),
                    sql_type: crate::backend::parser::SqlType::new(
                        crate::backend::parser::SqlTypeKind::Bool,
                    ),
                },
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
            rel: rel(),
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
    fn insert_sql_inserts_row() {
        let base = temp_dir("insert_sql");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let xid = txns.begin();
        assert_eq!(
            run_sql(
                &base,
                &txns,
                xid,
                "insert into people (id, name, note) values (1, 'alice', 'alpha')"
            )
            .unwrap(),
            StatementResult::AffectedRows(1)
        );
        txns.commit(xid).unwrap();
        match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select name, note from people",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
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
                    crate::backend::parser::SqlType::new(
                        crate::backend::parser::SqlTypeKind::Varchar
                    )
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
        let base = temp_dir("insert_multi_sql");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let xid = txns.begin();
        assert_eq!(
            run_sql(
                &base,
                &txns,
                xid,
                "insert into people (id, name, note) values (1, 'alice', 'alpha'), (2, 'bob', null)"
            )
            .unwrap(),
            StatementResult::AffectedRows(2)
        );
        txns.commit(xid).unwrap();
        match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select id, name, note from people",
        )
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
    fn update_sql_updates_matching_rows() {
        let base = temp_dir("update_sql");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let insert_xid = txns.begin();
        run_sql(
            &base,
            &txns,
            insert_xid,
            "insert into people (id, name, note) values (1, 'alice', 'old')",
        )
        .unwrap();
        txns.commit(insert_xid).unwrap();
        let update_xid = txns.begin();
        assert_eq!(
            run_sql(
                &base,
                &txns,
                update_xid,
                "update people set note = 'new' where id = 1"
            )
            .unwrap(),
            StatementResult::AffectedRows(1)
        );
        txns.commit(update_xid).unwrap();
        match run_sql(
            &base,
            &txns,
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
        let base = temp_dir("delete_sql");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let insert_xid = txns.begin();
        run_sql(
            &base,
            &txns,
            insert_xid,
            "insert into people (id, name, note) values (1, 'alice', null)",
        )
        .unwrap();
        run_sql(
            &base,
            &txns,
            insert_xid,
            "insert into people (id, name, note) values (2, 'bob', 'keep')",
        )
        .unwrap();
        txns.commit(insert_xid).unwrap();
        let delete_xid = txns.begin();
        assert_eq!(
            run_sql(
                &base,
                &txns,
                delete_xid,
                "delete from people where note is null"
            )
            .unwrap(),
            StatementResult::AffectedRows(1)
        );
        txns.commit(delete_xid).unwrap();
        match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select name from people",
        )
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
        let base = temp_dir("order_by_limit_offset");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let insert_xid = txns.begin();
        run_sql(&base, &txns, insert_xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (3, 'carol', 'c'), (2, 'bob', null)").unwrap();
        txns.commit(insert_xid).unwrap();
        match run_sql(
            &base,
            &txns,
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
        let base = temp_dir("order_by_nulls");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let insert_xid = txns.begin();
        run_sql(&base, &txns, insert_xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'c')").unwrap();
        txns.commit(insert_xid).unwrap();
        match run_sql(
            &base,
            &txns,
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
        match run_sql(
            &base,
            &txns,
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
        let base = temp_dir("null_predicates");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let insert_xid = txns.begin();
        run_sql(&base, &txns, insert_xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'c')").unwrap();
        txns.commit(insert_xid).unwrap();
        match run_sql(
            &base,
            &txns,
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
        match run_sql(
            &base,
            &txns,
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
        match run_sql(
            &base,
            &txns,
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
        match run_sql(
            &base,
            &txns,
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
    fn show_tables_lists_catalog_tables() {
        let base = temp_dir("show_tables");
        let txns = TransactionManager::new_durable(&base).unwrap();
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "show tables").unwrap() {
            StatementResult::Query {
                column_names, rows, ..
            } => {
                assert_eq!(column_names, vec!["table_name".to_string()]);
                assert_eq!(rows, vec![vec![Value::Text("people".into())]]);
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
        let base = temp_dir("explain_analyze_sql");
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
                assert!(rendered.iter().any(|line| line.contains("actual rows=")));
                assert!(rendered.iter().any(|line| line.contains("Execution Time:")));
                assert!(rendered.iter().any(|line| line.contains("Buffers: shared")));
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
            "select people.name, pets.name from people, pets",
            catalog_with_pets(),
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![
                        vec![Value::Text("alice".into()), Value::Text("Kitchen".into())],
                        vec![Value::Text("alice".into()), Value::Text("Mocha".into())],
                        vec![Value::Text("bob".into()), Value::Text("Kitchen".into())],
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
            crate::backend::executor::expr_ops::compare_order_values(&small, &wrapped, None, false),
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
    fn pg_input_error_info_supports_oidvector_tokens() {
        let valid = expr_casts::soft_input_error_info(" 1 2  4 ", "oidvector").unwrap();
        assert!(valid.is_none());

        let invalid = expr_casts::soft_input_error_info("01 01XYZ", "oidvector")
            .unwrap()
            .expect("expected invalid oidvector input");
        assert_eq!(invalid.message, "invalid input syntax for type oid: \"XYZ\"");
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

        assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select attname from pg_attribute where attrelid = 1259 order by attnum",
            )
            .unwrap(),
            vec![
                vec![Value::Text("oid".into())],
                vec![Value::Text("relname".into())],
                vec![Value::Text("relnamespace".into())],
                vec![Value::Text("reltype".into())],
                vec![Value::Text("relfilenode".into())],
                vec![Value::Text("relkind".into())],
            ],
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
    fn shipment_shaped_array_query_runs() {
        run_with_large_stack("shipment_shaped_array_query_runs", || {
            let base = temp_dir("shipment_arrays");
            let mut txns = TransactionManager::new_durable(&base).unwrap();
            let xid = txns.begin();
            run_sql_with_catalog(&base, &txns, xid, "insert into om_shipments (id, company_id, year, container_numbers, container_types_categories, container_size_categories) values (1, 'acme', '2024', ARRAY['c1', 'c2']::varchar[], ARRAY['dry', 'dry']::varchar[], ARRAY['40_high_cube', '20_standard']::varchar[]), (2, 'acme', '2024', ARRAY['c3']::varchar[], ARRAY['dry']::varchar[], ARRAY['40_high_cube']::varchar[]), (3, 'beta', '2024', ARRAY['c4']::varchar[], ARRAY['dry']::varchar[], ARRAY['20_standard']::varchar[])", shipments_catalog()).unwrap();
            txns.commit(xid).unwrap();
            match run_sql_with_catalog(&base, &txns, INVALID_TRANSACTION_ID, "select om_shipments.company_id, count(distinct om_shipments.id) as shipments_filtered, sum((select count(*) from unnest(om_shipments.container_numbers, om_shipments.container_types_categories, om_shipments.container_size_categories) as c(num, type_cat, size_cat) where (c.size_cat)::text = any(ARRAY['40_high_cube']::varchar[]))) as containers_filtered from om_shipments where om_shipments.year = '2024' and om_shipments.container_size_categories && ARRAY['40_high_cube']::varchar[] group by om_shipments.company_id order by om_shipments.company_id", shipments_catalog()).unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("acme".into()), Value::Int64(2), Value::Int64(2)]]); } other => panic!("expected query result, got {:?}", other), }
        });
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

        let err = crate::backend::executor::value_io::encode_value(
            &column,
            &Value::Text("34.5".into()),
        )
        .unwrap_err();
        assert!(
            matches!(err, ExecError::InvalidIntegerInput { ty: "smallint", .. }),
            "got {err:?}"
        );

        let err = crate::backend::executor::value_io::encode_value(
            &column,
            &Value::Text("100000".into()),
        )
        .unwrap_err();
        assert!(
            matches!(
                err,
                ExecError::IntegerOutOfRange {
                    ty: "smallint",
                    ..
                }
            ),
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

        let err = run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select bool 'yeah'",
        )
        .unwrap_err();
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
            vec![vec![Value::Bool(false), Value::Bool(true), Value::Bool(true)]],
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
                Value::Text("invalid input syntax for type bytea: \"\\x123\"".into()),
                Value::Null,
                Value::Null,
                Value::Text("22P02".into()),
            ]],
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
        let base = temp_dir("bit_insert_defaults");
        let txns = TransactionManager::new_durable(&base).unwrap();
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

        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "insert into bit_defaults default values",
            catalog.clone(),
        )
        .unwrap();
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "insert into bit_defaults (b2) values (B'1')",
            catalog.clone(),
        )
        .unwrap();
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "insert into bit_defaults values (DEFAULT, B'11')",
            catalog.clone(),
        )
        .unwrap();
        run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "insert into bit_defaults select B'1111', B'1'",
            catalog.clone(),
        )
        .unwrap();
        assert_query_rows(
            run_sql_with_catalog(&base, &txns, INVALID_TRANSACTION_ID, "table bit_defaults", catalog)
                .unwrap(),
            vec![
                vec![
                    Value::Bit(crate::include::nodes::datum::BitString::new(4, vec![0b1001_0000])),
                    Value::Bit(crate::include::nodes::datum::BitString::new(4, vec![0b0101_0000])),
                ],
                vec![
                    Value::Bit(crate::include::nodes::datum::BitString::new(4, vec![0b1001_0000])),
                    Value::Bit(crate::include::nodes::datum::BitString::new(1, vec![0b1000_0000])),
                ],
                vec![
                    Value::Bit(crate::include::nodes::datum::BitString::new(4, vec![0b1001_0000])),
                    Value::Bit(crate::include::nodes::datum::BitString::new(2, vec![0b1100_0000])),
                ],
                vec![
                    Value::Bit(crate::include::nodes::datum::BitString::new(4, vec![0b1111_0000])),
                    Value::Bit(crate::include::nodes::datum::BitString::new(1, vec![0b1000_0000])),
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
            snapshot: txns.snapshot(INVALID_TRANSACTION_ID).unwrap(),
            client_id: 77,
            next_command_id: 0,
            outer_rows: Vec::new(),
            timed: false,
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
        let base = temp_dir("qualified_star_target");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        seed_people_and_pets(&base, &mut txns);

        assert_query_rows(
            run_sql_with_catalog(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select p.* from people p order by p.id",
                catalog(),
            )
            .unwrap(),
            vec![
                vec![Value::Int32(1), Value::Text("alice".into()), Value::Text("a".into())],
                vec![Value::Int32(2), Value::Text("bob".into()), Value::Text("b".into())],
                vec![Value::Int32(3), Value::Text("carol".into()), Value::Null],
            ],
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
        assert!(matches!(err, ExecError::NumericFieldOverflow));
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
                    crate::backend::parser::SqlType::new(
                        crate::backend::parser::SqlTypeKind::Numeric
                    )
                );
                assert_eq!(
                    columns[1].sql_type,
                    crate::backend::parser::SqlType::new(
                        crate::backend::parser::SqlTypeKind::Numeric
                    )
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
    fn sum_and_avg_numeric_preserve_numeric_results() {
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
                assert_eq!(columns[0].sql_type, crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Numeric));
                assert_eq!(columns[1].sql_type, crate::backend::parser::SqlType::new(crate::backend::parser::SqlTypeKind::Numeric));
                assert_eq!(rows, vec![vec![Value::Numeric("4".into()), Value::Numeric("2".into())]]);
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
                    crate::backend::parser::SqlType::new(
                        crate::backend::parser::SqlTypeKind::Float4
                    )
                );
                assert_eq!(
                    columns[1].sql_type,
                    crate::backend::parser::SqlType::new(
                        crate::backend::parser::SqlTypeKind::Float8
                    )
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
                    crate::backend::parser::SqlType::new(
                        crate::backend::parser::SqlTypeKind::Numeric
                    )
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
                    crate::backend::parser::SqlType::new(
                        crate::backend::parser::SqlTypeKind::Float8
                    )
                );
                assert_eq!(
                    columns[1].sql_type,
                    crate::backend::parser::SqlType::new(
                        crate::backend::parser::SqlTypeKind::Float8
                    )
                );
                assert_eq!(
                    columns[2].sql_type,
                    crate::backend::parser::SqlType::new(
                        crate::backend::parser::SqlTypeKind::Numeric
                    )
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
    fn numeric_scalar_helpers_follow_postgres_basics() {
        let base = temp_dir("numeric_scalar_helpers");
        let txns = TransactionManager::new_durable(&base).unwrap();
        match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select round(42.4382::numeric, 2), trunc(42.4382::numeric, 2), div(4.2::numeric, 1::numeric), scale(0.00::numeric), min_scale(1.1000::numeric), trim_scale(1.120::numeric)",
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
                        Value::Int32(1),
                        Value::Numeric("1.12".into()),
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
                        Value::Numeric("1".into()),
                        Value::Numeric("1".into()),
                        Value::Numeric("2.0703893278913981".into()),
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
                assert_eq!(rows, vec![vec![Value::Array(vec![])]]);
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
    fn array_columns_round_trip_through_storage() {
        let base = temp_dir("array_storage_roundtrip");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let xid = txns.begin();
        assert_eq!(run_sql_with_catalog(&base, &txns, xid, "insert into om_shipments (id, company_id, year, container_numbers, container_types_categories, container_size_categories) values (1, 'acme', '2024', ARRAY['n1', null]::varchar[], ARRAY['dry']::varchar[], ARRAY['40_high_cube']::varchar[])", shipments_catalog()).unwrap(), StatementResult::AffectedRows(1));
        txns.commit(xid).unwrap();
        match run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select container_numbers from om_shipments",
            shipments_catalog(),
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![Value::Array(vec![
                        Value::Text("n1".into()),
                        Value::Null
                    ])]]
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
                assert_eq!(column_names, vec!["generate_series"]);
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
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select p.id, g.generate_series from (select id from people) p, generate_series(1, 2) g order by p.id, g.generate_series").unwrap() {
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
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select g.generate_series, h.generate_series from generate_series(1, 2) g, generate_series(5, 6) h order by g.generate_series, h.generate_series").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![
                    vec![Value::Int32(1), Value::Int32(5)],
                    vec![Value::Int32(1), Value::Int32(6)],
                    vec![Value::Int32(2), Value::Int32(5)],
                    vec![Value::Int32(2), Value::Int32(6)],
                ]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
    #[test]
    fn join_alias_hides_inner_relation_names() {
        let base = temp_dir("join_alias_hides_inner");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let pool = test_pool_with_pets(&base);
        let xid = txns.begin();
        let tid =
            heap_insert_mvcc(&*pool, 1, rel(), xid, &tuple(1, "alice", Some("alpha"))).unwrap();
        heap_flush(&*pool, 1, rel(), tid.block_number).unwrap();
        let tid =
            heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &pet_tuple(10, "Kitchen", 1)).unwrap();
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
            matches!(err, ExecError::Parse(ParseError::UnknownColumn(name)) if name == "p.name")
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
        let tid =
            heap_insert_mvcc(&*pool, 1, rel(), xid, &tuple(1, "alice", Some("alpha"))).unwrap();
        heap_flush(&*pool, 1, rel(), tid.block_number).unwrap();
        let tid =
            heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &pet_tuple(10, "Kitchen", 1)).unwrap();
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
                &Expr::RegexMatch(
                    Box::new(Expr::Column(0)),
                    Box::new(Expr::Const(Value::Text("foo".into())))
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
                &Expr::RegexMatch(
                    Box::new(Expr::Column(0)),
                    Box::new(Expr::Const(Value::Null))
                ),
                &mut slot,
                &mut ctx
            )
            .unwrap(),
            Value::Null
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
        assert!(matches!(err, ExecError::InvalidStorageValue { column, .. } if column == "json"));
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

        let err = run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'strict $['::jsonpath")
            .unwrap_err();
        assert!(matches!(err, ExecError::InvalidStorageValue { column, .. } if column == "jsonpath"));
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

        let err = run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select '\"\\u\"'::jsonpath")
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
                assert_eq!(rows, vec![vec![
                    Value::Text("ah".into()),
                    Value::Text("aho".into()),
                    Value::Text("ababab".into()),
                    Value::Text("".into()),
                ]]);
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
        let base = temp_dir("scalar_subquery_target_list");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        seed_people_and_pets(&base, &mut txns);
        assert_query_rows(
            run_sql_with_catalog(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select p.name, (select count(*) from pets q where q.owner_id = p.id) from people p order by p.id",
                catalog_with_pets(),
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
                assert_eq!(column_names, vec!["column1".to_string(), "column2".to_string()]);
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
    fn insert_values_can_reference_statement_ctes() {
        let base = temp_dir("insert_values_ctes");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let xid = txns.begin();
        run_sql(
            &base,
            &txns,
            xid,
            "with q(v) as (values (7)) insert into people (id, name, note) values ((select v from q), 'alice', 'a')",
        )
        .unwrap();
        txns.commit(xid).unwrap();

        assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select id, name from people",
            )
            .unwrap(),
            vec![vec![Value::Int32(7), Value::Text("alice".into())]],
        );
    }

    #[test]
    fn update_can_reference_statement_ctes() {
        let base = temp_dir("update_ctes");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let xid = txns.begin();
        run_sql(
            &base,
            &txns,
            xid,
            "insert into people (id, name, note) values (1, 'alice', 'old')",
        )
        .unwrap();
        txns.commit(xid).unwrap();

        let xid = txns.begin();
        run_sql(
            &base,
            &txns,
            xid,
            "with q(v) as (values ('new')) update people set note = (select v from q) where id = 1",
        )
        .unwrap();
        txns.commit(xid).unwrap();

        assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select note from people",
            )
            .unwrap(),
            vec![vec![Value::Text("new".into())]],
        );
    }

    #[test]
    fn delete_can_reference_statement_ctes() {
        let base = temp_dir("delete_ctes");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let xid = txns.begin();
        run_sql(
            &base,
            &txns,
            xid,
            "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'b')",
        )
        .unwrap();
        txns.commit(xid).unwrap();

        let xid = txns.begin();
        run_sql(
            &base,
            &txns,
            xid,
            "with q(v) as (values (2)) delete from people where id in (select v from q)",
        )
        .unwrap();
        txns.commit(xid).unwrap();

        assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select id from people",
            )
            .unwrap(),
            vec![vec![Value::Int32(1)]],
        );
    }

    #[test]
    fn scalar_subquery_zero_rows_yields_null() {
        let base = temp_dir("scalar_subquery_zero_rows");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        seed_people_and_pets(&base, &mut txns);
        assert_query_rows(
            run_sql_with_catalog(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select p.name, (select q.name from pets q where q.owner_id = p.id and q.id = 999) from people p order by p.id",
                catalog_with_pets(),
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
        let base = temp_dir("scalar_subquery_multiple_rows");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        seed_people_and_pets(&base, &mut txns);
        let err = run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select (select q.name from pets q where q.owner_id = p.id) from people p where p.id = 1",
            catalog_with_pets(),
        )
        .unwrap_err();
        assert!(
            format!("{err:?}")
                .contains("more than one row returned by a subquery used as an expression")
        );
    }

    #[test]
    fn exists_and_not_exists_are_correlated_per_row() {
        let base = temp_dir("exists_correlated_per_row");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        seed_people_and_pets(&base, &mut txns);
        assert_query_rows(
            run_sql_with_catalog(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select p.id from people p where exists (select 1 from pets q where q.owner_id = p.id) order by p.id",
                catalog_with_pets(),
            )
            .unwrap(),
            vec![vec![Value::Int32(1)], vec![Value::Int32(2)]],
        );
        assert_query_rows(
            run_sql_with_catalog(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select p.id from people p where not exists (select 1 from pets q where q.owner_id = p.id) order by p.id",
                catalog_with_pets(),
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
        let base = temp_dir("correlated_any_subquery");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        seed_people_and_pets(&base, &mut txns);
        assert_query_rows(
            run_sql_with_catalog(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select p.id from people p where p.id = any (select q.owner_id from pets q where q.owner_id is not null) order by p.id",
                catalog_with_pets(),
            )
            .unwrap(),
            vec![vec![Value::Int32(1)], vec![Value::Int32(2)]],
        );
    }

    #[test]
    fn grouped_query_having_can_use_correlated_exists() {
        let base = temp_dir("grouped_having_correlated_exists");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        seed_people_and_pets(&base, &mut txns);
        assert_query_rows(
            run_sql_with_catalog(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select p.id, count(*) from people p group by p.id having exists (select 1 from pets q where q.owner_id = p.id) order by p.id",
                catalog_with_pets(),
            )
            .unwrap(),
            vec![
                vec![Value::Int32(1), Value::Int64(1)],
                vec![Value::Int32(2), Value::Int64(1)],
            ],
        );
    }

    #[test]
    fn degenerate_having_does_not_scan_where_clause() {
        let base = temp_dir("degenerate_having_no_scan");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let pool = test_pool(&base);
        let xid = txns.begin();
        for row in [
            tuple(0, "zero", Some("z")),
            tuple(1, "one", Some("o")),
        ] {
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
        let base = temp_dir("nested_outer_correlation");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        seed_people_and_pets(&base, &mut txns);
        assert_query_rows(
            run_sql_with_catalog(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select p.id from people p where exists (select 1 from pets q where q.owner_id = p.id and exists (select 1 from people r where r.id = p.id and r.name = p.name)) order by p.id",
                catalog_with_pets(),
            )
            .unwrap(),
            vec![vec![Value::Int32(1)], vec![Value::Int32(2)]],
        );
    }

    #[test]
    fn scalar_subquery_can_be_used_in_order_by() {
        let base = temp_dir("scalar_subquery_order_by");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        seed_people_and_pets(&base, &mut txns);
        assert_query_rows(
            run_sql_with_catalog(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select p.name from people p order by (select count(*) from pets q where q.owner_id = p.id) desc, p.id",
                catalog_with_pets(),
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
        assert!(matches!(err, ExecError::NumericFieldOverflow));
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
                assert_eq!(rows.len(), 5);
                assert_eq!(rows[0], vec![Value::Numeric("0.0".into())]);
                assert_eq!(rows[4], vec![Value::Numeric("4.0".into())]);
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
                assert_eq!(rows.len(), 4);
                assert_eq!(rows[0], vec![Value::Numeric("0.0".into())]);
                assert_eq!(rows[3], vec![Value::Numeric("0.9".into())]);
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
