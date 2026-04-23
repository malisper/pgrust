use super::super::*;
use crate::backend::commands::partition::{
    validate_new_partition_bound, validate_partition_relation_compatibility,
    validate_relation_rows_for_partition_bound,
};
use crate::backend::executor::ExecutorContext;
use crate::backend::parser::{lower_partition_bound_for_relation, serialize_partition_bound};

fn ddl_executor_context(
    db: &Database,
    catalog: &dyn CatalogLookup,
    client_id: ClientId,
    xid: TransactionId,
    cid: CommandId,
    interrupts: std::sync::Arc<crate::backend::utils::misc::interrupts::InterruptState>,
) -> Result<ExecutorContext, ExecError> {
    let snapshot = db.txns.read().snapshot_for_command(xid, cid)?;
    Ok(ExecutorContext {
        pool: std::sync::Arc::clone(&db.pool),
        txns: db.txns.clone(),
        txn_waiter: Some(db.txn_waiter.clone()),
        sequences: Some(db.sequences.clone()),
        large_objects: Some(db.large_objects.clone()),
        async_notify_runtime: Some(db.async_notify_runtime.clone()),
        advisory_locks: std::sync::Arc::clone(&db.advisory_locks),
        checkpoint_stats: db.checkpoint_stats_snapshot(),
        datetime_config: crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        interrupts,
        stats: std::sync::Arc::clone(&db.stats),
        session_stats: db.session_stats_state(client_id),
        snapshot,
        client_id,
        current_database_name: db.current_database_name(),
        session_user_oid: db.auth_state(client_id).session_user_oid(),
        current_user_oid: db.auth_state(client_id).current_user_oid(),
        active_role_oid: db.auth_state(client_id).active_role_oid(),
        statement_lock_scope_id: None,
        transaction_lock_scope_id: None,
        next_command_id: cid,
        default_toast_compression: crate::include::access::htup::AttributeCompression::Pglz,
        expr_bindings: crate::backend::executor::ExprEvalBindings::default(),
        case_test_values: Vec::new(),
        system_bindings: Vec::new(),
        subplans: Vec::new(),
        timed: false,
        allow_side_effects: false,
        pending_async_notifications: Vec::new(),
        catalog: catalog.materialize_visible_catalog(),
        compiled_functions: std::collections::HashMap::new(),
        cte_tables: std::collections::HashMap::new(),
        cte_producers: std::collections::HashMap::new(),
        recursive_worktables: std::collections::HashMap::new(),
        deferred_foreign_keys: None,
    })
}

impl Database {
    pub(crate) fn execute_alter_table_attach_partition_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::AlterTableAttachPartitionStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self
            .execute_alter_table_attach_partition_stmt_in_transaction_with_search_path(
                client_id,
                stmt,
                xid,
                0,
                configured_search_path,
                &mut catalog_effects,
            );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_table_attach_partition_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &crate::backend::parser::AlterTableAttachPartitionStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let parent = catalog
            .lookup_any_relation(&stmt.parent_table)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(stmt.parent_table.clone()))
            })?;
        if parent.relkind != 'p' {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: stmt.parent_table.clone(),
                expected: "partitioned table",
            }));
        }
        let child = catalog
            .lookup_any_relation(&stmt.partition_table)
            .ok_or_else(|| {
                ExecError::Parse(ParseError::TableDoesNotExist(stmt.partition_table.clone()))
            })?;
        ensure_relation_owner(self, client_id, &parent, &stmt.parent_table)?;
        ensure_relation_owner(self, client_id, &child, &stmt.partition_table)?;
        validate_partition_relation_compatibility(
            &catalog,
            &parent,
            &stmt.parent_table,
            &child,
            &stmt.partition_table,
        )?;

        let bound = lower_partition_bound_for_relation(&parent, &stmt.bound, &catalog)
            .map_err(ExecError::Parse)?;
        let mut ctx = ddl_executor_context(
            self,
            &catalog,
            client_id,
            xid,
            cid.saturating_add(1),
            std::sync::Arc::clone(&interrupts),
        )?;
        validate_relation_rows_for_partition_bound(&catalog, &parent, &child, &bound, &mut ctx)?;
        validate_new_partition_bound(
            &catalog,
            &parent,
            &stmt.partition_table,
            &bound,
            Some(child.relation_oid),
        )?;

        let inherit_ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: cid.saturating_add(2),
            client_id,
            waiter: None,
            interrupts: std::sync::Arc::clone(&interrupts),
        };
        let inherit_effect = self
            .catalog
            .write()
            .create_relation_inheritance_mvcc(
                child.relation_oid,
                &[parent.relation_oid],
                &inherit_ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&inherit_effect)?;
        catalog_effects.push(inherit_effect);

        let relpartbound = Some(serialize_partition_bound(&bound).map_err(ExecError::Parse)?);
        let updated_child = self.replace_relation_partition_metadata_in_transaction(
            client_id,
            child.relation_oid,
            true,
            relpartbound,
            child.partitioned_table.clone(),
            xid,
            cid.saturating_add(3),
            configured_search_path,
            catalog_effects,
        )?;
        if bound.is_default() {
            self.update_partitioned_table_default_partition_in_transaction(
                client_id,
                parent.relation_oid,
                updated_child.relation_oid,
                xid,
                cid.saturating_add(4),
                configured_search_path,
                catalog_effects,
            )?;
        }
        Ok(StatementResult::AffectedRows(0))
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::executor::{ExecError, StatementResult, Value};
    use crate::backend::parser::ParseError;
    use crate::pgrust::database::Database;
    use crate::pgrust::session::Session;
    use std::path::PathBuf;

    fn temp_dir(label: &str) -> PathBuf {
        crate::pgrust::test_support::seeded_temp_dir("partition", label)
    }

    fn query_rows(session: &mut Session, db: &Database, sql: &str) -> Vec<Vec<Value>> {
        match session.execute(db, sql).unwrap() {
            StatementResult::Query { rows, .. } => rows,
            other => panic!("expected query result, got {other:?}"),
        }
    }

    #[test]
    fn partition_introspection_functions_follow_declarative_tree() {
        let base = temp_dir("introspection");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table measurement (a int4, b int4) partition by range (a)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table measurement_lo partition of measurement \
                 for values from (minvalue) to (10) partition by list (b)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table measurement_lo_list partition of measurement_lo for values in (1)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table measurement_hi partition of measurement \
                 for values from (10) to (20)",
            )
            .unwrap();
        session
            .execute(&db, "create table plain_table (a int4)")
            .unwrap();

        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select relid::regclass::text, coalesce(parentrelid::regclass::text, ''), \
                        level, isleaf \
                   from pg_partition_tree('measurement') \
                  order by level, relid::regclass::text",
            ),
            vec![
                vec![
                    Value::Text("measurement".into()),
                    Value::Text("".into()),
                    Value::Int32(0),
                    Value::Bool(false),
                ],
                vec![
                    Value::Text("measurement_hi".into()),
                    Value::Text("measurement".into()),
                    Value::Int32(1),
                    Value::Bool(true),
                ],
                vec![
                    Value::Text("measurement_lo".into()),
                    Value::Text("measurement".into()),
                    Value::Int32(1),
                    Value::Bool(false),
                ],
                vec![
                    Value::Text("measurement_lo_list".into()),
                    Value::Text("measurement_lo".into()),
                    Value::Int32(2),
                    Value::Bool(true),
                ],
            ]
        );

        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select relid::regclass::text from pg_partition_ancestors('measurement_lo_list')",
            ),
            vec![
                vec![Value::Text("measurement_lo_list".into())],
                vec![Value::Text("measurement_lo".into())],
                vec![Value::Text("measurement".into())],
            ]
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select pg_partition_root('measurement_lo_list')::regclass::text",
            ),
            vec![vec![Value::Text("measurement".into())]]
        );
        assert_eq!(
            query_rows(&mut session, &db, "select * from pg_partition_tree(0)"),
            Vec::<Vec<Value>>::new()
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select * from pg_partition_ancestors(null)"
            ),
            Vec::<Vec<Value>>::new()
        );
        assert_eq!(
            query_rows(&mut session, &db, "select pg_partition_root(0)"),
            vec![vec![Value::Null]]
        );
        assert_eq!(
            query_rows(&mut session, &db, "select pg_partition_root('plain_table')",),
            vec![vec![Value::Null]]
        );
    }

    #[test]
    fn partitioned_root_dml_routes_rows_and_only_root_is_empty() {
        let base = temp_dir("routing");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table routed (a int4, b text) partition by list (a)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table routed1 partition of routed for values in (1)",
            )
            .unwrap();
        session
            .execute(
                &db,
                "create table routed2 partition of routed for values in (2)",
            )
            .unwrap();

        assert_eq!(
            session
                .execute(&db, "insert into routed values (1, 'one'), (2, 'two')")
                .unwrap(),
            StatementResult::AffectedRows(2)
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select tableoid::regclass::text, a, b from routed order by 1, 2",
            ),
            vec![
                vec![
                    Value::Text("routed1".into()),
                    Value::Int32(1),
                    Value::Text("one".into()),
                ],
                vec![
                    Value::Text("routed2".into()),
                    Value::Int32(2),
                    Value::Text("two".into()),
                ],
            ]
        );
        assert_eq!(
            query_rows(&mut session, &db, "select * from only routed"),
            Vec::<Vec<Value>>::new()
        );

        assert_eq!(
            session
                .execute(&db, "update routed set b = 'uno' where a = 1")
                .unwrap(),
            StatementResult::AffectedRows(1)
        );
        assert_eq!(
            session
                .execute(&db, "delete from routed where a = 2")
                .unwrap(),
            StatementResult::AffectedRows(1)
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select tableoid::regclass::text, a, b from routed order by 1, 2",
            ),
            vec![vec![
                Value::Text("routed1".into()),
                Value::Int32(1),
                Value::Text("uno".into()),
            ]]
        );

        match session.execute(&db, "insert into routed values (3, 'three')") {
            Err(ExecError::DetailedError {
                message,
                detail: Some(detail),
                sqlstate,
                ..
            }) => {
                assert_eq!(message, "no partition of relation \"routed\" found for row");
                assert!(detail.contains("(a) = (3)"));
                assert_eq!(sqlstate, "23514");
            }
            other => panic!("expected no-partition error, got {other:?}"),
        }

        match session.execute(&db, "insert into routed1 values (2, 'bad')") {
            Err(ExecError::DetailedError {
                message,
                detail: Some(detail),
                sqlstate,
                ..
            }) => {
                assert_eq!(
                    message,
                    "new row for relation \"routed1\" violates partition constraint"
                );
                assert!(detail.contains("(2, bad)"));
                assert_eq!(sqlstate, "23514");
            }
            other => panic!("expected partition constraint error, got {other:?}"),
        }

        match session.execute(&db, "update routed set a = 2 where a = 1") {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(message))) => {
                assert_eq!(
                    message,
                    "updating partition key columns on partitioned tables"
                );
            }
            other => panic!("expected partition-key update rejection, got {other:?}"),
        }
    }

    #[test]
    fn attach_partition_validates_rows_and_updates_metadata() {
        let base = temp_dir("attach");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        session
            .execute(
                &db,
                "create table parent_attach (a int4) partition by range (a)",
            )
            .unwrap();
        session
            .execute(&db, "create table child_ok (a int4)")
            .unwrap();
        session
            .execute(&db, "insert into child_ok values (0), (5)")
            .unwrap();

        assert_eq!(
            session
                .execute(
                    &db,
                    "alter table parent_attach attach partition child_ok \
                     for values from (0) to (10)",
                )
                .unwrap(),
            StatementResult::AffectedRows(0)
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select pg_partition_root('child_ok')::regclass::text",
            ),
            vec![vec![Value::Text("parent_attach".into())]]
        );
        assert_eq!(
            query_rows(
                &mut session,
                &db,
                "select relispartition, relpartbound is not null \
                   from pg_class where relname = 'child_ok'",
            ),
            vec![vec![Value::Bool(true), Value::Bool(true)]]
        );

        session
            .execute(&db, "create table child_bad (a int4)")
            .unwrap();
        session
            .execute(&db, "insert into child_bad values (15)")
            .unwrap();
        match session.execute(
            &db,
            "alter table parent_attach attach partition child_bad \
             for values from (0) to (10)",
        ) {
            Err(ExecError::DetailedError {
                message,
                detail: Some(detail),
                sqlstate,
                ..
            }) => {
                assert_eq!(
                    message,
                    "new row for relation \"child_bad\" violates partition constraint"
                );
                assert!(detail.contains("(15)"));
                assert_eq!(sqlstate, "23514");
            }
            other => panic!("expected attach validation failure, got {other:?}"),
        }

        session
            .execute(&db, "create table child_overlap (a int4)")
            .unwrap();
        match session.execute(
            &db,
            "alter table parent_attach attach partition child_overlap \
             for values from (5) to (15)",
        ) {
            Err(ExecError::DetailedError {
                message, sqlstate, ..
            }) => {
                assert_eq!(
                    message,
                    "partition \"child_overlap\" would overlap partition \"child_ok\""
                );
                assert_eq!(sqlstate, "42P17");
            }
            other => panic!("expected overlap failure, got {other:?}"),
        }
    }
}
