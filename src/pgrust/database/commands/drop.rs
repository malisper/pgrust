use super::super::*;
use crate::include::nodes::parsenodes::{DropIndexStatement, DropSchemaStatement};

impl Database {
    pub(crate) fn execute_drop_domain_stmt_with_search_path(
        &self,
        _client_id: ClientId,
        drop_stmt: &DropDomainStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let (normalized, _, _) =
            self.normalize_domain_name_for_create(&drop_stmt.domain_name, configured_search_path)?;
        let mut domains = self.domains.write();
        if domains.remove(&normalized).is_none() {
            if drop_stmt.if_exists {
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::Parse(ParseError::UnsupportedType(
                drop_stmt.domain_name.clone(),
            )));
        }
        self.plan_cache.invalidate_all();
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_table_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &crate::backend::parser::DropTableStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        temp_effects: &mut Vec<TempMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_drop_relation_stmt_in_transaction_with_search_path(
            client_id,
            &drop_stmt.table_names,
            drop_stmt.if_exists,
            xid,
            cid,
            configured_search_path,
            catalog_effects,
            Some(temp_effects),
            'r',
            "table",
        )
    }

    pub(crate) fn execute_drop_view_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropViewStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_drop_relation_stmt_in_transaction_with_search_path(
            client_id,
            &drop_stmt.view_names,
            drop_stmt.if_exists,
            xid,
            cid,
            configured_search_path,
            catalog_effects,
            None,
            'v',
            "view",
        )
    }

    pub(crate) fn execute_drop_index_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropIndexStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        self.execute_drop_relation_stmt_in_transaction_with_search_path(
            client_id,
            &drop_stmt.index_names,
            drop_stmt.if_exists,
            xid,
            cid,
            configured_search_path,
            catalog_effects,
            None,
            'i',
            "index",
        )
    }

    pub(crate) fn execute_drop_schema_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        drop_stmt: &DropSchemaStatement,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let mut dropped = 0usize;
        for schema_name in &drop_stmt.schema_names {
            let maybe_schema = catcache
                .namespace_by_name(schema_name)
                .cloned()
                .filter(|row| !self.other_session_temp_namespace_oid(client_id, row.oid));
            let schema = match maybe_schema {
                Some(schema) => schema,
                None if drop_stmt.if_exists => continue,
                None => {
                    return Err(ExecError::DetailedError {
                        message: format!("schema \"{schema_name}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    });
                }
            };
            if schema.oid == crate::include::catalog::PG_CATALOG_NAMESPACE_OID {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "cannot drop schema {schema_name} because it is required by the database system"
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "2BP01",
                });
            }
            let auth = self.auth_state(client_id);
            let auth_catalog = self.txn_auth_catalog(client_id, xid, cid).map_err(|err| {
                ExecError::Parse(ParseError::UnexpectedToken {
                    expected: "authorization catalog",
                    actual: format!("{err:?}"),
                })
            })?;
            if !auth.has_effective_membership(schema.nspowner, &auth_catalog) {
                return Err(ExecError::DetailedError {
                    message: format!("must be owner of schema {schema_name}"),
                    detail: None,
                    hint: None,
                    sqlstate: "42501",
                });
            }
            let has_relations = catcache
                .class_rows()
                .into_iter()
                .any(|row| row.relnamespace == schema.oid);
            if has_relations {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "cannot drop schema {schema_name} because other objects depend on it"
                    ),
                    detail: Some("schema is not empty".into()),
                    hint: None,
                    sqlstate: "2BP01",
                });
            }
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid,
                client_id,
                waiter: Some(self.txn_waiter.clone()),
                interrupts: self.interrupt_state(client_id),
            };
            let effect = self
                .catalog
                .write()
                .drop_namespace_mvcc(schema.oid, &schema.nspname, schema.nspowner, &ctx)
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);
            dropped += 1;
        }
        Ok(StatementResult::AffectedRows(dropped))
    }

    fn execute_drop_relation_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        relation_names: &[String],
        if_exists: bool,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
        mut temp_effects: Option<&mut Vec<TempMutationEffect>>,
        expected_relkind: char,
        expected_name: &'static str,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let rels = relation_names
            .iter()
            .filter_map(|name| catalog.lookup_any_relation(name).map(|e| e.rel))
            .collect::<Vec<_>>();
        lock_tables_interruptible(
            &self.table_locks,
            client_id,
            &rels,
            TableLockMode::AccessExclusive,
            interrupts.as_ref(),
        )?;

        let mut dropped = 0usize;
        let mut result = Ok(StatementResult::AffectedRows(0));
        for relation_name in relation_names {
            let maybe_entry = catalog.lookup_any_relation(relation_name);
            if expected_relkind == 'r'
                && maybe_entry
                    .as_ref()
                    .is_some_and(|entry| entry.relpersistence == 't')
            {
                if let Some(entry) = maybe_entry.as_ref() {
                    if let Err(err) = ensure_relation_owner(self, client_id, entry, relation_name) {
                        result = Err(err);
                        break;
                    }
                }
                match self.drop_temp_relation_in_transaction(
                    client_id,
                    relation_name,
                    xid,
                    cid,
                    catalog_effects,
                    temp_effects
                        .as_deref_mut()
                        .expect("temp effects required for DROP TABLE"),
                ) {
                    Ok(_) => dropped += 1,
                    Err(_) if if_exists => {}
                    Err(err) => {
                        result = Err(err);
                        break;
                    }
                }
                continue;
            }

            let relation_oid = match maybe_entry.as_ref() {
                Some(entry) if entry.relkind == expected_relkind => entry.relation_oid,
                Some(_) => {
                    result = Err(ExecError::Parse(ParseError::WrongObjectType {
                        name: relation_name.clone(),
                        expected: expected_name,
                    }));
                    break;
                }
                None if if_exists => continue,
                None => {
                    result = Err(ExecError::Parse(ParseError::TableDoesNotExist(
                        relation_name.clone(),
                    )));
                    break;
                }
            };
            if let Some(entry) = maybe_entry.as_ref() {
                if let Err(err) = ensure_relation_owner(self, client_id, entry, relation_name) {
                    result = Err(err);
                    break;
                }
            }
            if expected_relkind != 'i' {
                if expected_relkind == 'r' {
                    if let Err(err) = reject_relation_with_referencing_foreign_keys(
                        &catalog,
                        relation_oid,
                        "DROP TABLE on table without referencing foreign keys",
                    ) {
                        result = Err(err);
                        break;
                    }
                }
                if let Err(err) = reject_relation_with_dependent_views(
                    self,
                    client_id,
                    Some((xid, cid)),
                    relation_oid,
                    if expected_relkind == 'v' {
                        "DROP VIEW on relation without dependent views"
                    } else {
                        "DROP TABLE on relation without dependent views"
                    },
                ) {
                    result = Err(err);
                    break;
                }
            }
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid,
                client_id,
                waiter: Some(self.txn_waiter.clone()),
                interrupts: Arc::clone(&interrupts),
            };
            let drop_result = match expected_relkind {
                'v' => self
                    .catalog
                    .write()
                    .drop_view_by_oid_mvcc(relation_oid, &ctx)
                    .map(|(_, effect)| effect),
                'i' => self
                    .catalog
                    .write()
                    .drop_relation_entry_by_oid_mvcc(relation_oid, &ctx)
                    .map(|(_, effect)| effect),
                _ => self
                    .catalog
                    .write()
                    .drop_relation_by_oid_mvcc(relation_oid, &ctx)
                    .map(|(_, effect)| effect),
            };
            match drop_result {
                Ok(effect) => {
                    if expected_relkind != 'v' {
                        self.apply_catalog_mutation_effect_immediate(&effect)?;
                    }
                    if expected_relkind == 'r' {
                        self.session_stats_state(client_id)
                            .write()
                            .note_relation_drop(relation_oid, &self.stats);
                    }
                    catalog_effects.push(effect);
                    dropped += 1;
                }
                Err(CatalogError::UnknownTable(_)) if if_exists => {}
                Err(CatalogError::UnknownTable(_)) => {
                    result = Err(ExecError::Parse(ParseError::TableDoesNotExist(
                        relation_name.clone(),
                    )));
                    break;
                }
                Err(other) => {
                    result = Err(ExecError::Parse(ParseError::UnexpectedToken {
                        expected: match expected_relkind {
                            'i' => "droppable index",
                            'v' => "droppable view",
                            _ => "droppable table",
                        },
                        actual: format!("{other:?}"),
                    }));
                    break;
                }
            }
        }

        for rel in rels {
            self.table_locks.unlock_table(rel, client_id);
        }

        if result.is_ok() {
            Ok(StatementResult::AffectedRows(dropped))
        } else {
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgrust::session::Session;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "pgrust_drop_{}_{}_{}",
            label,
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn drop_index_removes_index_relation() {
        let base = temp_dir("index");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create table widgets (id int4)")
            .unwrap();
        session
            .execute(&db, "create index widgets_id_idx on widgets(id)")
            .unwrap();

        assert!(
            db.backend_catcache(1, None)
                .unwrap()
                .class_by_name("widgets_id_idx")
                .is_some()
        );

        session.execute(&db, "drop index widgets_id_idx").unwrap();

        assert!(
            db.backend_catcache(1, None)
                .unwrap()
                .class_by_name("widgets_id_idx")
                .is_none()
        );
    }

    #[test]
    fn drop_schema_removes_empty_namespace() {
        let base = temp_dir("schema");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session.execute(&db, "create schema tenant_drop").unwrap();

        assert!(
            db.backend_catcache(1, None)
                .unwrap()
                .namespace_by_name("tenant_drop")
                .is_some()
        );

        session.execute(&db, "drop schema tenant_drop").unwrap();

        assert!(
            db.backend_catcache(1, None)
                .unwrap()
                .namespace_by_name("tenant_drop")
                .is_none()
        );
    }
}
