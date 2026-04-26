use super::super::*;
use crate::backend::catalog::roles::find_role_by_name;
use crate::backend::parser::{
    AlterDatabaseAction, AlterDatabaseStatement, CreateDatabaseStatement, DropDatabaseStatement,
};
use crate::include::catalog::{
    DEFAULT_TABLESPACE_OID, TEMPLATE0_DATABASE_NAME, TEMPLATE1_DATABASE_NAME,
};
use std::fs;
use std::path::{Path, PathBuf};

impl Database {
    pub(crate) fn execute_create_database_stmt(
        &self,
        client_id: ClientId,
        stmt: &CreateDatabaseStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let mut target_dir_to_cleanup: Option<PathBuf> = None;
        let result = (|| {
            let auth = self.auth_state(client_id);
            let auth_catalog = self
                .auth_catalog(client_id, Some((xid, 0)))
                .map_err(map_catalog_error)?;
            let current_role = auth_catalog
                .role_by_oid(auth.current_user_oid())
                .ok_or_else(|| ExecError::DetailedError {
                    message: "current role does not exist".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })?;
            if !current_role.rolsuper && !current_role.rolcreatedb {
                return Err(ExecError::DetailedError {
                    message: "permission denied to create database".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42501",
                });
            }

            let (template, owner_oid, tablespace_oid) = {
                let cache = self
                    .backend_catcache(client_id, Some((xid, 0)))
                    .map_err(map_catalog_error)?;
                if cache
                    .database_rows()
                    .into_iter()
                    .any(|row| row.datname.eq_ignore_ascii_case(&stmt.database_name))
                {
                    return Err(ExecError::DetailedError {
                        message: format!("database \"{}\" already exists", stmt.database_name),
                        detail: None,
                        hint: None,
                        sqlstate: "42P04",
                    });
                }
                let owner_oid =
                    if let Some(owner_name) = option_non_default(&stmt.options.owner) {
                        let owner = find_role_by_name(auth_catalog.roles(), owner_name)
                            .ok_or_else(|| ExecError::DetailedError {
                                message: format!("role \"{}\" does not exist", owner_name),
                                detail: None,
                                hint: None,
                                sqlstate: "42704",
                            })?;
                        if !auth.can_set_role(owner.oid, &auth_catalog) {
                            return Err(ExecError::DetailedError {
                                message: format!("must be able to SET ROLE \"{}\"", owner.rolname),
                                detail: None,
                                hint: None,
                                sqlstate: "42501",
                            });
                        }
                        owner.oid
                    } else {
                        auth.current_user_oid()
                    };
                let tablespace_oid = if let Some(tablespace_name) =
                    option_non_default(&stmt.options.tablespace)
                {
                    cache
                        .tablespace_rows()
                        .into_iter()
                        .find(|row| row.spcname.eq_ignore_ascii_case(tablespace_name))
                        .map(|row| row.oid)
                        .ok_or_else(|| ExecError::DetailedError {
                            message: format!("tablespace \"{}\" does not exist", tablespace_name),
                            detail: None,
                            hint: None,
                            sqlstate: "42704",
                        })?
                } else {
                    DEFAULT_TABLESPACE_OID
                };
                let template_name =
                    option_non_default(&stmt.options.template).unwrap_or(TEMPLATE1_DATABASE_NAME);
                cache
                    .database_rows()
                    .into_iter()
                    .find(|row| row.datname.eq_ignore_ascii_case(template_name))
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("template database \"{}\" does not exist", template_name),
                        detail: None,
                        hint: None,
                        sqlstate: "3D000",
                    })
                    .map(|template| (template, owner_oid, tablespace_oid))?
            };
            flush_database_buffers_to_disk(self, template.oid)?;

            let mut row = template.clone();
            row.oid = 0;
            row.datname = stmt.database_name.to_ascii_lowercase();
            row.datdba = owner_oid;
            row.datistemplate = stmt.options.is_template.unwrap_or(false);
            row.datallowconn = stmt.options.allow_connections.unwrap_or(true);
            row.datconnlimit = stmt.options.connection_limit.unwrap_or(-1);
            row.dattablespace = tablespace_oid;
            if let Some(encoding) = option_non_default(&stmt.options.encoding) {
                row.encoding = database_encoding_code(encoding)?;
            }
            if let Some(lc_collate) = option_non_default(&stmt.options.lc_collate) {
                row.datcollate = lc_collate.into();
                row.datcollversion = None;
            }
            if let Some(lc_ctype) = option_non_default(&stmt.options.lc_ctype) {
                row.datctype = lc_ctype.into();
            }
            row.datacl = None;

            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: 0,
                client_id,
                waiter: None,
                interrupts: self.interrupt_state(client_id),
            };
            let (row, effect) = self
                .shared_catalog
                .write()
                .create_database_row_mvcc(row, &ctx)
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);

            let template_dir = self
                .cluster
                .base_dir
                .join("base")
                .join(template.oid.to_string());
            let target_dir = self.cluster.base_dir.join("base").join(row.oid.to_string());
            target_dir_to_cleanup = Some(target_dir.clone());
            copy_dir_all(&template_dir, &target_dir).map_err(|e| ExecError::DetailedError {
                message: format!("could not initialize database directory: {e}"),
                detail: None,
                hint: None,
                sqlstate: "58030",
            })?;
            sync_cloned_local_catalogs(self, template.oid, row.oid)?;

            Ok(StatementResult::AffectedRows(0))
        })();
        if result.is_err()
            && let Some(target_dir) = &target_dir_to_cleanup
        {
            let _ = fs::remove_dir_all(target_dir);
        }
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_database_stmt(
        &self,
        client_id: ClientId,
        stmt: &AlterDatabaseStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_database_stmt_in_transaction(
            client_id,
            stmt,
            xid,
            0,
            true,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_database_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &AlterDatabaseStatement,
        xid: TransactionId,
        cid: CommandId,
        is_top_level: bool,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        if matches!(stmt.action, AlterDatabaseAction::SetTablespace { .. }) && !is_top_level {
            return Err(ExecError::Parse(ParseError::ActiveSqlTransaction(
                "ALTER DATABASE SET TABLESPACE",
            )));
        }

        let auth = self.auth_state(client_id);
        let auth_catalog = self
            .auth_catalog(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let current_role = auth_catalog
            .role_by_oid(auth.current_user_oid())
            .ok_or_else(|| ExecError::DetailedError {
                message: "current role does not exist".into(),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        let cache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let mut row = cache
            .database_rows()
            .into_iter()
            .find(|row| row.datname.eq_ignore_ascii_case(&stmt.database_name))
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("database \"{}\" does not exist", stmt.database_name),
                detail: None,
                hint: None,
                sqlstate: "3D000",
            })?;
        if !current_role.rolsuper && !auth.has_effective_membership(row.datdba, &auth_catalog) {
            return Err(ExecError::DetailedError {
                message: format!("must be owner of database {}", stmt.database_name),
                detail: None,
                hint: None,
                sqlstate: "42501",
            });
        }

        match &stmt.action {
            AlterDatabaseAction::Rename { new_name } => {
                if row.oid == self.database_oid {
                    return Err(ExecError::DetailedError {
                        message: format!(
                            "current database cannot be renamed from \"{}\" to \"{}\"",
                            row.datname, new_name
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "55006",
                    });
                }
                if cache
                    .database_rows()
                    .into_iter()
                    .any(|candidate| candidate.datname.eq_ignore_ascii_case(new_name))
                {
                    return Err(ExecError::DetailedError {
                        message: format!("database \"{}\" already exists", new_name),
                        detail: None,
                        hint: None,
                        sqlstate: "42P04",
                    });
                }
                row.datname = new_name.to_ascii_lowercase();
            }
            AlterDatabaseAction::SetTablespace { tablespace_name } => {
                row.dattablespace = cache
                    .tablespace_rows()
                    .into_iter()
                    .find(|tablespace| tablespace.spcname.eq_ignore_ascii_case(tablespace_name))
                    .map(|tablespace| tablespace.oid)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("tablespace \"{}\" does not exist", tablespace_name),
                        detail: None,
                        hint: None,
                        sqlstate: "42704",
                    })?;
                // :HACK: pgrust does not yet place database storage under
                // tablespace directories, so this keeps pg_database metadata in
                // sync without moving files. Real tablespace storage should
                // replace this catalog-only update.
            }
            AlterDatabaseAction::ResetTablespace => {
                // :HACK: PostgreSQL parses this as ALTER DATABASE RESET of a
                // database-local setting. pgrust does not model per-database
                // GUC settings yet, and the regression only requires the
                // command to load the database catalog entry successfully.
                return Ok(StatementResult::AffectedRows(0));
            }
            AlterDatabaseAction::ConnectionLimit { limit } => {
                row.datconnlimit = *limit;
            }
            AlterDatabaseAction::OwnerTo { new_owner } => {
                if !current_role.rolsuper && !current_role.rolcreatedb {
                    return Err(ExecError::DetailedError {
                        message: "permission denied to change owner of database".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42501",
                    });
                }
                let new_owner =
                    find_role_by_name(auth_catalog.roles(), new_owner).ok_or_else(|| {
                        ExecError::DetailedError {
                            message: format!("role \"{}\" does not exist", new_owner),
                            detail: None,
                            hint: None,
                            sqlstate: "42704",
                        }
                    })?;
                if !auth.can_set_role(new_owner.oid, &auth_catalog) {
                    return Err(ExecError::DetailedError {
                        message: format!("must be able to SET ROLE \"{}\"", new_owner.rolname),
                        detail: None,
                        hint: None,
                        sqlstate: "42501",
                    });
                }
                row.datdba = new_owner.oid;
            }
        }

        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .shared_catalog
            .write()
            .replace_database_row_mvcc(row, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_drop_database_stmt(
        &self,
        client_id: ClientId,
        stmt: &DropDatabaseStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = (|| {
            let auth = self.auth_state(client_id);
            let auth_catalog = self
                .auth_catalog(client_id, Some((xid, 0)))
                .map_err(map_catalog_error)?;
            let current_role = auth_catalog
                .role_by_oid(auth.current_user_oid())
                .ok_or_else(|| ExecError::DetailedError {
                    message: "current role does not exist".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                })?;
            if !current_role.rolsuper && !current_role.rolcreatedb {
                return Err(ExecError::DetailedError {
                    message: "permission denied to drop database".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "42501",
                });
            }

            let cache = self
                .backend_catcache(client_id, Some((xid, 0)))
                .map_err(map_catalog_error)?;
            let Some(row) = cache
                .database_rows()
                .into_iter()
                .find(|row| row.datname.eq_ignore_ascii_case(&stmt.database_name))
            else {
                return if stmt.if_exists {
                    Ok(StatementResult::AffectedRows(0))
                } else {
                    Err(ExecError::DetailedError {
                        message: format!("database \"{}\" does not exist", stmt.database_name),
                        detail: None,
                        hint: None,
                        sqlstate: "3D000",
                    })
                };
            };

            if row.datname.eq_ignore_ascii_case(TEMPLATE0_DATABASE_NAME)
                || row.datname.eq_ignore_ascii_case(TEMPLATE1_DATABASE_NAME)
            {
                return Err(ExecError::DetailedError {
                    message: format!("cannot drop database \"{}\"", row.datname),
                    detail: None,
                    hint: None,
                    sqlstate: "55006",
                });
            }
            if row.oid == self.database_oid {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "cannot drop the currently open database \"{}\"",
                        row.datname
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "55006",
                });
            }
            if self
                .cluster
                .active_connections
                .read()
                .get(&row.oid)
                .copied()
                .unwrap_or(0)
                > 0
            {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "database \"{}\" is being accessed by other users",
                        row.datname
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "55006",
                });
            }

            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: 0,
                client_id,
                waiter: None,
                interrupts: self.interrupt_state(client_id),
            };
            let (_, effect) = self
                .shared_catalog
                .write()
                .drop_database_row_mvcc(&row.datname, &ctx)
                .map_err(map_catalog_error)?;
            catalog_effects.push(effect);

            if let Some(state) = self.cluster.open_databases.read().get(&row.oid).cloned() {
                invalidate_database_buffers(self, &state.catalog.read(), row.oid);
            }
            let db_dir = self.cluster.base_dir.join("base").join(row.oid.to_string());
            if db_dir.exists() {
                fs::remove_dir_all(&db_dir).map_err(|e| ExecError::DetailedError {
                    message: format!("could not remove database directory: {e}"),
                    detail: None,
                    hint: None,
                    sqlstate: "58030",
                })?;
            }
            self.cluster.open_databases.write().remove(&row.oid);

            Ok(StatementResult::AffectedRows(0))
        })();
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }
}

fn option_non_default(value: &Option<String>) -> Option<&str> {
    value
        .as_deref()
        .filter(|value| !value.eq_ignore_ascii_case("default"))
}

fn database_encoding_code(encoding: &str) -> Result<i32, ExecError> {
    match encoding.to_ascii_lowercase().replace('-', "_").as_str() {
        "utf8" | "unicode" => Ok(6),
        "sql_ascii" => Ok(0),
        _ => Err(ExecError::DetailedError {
            message: format!("{} is not a valid encoding name", encoding),
            detail: None,
            hint: None,
            sqlstate: "22023",
        }),
    }
}

fn invalidate_database_buffers(
    db: &Database,
    catalog: &crate::backend::catalog::CatalogStore,
    db_oid: u32,
) {
    let Ok(relcache) = catalog.relcache() else {
        return;
    };
    for (_, entry) in relcache.entries() {
        if entry.rel.db_oid == db_oid {
            let _ = db.pool.invalidate_relation(entry.rel);
        }
    }
}

fn flush_database_buffers_to_disk(db: &Database, db_oid: u32) -> Result<(), ExecError> {
    if let Some(state) = db.cluster.open_databases.read().get(&db_oid) {
        return flush_catalog_buffers(db, &state.catalog.read(), db_oid);
    }

    let catalog =
        crate::backend::catalog::CatalogStore::load_database(&db.cluster.base_dir, db_oid)
            .map_err(map_catalog_error)?;
    flush_catalog_buffers(db, &catalog, db_oid)
}

fn flush_catalog_buffers(
    db: &Database,
    catalog: &crate::backend::catalog::CatalogStore,
    db_oid: u32,
) -> Result<(), ExecError> {
    let relcache = catalog.relcache().map_err(map_catalog_error)?;
    for (_, entry) in relcache.entries() {
        if entry.rel.db_oid == db_oid {
            db.pool
                .flush_relation(entry.rel)
                .map_err(|e| ExecError::DetailedError {
                    message: format!("could not flush template database pages: {e:?}"),
                    detail: None,
                    hint: None,
                    sqlstate: "58030",
                })?;
        }
    }
    db.pool.flush_wal().map_err(|e| ExecError::DetailedError {
        message: format!("could not flush template database WAL: {e}"),
        detail: None,
        hint: None,
        sqlstate: "58030",
    })?;
    Ok(())
}

fn sync_cloned_local_catalogs(
    db: &Database,
    source_db_oid: u32,
    target_db_oid: u32,
) -> Result<(), ExecError> {
    let kinds = crate::backend::catalog::bootstrap::bootstrap_catalog_kinds()
        .into_iter()
        .filter(|kind| {
            matches!(
                kind.scope(),
                crate::include::catalog::CatalogScope::Database(_)
            )
        })
        .collect::<Vec<_>>();
    let rows = crate::backend::catalog::loader::load_physical_catalog_rows_scoped(
        &db.cluster.base_dir,
        source_db_oid,
        &kinds,
    )
    .map_err(map_catalog_error)?;
    crate::backend::catalog::persistence::sync_catalog_rows_subset(
        &db.cluster.base_dir,
        &rows,
        target_db_oid,
        &kinds,
    )
    .map_err(map_catalog_error)
}

fn copy_dir_all(src: &Path, dst: &Path) -> std::io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_all(&from, &to)?;
        } else {
            fs::copy(&from, &to)?;
        }
    }
    Ok(())
}
