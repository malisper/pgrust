use super::super::*;
use crate::backend::parser::{CreateDatabaseStatement, DropDatabaseStatement};
use crate::include::catalog::{TEMPLATE0_DATABASE_NAME, TEMPLATE1_DATABASE_NAME};
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

        let template = {
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
            cache
                .database_rows()
                .into_iter()
                .find(|row| row.datname.eq_ignore_ascii_case(TEMPLATE1_DATABASE_NAME))
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!(
                        "template database \"{}\" does not exist",
                        TEMPLATE1_DATABASE_NAME
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "3D000",
                })?
        };
        flush_database_buffers_to_disk(self, template.oid)?;

        let mut row = template.clone();
        row.oid = 0;
        row.datname = stmt.database_name.to_ascii_lowercase();
        row.datdba = auth.current_user_oid();
        row.datistemplate = false;
        row.datallowconn = true;
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
