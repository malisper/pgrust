use super::super::*;
use crate::include::nodes::parsenodes::CreateTablespaceStatement;
use std::fs;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::symlink;
#[cfg(windows)]
use std::os::windows::fs::symlink_dir as symlink;

const TABLESPACE_VERSION_DIRECTORY: &str = "PG_18_202406281";

impl Database {
    pub(crate) fn execute_create_tablespace_stmt(
        &self,
        client_id: ClientId,
        stmt: &CreateTablespaceStatement,
        allow_in_place_tablespaces: bool,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_create_tablespace_stmt_in_transaction(
            client_id,
            stmt,
            allow_in_place_tablespaces,
            xid,
            0,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_tablespace_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &CreateTablespaceStatement,
        allow_in_place_tablespaces: bool,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        if catcache
            .tablespace_rows()
            .into_iter()
            .any(|row| row.spcname.eq_ignore_ascii_case(&stmt.tablespace_name))
        {
            return Err(ExecError::DetailedError {
                message: format!("tablespace \"{}\" already exists", stmt.tablespace_name),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }

        let owner_oid = self.auth_state(client_id).current_user_oid();
        let normalized_location =
            normalize_tablespace_location(&stmt.location, allow_in_place_tablespaces)?;
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let (_oid, effect) = self
            .catalog
            .write()
            .create_tablespace_mvcc(&stmt.tablespace_name, owner_oid, &ctx)
            .map_err(map_catalog_error)?;
        create_tablespace_directories(
            &self.cluster.base_dir,
            &normalized_location,
            _oid,
            allow_in_place_tablespaces,
        )?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}

fn normalize_tablespace_location(
    location: &str,
    allow_in_place_tablespaces: bool,
) -> Result<String, ExecError> {
    let trimmed = location.trim();
    if trimmed.contains('\'') {
        return Err(ExecError::DetailedError {
            message: "tablespace location cannot contain single quotes".into(),
            detail: None,
            hint: None,
            sqlstate: "42602",
        });
    }
    if trimmed.is_empty() {
        if allow_in_place_tablespaces {
            return Ok(String::new());
        }
        return Err(ExecError::DetailedError {
            message: "tablespace location must be an absolute path".into(),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }

    let mut normalized = trimmed.to_string();
    while normalized.len() > 1 && normalized.ends_with(std::path::MAIN_SEPARATOR) {
        normalized.pop();
    }
    if !Path::new(&normalized).is_absolute() {
        return Err(ExecError::DetailedError {
            message: "tablespace location must be an absolute path".into(),
            detail: None,
            hint: None,
            sqlstate: "42P17",
        });
    }
    Ok(normalized)
}

fn create_tablespace_directories(
    base_dir: &Path,
    location: &str,
    tablespace_oid: u32,
    allow_in_place_tablespaces: bool,
) -> Result<(), ExecError> {
    if cfg!(target_arch = "wasm32") {
        return Err(ExecError::DetailedError {
            message: "CREATE TABLESPACE is not supported in browser wasm builds".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }

    let pg_tblspc_dir = base_dir.join("pg_tblspc");
    fs::create_dir_all(&pg_tblspc_dir).map_err(exec_error_for_io)?;

    let linkloc = pg_tblspc_dir.join(tablespace_oid.to_string());
    let in_place = allow_in_place_tablespaces && location.is_empty();
    let version_base = if in_place {
        ensure_fresh_directory(&linkloc)?;
        linkloc.clone()
    } else {
        let location_path = PathBuf::from(location);
        ensure_existing_directory(&location_path)?;
        create_symlink_if_missing(&location_path, &linkloc)?;
        linkloc.clone()
    };

    let version_dir = version_base.join(TABLESPACE_VERSION_DIRECTORY);
    ensure_fresh_directory(&version_dir)?;
    Ok(())
}

fn ensure_existing_directory(path: &Path) -> Result<(), ExecError> {
    let metadata = fs::metadata(path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            ExecError::DetailedError {
                message: format!("directory \"{}\" does not exist", path.display()),
                detail: None,
                hint: None,
                sqlstate: "58P01",
            }
        } else {
            exec_error_for_io(err)
        }
    })?;
    if !metadata.is_dir() {
        return Err(ExecError::DetailedError {
            message: format!("\"{}\" exists but is not a directory", path.display()),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    Ok(())
}

fn ensure_fresh_directory(path: &Path) -> Result<(), ExecError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.is_dir() {
                return Err(ExecError::DetailedError {
                    message: format!("\"{}\" exists but is not a directory", path.display()),
                    detail: None,
                    hint: None,
                    sqlstate: "42809",
                });
            }
            return Err(ExecError::DetailedError {
                message: format!(
                    "directory \"{}\" already in use as a tablespace",
                    path.display()
                ),
                detail: None,
                hint: None,
                sqlstate: "55006",
            });
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(exec_error_for_io(err)),
    }
    fs::create_dir_all(path).map_err(exec_error_for_io)?;
    Ok(())
}

#[cfg(any(unix, windows))]
fn create_symlink_if_missing(target: &Path, link: &Path) -> Result<(), ExecError> {
    match fs::symlink_metadata(link) {
        Ok(_) => {
            return Err(ExecError::DetailedError {
                message: format!(
                    "directory \"{}\" already in use as a tablespace",
                    link.display()
                ),
                detail: None,
                hint: None,
                sqlstate: "55006",
            });
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(exec_error_for_io(err)),
    }

    symlink(target, link).map_err(exec_error_for_io)
}

#[cfg(not(any(unix, windows)))]
fn create_symlink_if_missing(_target: &Path, _link: &Path) -> Result<(), ExecError> {
    Err(ExecError::DetailedError {
        message: "tablespace symlinks are not supported on this platform".into(),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    })
}

fn exec_error_for_io(err: std::io::Error) -> ExecError {
    ExecError::DetailedError {
        message: err.to_string(),
        detail: None,
        hint: None,
        sqlstate: "58030",
    }
}
