use super::super::*;
use super::maintenance::copy_rewritten_relation_storage;
use super::privilege::{acl_grants_privilege, effective_acl_grantee_names};
use crate::backend::catalog::roles::find_role_by_name;
use crate::backend::utils::cache::syscache::{SearchSysCache1, SysCacheId, SysCacheTuple, oid_key};
use crate::backend::utils::misc::notices::push_notice;
use crate::include::catalog::{DEFAULT_TABLESPACE_OID, GLOBAL_TABLESPACE_OID, PgTablespaceRow};
use crate::include::nodes::parsenodes::{
    AlterMoveAllTablespaceStatement, AlterTablespaceAction, AlterTablespaceStatement,
    CreateTablespaceStatement, DropTablespaceStatement, MoveAllTablespaceObjectKind, RoleSpec,
};
use pgrust_commands::tablespace::{
    merge_tablespace_options, normalize_tablespace_location, normalize_tablespace_options,
    reset_tablespace_options,
};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::symlink;
#[cfg(windows)]
use std::os::windows::fs::symlink_dir as symlink;

const TABLESPACE_VERSION_DIRECTORY: &str = "PG_18_202406281";
const TABLESPACE_CREATE_PRIVILEGE: char = 'C';
const TABLESPACE_ALL_PRIVILEGES: &str = "C";

fn tablespace_error_to_exec(error: pgrust_commands::tablespace::TablespaceError) -> ExecError {
    match error {
        pgrust_commands::tablespace::TablespaceError::Detailed {
            message,
            detail,
            hint,
            sqlstate,
        } => ExecError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        },
    }
}

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
        let normalized_location =
            normalize_tablespace_location(&stmt.location, allow_in_place_tablespaces)
                .map_err(tablespace_error_to_exec)?;
        let spcoptions =
            normalize_tablespace_options(&stmt.options).map_err(tablespace_error_to_exec)?;
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

        let owner_oid =
            resolve_role_spec_oid(self, client_id, Some((xid, cid)), stmt.owner.as_ref())?;
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
            .create_tablespace_mvcc(&stmt.tablespace_name, owner_oid, spcoptions, &ctx)
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

    pub(crate) fn execute_drop_tablespace_stmt(
        &self,
        client_id: ClientId,
        stmt: &DropTablespaceStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_drop_tablespace_stmt_in_transaction(
            client_id,
            stmt,
            xid,
            0,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_tablespace_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &DropTablespaceStatement,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), None);
        let Some(row) = catalog
            .tablespace_rows()
            .into_iter()
            .find(|row| row.spcname.eq_ignore_ascii_case(&stmt.tablespace_name))
        else {
            if stmt.if_exists {
                push_notice(format!(
                    "tablespace \"{}\" does not exist, skipping",
                    stmt.tablespace_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!("tablespace \"{}\" does not exist", stmt.tablespace_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };
        if matches!(row.oid, DEFAULT_TABLESPACE_OID | GLOBAL_TABLESPACE_OID) {
            return Err(ExecError::DetailedError {
                message: format!("tablespace \"{}\" cannot be dropped", row.spcname),
                detail: None,
                hint: None,
                sqlstate: "42809",
            });
        }
        ensure_tablespace_owner(self, client_id, Some((xid, cid)), &row)?;
        if let Some(dependent) = dependent_partitioned_relation_in_tablespace(&catalog, row.oid) {
            return Err(ExecError::DetailedError {
                message: format!(
                    "tablespace \"{}\" cannot be dropped because some objects depend on it",
                    row.spcname
                ),
                detail: Some(format!("tablespace for {dependent}")),
                hint: None,
                sqlstate: "2BP01",
            });
        }
        if relation_exists_in_tablespace(&catalog, row.oid) {
            return Err(ExecError::DetailedError {
                message: format!("tablespace \"{}\" is not empty", row.spcname),
                detail: None,
                hint: None,
                sqlstate: "55006",
            });
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
            .catalog
            .write()
            .drop_tablespace_mvcc(row.oid, &ctx)
            .map_err(map_catalog_error)?;
        remove_tablespace_directory(&self.cluster.base_dir, row.oid);
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_tablespace_stmt(
        &self,
        client_id: ClientId,
        stmt: &AlterTablespaceStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_tablespace_stmt_in_transaction(
            client_id,
            stmt,
            xid,
            0,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_tablespace_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &AlterTablespaceStatement,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catcache = self
            .backend_catcache(client_id, Some((xid, cid)))
            .map_err(map_catalog_error)?;
        let row = catcache
            .tablespace_rows()
            .into_iter()
            .find(|row| row.spcname.eq_ignore_ascii_case(&stmt.tablespace_name))
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("tablespace \"{}\" does not exist", stmt.tablespace_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        ensure_tablespace_owner(self, client_id, Some((xid, cid)), &row)?;
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid,
            client_id,
            waiter: None,
            interrupts: self.interrupt_state(client_id),
        };
        let effect = match &stmt.action {
            AlterTablespaceAction::SetOptions(options) => {
                let updated = merge_tablespace_options(row.spcoptions.clone(), options)
                    .map_err(tablespace_error_to_exec)?;
                self.catalog
                    .write()
                    .alter_tablespace_options_mvcc(row.oid, updated, &ctx)
            }
            AlterTablespaceAction::ResetOptions(names) => {
                let updated = reset_tablespace_options(row.spcoptions.clone(), names)
                    .map_err(tablespace_error_to_exec)?;
                self.catalog
                    .write()
                    .alter_tablespace_options_mvcc(row.oid, updated, &ctx)
            }
            AlterTablespaceAction::Rename { new_name } => self
                .catalog
                .write()
                .alter_tablespace_rename_mvcc(row.oid, new_name, &ctx),
            AlterTablespaceAction::OwnerTo { new_owner } => {
                let new_owner_oid =
                    resolve_role_spec_oid(self, client_id, Some((xid, cid)), Some(new_owner))?;
                self.catalog
                    .write()
                    .alter_tablespace_owner_mvcc(row.oid, new_owner_oid, &ctx)
            }
        }
        .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_move_all_tablespace_stmt(
        &self,
        client_id: ClientId,
        stmt: &AlterMoveAllTablespaceStatement,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_move_all_tablespace_stmt_in_transaction(
            client_id,
            stmt,
            xid,
            0,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_move_all_tablespace_stmt_in_transaction(
        &self,
        client_id: ClientId,
        stmt: &AlterMoveAllTablespaceStatement,
        xid: TransactionId,
        cid: CommandId,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let source_oid =
            tablespace_oid_by_name(self, client_id, Some((xid, cid)), &stmt.source_tablespace)?;
        let target_oid =
            tablespace_oid_by_name(self, client_id, Some((xid, cid)), &stmt.target_tablespace)?;
        ensure_non_global_relation_tablespace(&stmt.target_tablespace, target_oid, false)?;
        ensure_tablespace_create_privilege(self, client_id, Some((xid, cid)), target_oid)?;

        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), None);
        let mut relation_oids = catalog
            .class_rows()
            .into_iter()
            .filter(|row| row.reltablespace == source_oid)
            .filter(|row| match stmt.object_kind {
                MoveAllTablespaceObjectKind::Table => matches!(row.relkind, 'r' | 'p'),
                MoveAllTablespaceObjectKind::Index => matches!(row.relkind, 'i' | 'I'),
                MoveAllTablespaceObjectKind::MaterializedView => row.relkind == 'm',
            })
            .map(|row| row.oid)
            .collect::<Vec<_>>();
        relation_oids.sort_unstable();
        relation_oids.dedup();
        if relation_oids.is_empty() {
            push_notice(format!(
                "no matching relations in tablespace \"{}\" found",
                stmt.source_tablespace
            ));
            return Ok(StatementResult::AffectedRows(0));
        }

        for (offset, relation_oid) in relation_oids.into_iter().enumerate() {
            let move_cid = cid.saturating_add(offset as u32);
            let catalog = self.lazy_catalog_lookup(client_id, Some((xid, move_cid)), None);
            let Some(relation) = catalog.lookup_relation_by_oid(relation_oid) else {
                continue;
            };
            if relation.rel.spc_oid == target_oid {
                continue;
            }
            let ctx = CatalogWriteContext {
                pool: self.pool.clone(),
                txns: self.txns.clone(),
                xid,
                cid: move_cid,
                client_id,
                waiter: None,
                interrupts: self.interrupt_state(client_id),
            };
            let effect = self
                .catalog
                .write()
                .set_relation_tablespace_mvcc(relation.relation_oid, target_oid, true, &ctx)
                .map_err(map_catalog_error)?;
            for old_rel in &effect.dropped_rels {
                self.pool
                    .flush_relation(*old_rel)
                    .map_err(|err| ExecError::DetailedError {
                        message: format!(
                            "could not flush relation before tablespace move: {err:?}"
                        ),
                        detail: None,
                        hint: None,
                        sqlstate: "58030",
                    })?;
            }
            copy_rewritten_relation_storage(&self.cluster.base_dir, &effect)?;
            self.apply_catalog_mutation_effect_immediate(&effect)?;
            catalog_effects.push(effect);
        }
        Ok(StatementResult::AffectedRows(0))
    }
}

pub(super) fn resolve_relation_tablespace_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    explicit_tablespace: Option<&str>,
    gucs: Option<&HashMap<String, String>>,
) -> Result<u32, ExecError> {
    let effective_name = explicit_tablespace
        .filter(|name| !name.trim().is_empty())
        .or_else(|| {
            gucs.and_then(|gucs| gucs.get("default_tablespace"))
                .map(String::as_str)
                .filter(|name| !name.trim().is_empty())
        });
    let Some(name) = effective_name else {
        return Ok(0);
    };
    let tablespace_oid = tablespace_oid_by_name(db, client_id, txn_ctx, name)?;
    ensure_non_global_relation_tablespace(name, tablespace_oid, false)?;
    ensure_tablespace_create_privilege(db, client_id, txn_ctx, tablespace_oid)?;
    Ok(tablespace_oid)
}

pub(super) fn resolve_reindex_tablespace_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    tablespace: Option<&str>,
    concurrently: bool,
) -> Result<Option<u32>, ExecError> {
    let Some(name) = tablespace.filter(|name| !name.trim().is_empty()) else {
        return Ok(None);
    };
    let tablespace_oid = tablespace_oid_by_name(db, client_id, txn_ctx, name)?;
    ensure_non_global_relation_tablespace(name, tablespace_oid, concurrently)?;
    ensure_tablespace_create_privilege(db, client_id, txn_ctx, tablespace_oid)?;
    Ok(Some(tablespace_oid))
}

pub(super) fn tablespace_oid_by_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    tablespace_name: &str,
) -> Result<u32, ExecError> {
    if tablespace_name.eq_ignore_ascii_case("pg_default") {
        return Ok(0);
    }
    SearchSysCache1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::TABLESPACENAME,
        Value::Text(tablespace_name.to_ascii_lowercase().into()),
    )
    .map_err(map_catalog_error)?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Tablespace(row) if row.spcname.eq_ignore_ascii_case(tablespace_name) => {
            Some(row)
        }
        _ => None,
    })
    .map(|row| {
        if row.oid == DEFAULT_TABLESPACE_OID {
            0
        } else {
            row.oid
        }
    })
    .ok_or_else(|| ExecError::DetailedError {
        message: format!("tablespace \"{tablespace_name}\" does not exist"),
        detail: None,
        hint: None,
        sqlstate: "42704",
    })
}

pub(super) fn ensure_non_global_relation_tablespace(
    tablespace_name: &str,
    tablespace_oid: u32,
    concurrently: bool,
) -> Result<(), ExecError> {
    if tablespace_oid == GLOBAL_TABLESPACE_OID {
        return Err(ExecError::DetailedError {
            message: if concurrently {
                format!("cannot move non-shared relation to tablespace \"{tablespace_name}\"")
            } else {
                "only shared relations can be placed in pg_global tablespace".into()
            },
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    Ok(())
}

pub(super) fn ensure_tablespace_create_privilege(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    tablespace_oid: u32,
) -> Result<(), ExecError> {
    if tablespace_oid == 0 || tablespace_oid == DEFAULT_TABLESPACE_OID {
        return Ok(());
    }
    let Some(row) = SearchSysCache1(
        db,
        client_id,
        txn_ctx,
        SysCacheId::TABLESPACEOID,
        oid_key(tablespace_oid),
    )
    .map_err(map_catalog_error)?
    .into_iter()
    .find_map(|tuple| match tuple {
        SysCacheTuple::Tablespace(row) => Some(row),
        _ => None,
    }) else {
        return Ok(());
    };
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    if auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|role| role.rolsuper)
        || auth.has_effective_membership(row.spcowner, &auth_catalog)
    {
        return Ok(());
    }
    let effective_names = effective_acl_grantee_names(&auth, &auth_catalog);
    if row
        .spcacl
        .as_deref()
        .is_some_and(|acl| acl_grants_privilege(acl, &effective_names, TABLESPACE_CREATE_PRIVILEGE))
    {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("permission denied for tablespace {}", row.spcname),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

fn resolve_role_spec_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    role_spec: Option<&RoleSpec>,
) -> Result<u32, ExecError> {
    let auth = db.auth_state(client_id);
    match role_spec {
        None | Some(RoleSpec::CurrentUser) | Some(RoleSpec::CurrentRole) => {
            Ok(auth.current_user_oid())
        }
        Some(RoleSpec::SessionUser) => Ok(auth.session_user_oid()),
        Some(RoleSpec::RoleName(name)) => {
            let auth_catalog = db
                .auth_catalog(client_id, txn_ctx)
                .map_err(map_catalog_error)?;
            find_role_by_name(auth_catalog.roles(), name)
                .map(|row| row.oid)
                .ok_or_else(|| {
                    ExecError::Parse(ParseError::DetailedError {
                        message: format!("role \"{name}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "42704",
                    })
                })
        }
    }
}

fn ensure_tablespace_owner(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
    row: &PgTablespaceRow,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    if auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|role| role.rolsuper)
        || auth.has_effective_membership(row.spcowner, &auth_catalog)
    {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("must be owner of tablespace {}", row.spcname),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

fn dependent_partitioned_relation_in_tablespace(
    catalog: &dyn crate::backend::parser::CatalogLookup,
    tablespace_oid: u32,
) -> Option<String> {
    catalog
        .class_rows()
        .into_iter()
        .find(|row| row.reltablespace == tablespace_oid && matches!(row.relkind, 'p' | 'I'))
        .map(|row| {
            let kind = if row.relkind == 'I' { "index" } else { "table" };
            let namespace = catalog
                .namespace_row_by_oid(row.relnamespace)
                .map(|nsp| nsp.nspname)
                .unwrap_or_else(|| "public".into());
            format!("{kind} {namespace}.{}", row.relname)
        })
}

fn relation_exists_in_tablespace(
    catalog: &dyn crate::backend::parser::CatalogLookup,
    tablespace_oid: u32,
) -> bool {
    catalog
        .class_rows()
        .into_iter()
        .any(|row| row.reltablespace == tablespace_oid)
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

fn remove_tablespace_directory(base_dir: &Path, tablespace_oid: u32) {
    let path = base_dir.join("pg_tblspc").join(tablespace_oid.to_string());
    if let Ok(metadata) = fs::symlink_metadata(&path) {
        if metadata.file_type().is_symlink() {
            let _ = fs::remove_file(path);
        } else {
            let _ = fs::remove_dir_all(path);
        }
    }
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
