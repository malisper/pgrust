use super::super::*;
use crate::backend::executor::StatementResult;
use crate::backend::parser::{
    AlterForeignDataWrapperOwnerStatement, AlterForeignDataWrapperRenameStatement,
    AlterForeignDataWrapperStatement, AlterGenericOptionAction,
    CommentOnForeignDataWrapperStatement, CreateForeignDataWrapperStatement,
    DropForeignDataWrapperStatement, ParseError,
};
use crate::include::catalog::{BOOL_TYPE_OID, FDW_HANDLER_TYPE_OID, PgForeignDataWrapperRow};
use crate::pgrust::database::ddl::ensure_can_set_role;

fn normalize_foreign_data_wrapper_name(name: &str) -> Result<String, ParseError> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    if trimmed.contains('.') || trimmed.split_whitespace().count() != 1 {
        return Err(ParseError::UnsupportedQualifiedName(trimmed.to_string()));
    }
    Ok(trimmed.trim_matches('"').to_ascii_lowercase())
}

fn format_fdw_options(
    options: &[crate::backend::parser::RelOption],
) -> Result<Option<Vec<String>>, ExecError> {
    let mut names = std::collections::BTreeSet::new();
    let mut values = Vec::new();
    for option in options {
        let lowered = option.name.to_ascii_lowercase();
        if !names.insert(lowered.clone()) {
            return Err(ExecError::DetailedError {
                message: format!("option \"{}\" provided more than once", option.name),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }
        values.push(format!("{lowered}={}", option.value));
    }
    Ok((!values.is_empty()).then_some(values))
}

fn resolve_fdw_proc_oid(
    catalog: &dyn crate::backend::parser::CatalogLookup,
    name: &str,
    expected_return_type_oid: u32,
    expected_pronargs: i16,
    _object_label: &'static str,
) -> Result<u32, ExecError> {
    let normalized = normalize_foreign_data_wrapper_name(name).map_err(ExecError::Parse)?;
    let row = catalog
        .proc_rows_by_name(&normalized)
        .into_iter()
        .find(|row| {
            row.proname.eq_ignore_ascii_case(&normalized) && row.pronargs == expected_pronargs
        })
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("function {name} does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42883",
        })?;
    if expected_return_type_oid == FDW_HANDLER_TYPE_OID && row.prorettype != expected_return_type_oid
    {
        return Err(ExecError::DetailedError {
            message: format!("function {name} must return type fdw_handler"),
            detail: None,
            hint: None,
            sqlstate: "42809",
        });
    }
    Ok(row.oid)
}

fn ensure_current_user_is_superuser(
    db: &Database,
    client_id: ClientId,
    fdw_name: &str,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, None)
        .map_err(map_catalog_error)?;
    let is_superuser = auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|role| role.rolsuper);
    if is_superuser {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("permission denied to create foreign-data wrapper \"{fdw_name}\""),
        detail: None,
        hint: Some("Must be superuser to create a foreign-data wrapper.".into()),
        sqlstate: "42501",
    })
}

fn ensure_superuser_capability(
    db: &Database,
    client_id: ClientId,
    fdw_name: &str,
    action: &'static str,
) -> Result<(), ExecError> {
    ensure_current_user_is_superuser(db, client_id, fdw_name).map_err(|_| ExecError::DetailedError {
        message: format!("permission denied to {action} foreign-data wrapper \"{fdw_name}\""),
        detail: None,
        hint: Some(format!(
            "Must be superuser to {action} a foreign-data wrapper."
        )),
        sqlstate: "42501",
    })
}

fn lookup_foreign_data_wrapper(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    name: &str,
) -> Result<Option<PgForeignDataWrapperRow>, ExecError> {
    let normalized = normalize_foreign_data_wrapper_name(name).map_err(ExecError::Parse)?;
    Ok(db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .foreign_data_wrapper_rows()
        .into_iter()
        .find(|row| row.fdwname.eq_ignore_ascii_case(&normalized)))
}

fn ensure_fdw_owner(
    db: &Database,
    client_id: ClientId,
    owner_oid: u32,
    fdw_name: &str,
    txn_ctx: CatalogTxnContext,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    if auth.has_effective_membership(owner_oid, &auth_catalog) {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("must be owner of foreign-data wrapper {fdw_name}"),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

fn validate_fdw_options(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    fdwvalidator: u32,
    options: &[String],
) -> Result<(), ExecError> {
    if fdwvalidator == 0 || options.is_empty() {
        return Ok(());
    }
    let proc_name = db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .proc_by_oid(fdwvalidator)
        .map(|row| row.proname.clone())
        .unwrap_or_default();
    if proc_name.eq_ignore_ascii_case("postgresql_fdw_validator") {
        let invalid = options
            .iter()
            .find_map(|option| option.split_once('=').map(|(name, _)| name))
            .unwrap_or("unknown");
        return Err(ExecError::DetailedError {
            message: format!("invalid option \"{invalid}\""),
            detail: None,
            hint: Some("There are no valid options in this context.".into()),
            sqlstate: "42601",
        });
    }
    Ok(())
}

impl Database {
    pub(crate) fn execute_create_foreign_data_wrapper_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateForeignDataWrapperStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let _ = configured_search_path;
        ensure_current_user_is_superuser(self, client_id, &stmt.fdw_name)?;
        let normalized =
            normalize_foreign_data_wrapper_name(&stmt.fdw_name).map_err(ExecError::Parse)?;
        if lookup_foreign_data_wrapper(self, client_id, None, &normalized)?.is_some() {
            return Err(ExecError::DetailedError {
                message: format!("foreign-data wrapper \"{}\" already exists", stmt.fdw_name),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }
        let catalog = self.lazy_catalog_lookup(client_id, None, None);
        let fdwhandler = stmt
            .handler_name
            .as_deref()
            .map(|name| resolve_fdw_proc_oid(&catalog, name, FDW_HANDLER_TYPE_OID, 0, "handler"))
            .transpose()?
            .unwrap_or(0);
        let fdwvalidator = stmt
            .validator_name
            .as_deref()
            .map(|name| resolve_fdw_proc_oid(&catalog, name, BOOL_TYPE_OID, 2, "validator"))
            .transpose()?
            .unwrap_or(0);
        let options = format_fdw_options(&stmt.options)?;

        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: 0,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let mut catalog_effects = Vec::new();
        let row = PgForeignDataWrapperRow {
            oid: 0,
            fdwname: normalized,
            fdwowner: self.auth_state(client_id).current_user_oid(),
            fdwhandler,
            fdwvalidator,
            fdwacl: None,
            fdwoptions: options,
        };
        let (_, effect) = self
            .catalog
            .write()
            .create_foreign_data_wrapper_mvcc(row, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        let result = self.finish_txn(
            client_id,
            xid,
            Ok(StatementResult::AffectedRows(0)),
            &catalog_effects,
            &[],
            &[],
        );
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_foreign_data_wrapper_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterForeignDataWrapperStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let _ = configured_search_path;
        if stmt.handler_name.is_none() && stmt.validator_name.is_none() && stmt.options.is_empty() {
            return Err(ExecError::DetailedError {
                message: format!(
                    "foreign-data wrapper \"{}\" has no options to change",
                    stmt.fdw_name
                ),
                detail: None,
                hint: None,
                sqlstate: "42601",
            });
        }
        ensure_superuser_capability(self, client_id, &stmt.fdw_name, "alter")?;
        let existing = lookup_foreign_data_wrapper(self, client_id, None, &stmt.fdw_name)?
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("foreign-data wrapper \"{}\" does not exist", stmt.fdw_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        ensure_fdw_owner(self, client_id, existing.fdwowner, &stmt.fdw_name, None)?;
        let catalog = self.lazy_catalog_lookup(client_id, None, None);
        let fdwhandler = match &stmt.handler_name {
            Some(Some(name)) => {
                resolve_fdw_proc_oid(&catalog, name, FDW_HANDLER_TYPE_OID, 0, "handler")?
            }
            Some(None) => 0,
            None => existing.fdwhandler,
        };
        let fdwvalidator = match &stmt.validator_name {
            Some(Some(name)) => {
                resolve_fdw_proc_oid(&catalog, name, BOOL_TYPE_OID, 2, "validator")?
            }
            Some(None) => 0,
            None => existing.fdwvalidator,
        };
        let mut option_map = existing
            .fdwoptions
            .clone()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|option| {
                option
                    .split_once('=')
                    .map(|(name, value)| (name.to_string(), value.to_string()))
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        for option in &stmt.options {
            let key = option.name.to_ascii_lowercase();
            match option.action {
                AlterGenericOptionAction::Add => {
                    if option_map.contains_key(&key) {
                        return Err(ExecError::DetailedError {
                            message: format!("option \"{}\" provided more than once", option.name),
                            detail: None,
                            hint: None,
                            sqlstate: "42710",
                        });
                    }
                    option_map.insert(key, option.value.clone().unwrap_or_default());
                }
                AlterGenericOptionAction::Set => {
                    let Some(value) = option.value.clone() else {
                        continue;
                    };
                    let Some(slot) = option_map.get_mut(&key) else {
                        return Err(ExecError::DetailedError {
                            message: format!("option \"{}\" not found", option.name),
                            detail: None,
                            hint: None,
                            sqlstate: "42704",
                        });
                    };
                    *slot = value;
                }
                AlterGenericOptionAction::Drop => {
                    if option_map.remove(&key).is_none() {
                        return Err(ExecError::DetailedError {
                            message: format!("option \"{}\" not found", option.name),
                            detail: None,
                            hint: None,
                            sqlstate: "42704",
                        });
                    }
                }
            }
        }
        let options = (!option_map.is_empty()).then(|| {
            option_map
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect::<Vec<_>>()
        });
        validate_fdw_options(
            self,
            client_id,
            None,
            fdwvalidator,
            options.as_deref().unwrap_or(&[]),
        )?;
        let replacement = PgForeignDataWrapperRow {
            oid: existing.oid,
            fdwname: existing.fdwname.clone(),
            fdwowner: existing.fdwowner,
            fdwhandler,
            fdwvalidator,
            fdwacl: existing.fdwacl.clone(),
            fdwoptions: options,
        };

        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: 0,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let mut catalog_effects = Vec::new();
        let (_, effect) = self
            .catalog
            .write()
            .replace_foreign_data_wrapper_mvcc(&existing, replacement, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        let result = self.finish_txn(
            client_id,
            xid,
            Ok(StatementResult::AffectedRows(0)),
            &catalog_effects,
            &[],
            &[],
        );
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_foreign_data_wrapper_owner_stmt(
        &self,
        client_id: ClientId,
        stmt: &AlterForeignDataWrapperOwnerStatement,
    ) -> Result<StatementResult, ExecError> {
        let existing = lookup_foreign_data_wrapper(self, client_id, None, &stmt.fdw_name)?
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("foreign-data wrapper \"{}\" does not exist", stmt.fdw_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        ensure_fdw_owner(self, client_id, existing.fdwowner, &stmt.fdw_name, None)?;
        let auth_catalog = self
            .auth_catalog(client_id, None)
            .map_err(map_catalog_error)?;
        let new_owner = auth_catalog
            .role_by_name(&stmt.new_owner)
            .cloned()
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("role \"{}\" does not exist", stmt.new_owner),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        ensure_can_set_role(self, client_id, new_owner.oid, &new_owner.rolname)?;
        if !new_owner.rolsuper {
            return Err(ExecError::DetailedError {
                message: format!(
                    "permission denied to change owner of foreign-data wrapper \"{}\"",
                    stmt.fdw_name
                ),
                detail: None,
                hint: Some("The owner of a foreign-data wrapper must be a superuser.".into()),
                sqlstate: "42501",
            });
        }
        let replacement = PgForeignDataWrapperRow {
            fdwowner: new_owner.oid,
            ..existing.clone()
        };
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: 0,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let (_, effect) = self
            .catalog
            .write()
            .replace_foreign_data_wrapper_mvcc(&existing, replacement, &ctx)
            .map_err(map_catalog_error)?;
        let result = self.finish_txn(
            client_id,
            xid,
            Ok(StatementResult::AffectedRows(0)),
            &[effect],
            &[],
            &[],
        );
        guard.disarm();
        result
    }

    pub(crate) fn execute_alter_foreign_data_wrapper_rename_stmt(
        &self,
        client_id: ClientId,
        stmt: &AlterForeignDataWrapperRenameStatement,
    ) -> Result<StatementResult, ExecError> {
        let existing = lookup_foreign_data_wrapper(self, client_id, None, &stmt.fdw_name)?
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("foreign-data wrapper \"{}\" does not exist", stmt.fdw_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        ensure_fdw_owner(self, client_id, existing.fdwowner, &stmt.fdw_name, None)?;
        let new_name =
            normalize_foreign_data_wrapper_name(&stmt.new_name).map_err(ExecError::Parse)?;
        if lookup_foreign_data_wrapper(self, client_id, None, &new_name)?.is_some() {
            return Err(ExecError::DetailedError {
                message: format!("foreign-data wrapper \"{}\" already exists", stmt.new_name),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }
        let replacement = PgForeignDataWrapperRow {
            fdwname: new_name,
            ..existing.clone()
        };
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: 0,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let (_, effect) = self
            .catalog
            .write()
            .replace_foreign_data_wrapper_mvcc(&existing, replacement, &ctx)
            .map_err(map_catalog_error)?;
        let result = self.finish_txn(
            client_id,
            xid,
            Ok(StatementResult::AffectedRows(0)),
            &[effect],
            &[],
            &[],
        );
        guard.disarm();
        result
    }

    pub(crate) fn execute_drop_foreign_data_wrapper_stmt(
        &self,
        client_id: ClientId,
        stmt: &DropForeignDataWrapperStatement,
    ) -> Result<StatementResult, ExecError> {
        let Some(existing) = lookup_foreign_data_wrapper(self, client_id, None, &stmt.fdw_name)?
        else {
            if stmt.if_exists {
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!("foreign-data wrapper \"{}\" does not exist", stmt.fdw_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };
        ensure_fdw_owner(self, client_id, existing.fdwowner, &stmt.fdw_name, None)?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: 0,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .drop_foreign_data_wrapper_mvcc(&existing, &ctx)
            .map_err(map_catalog_error)?;
        let result = self.finish_txn(
            client_id,
            xid,
            Ok(StatementResult::AffectedRows(0)),
            &[effect],
            &[],
            &[],
        );
        guard.disarm();
        result
    }

    pub(crate) fn execute_comment_on_foreign_data_wrapper_stmt(
        &self,
        client_id: ClientId,
        stmt: &CommentOnForeignDataWrapperStatement,
    ) -> Result<StatementResult, ExecError> {
        let existing = lookup_foreign_data_wrapper(self, client_id, None, &stmt.fdw_name)?
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("foreign-data wrapper \"{}\" does not exist", stmt.fdw_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        ensure_fdw_owner(self, client_id, existing.fdwowner, &stmt.fdw_name, None)?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let ctx = CatalogWriteContext {
            pool: self.pool.clone(),
            txns: self.txns.clone(),
            xid,
            cid: 0,
            client_id,
            waiter: Some(self.txn_waiter.clone()),
            interrupts: self.interrupt_state(client_id),
        };
        let effect = self
            .catalog
            .write()
            .comment_foreign_data_wrapper_mvcc(existing.oid, stmt.comment.as_deref(), &ctx)
            .map_err(map_catalog_error)?;
        let result = self.finish_txn(
            client_id,
            xid,
            Ok(StatementResult::AffectedRows(0)),
            &[effect],
            &[],
            &[],
        );
        guard.disarm();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::ExecError;
    use crate::pgrust::database::Database;
    use crate::pgrust::session::Session;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "pgrust_fdw_cmds_{}_{}_{}",
            label,
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn create_fdw_permission_error_matches_postgres_text() {
        let base = temp_dir("create_permission");
        let db = Database::open(&base, 16).unwrap();
        let mut superuser = Session::new(1);
        superuser.execute(&db, "create role tenant").unwrap();

        let tenant_oid = db
            .backend_catcache(1, None)
            .unwrap()
            .authid_rows()
            .into_iter()
            .find(|row| row.rolname == "tenant")
            .unwrap()
            .oid;

        let mut tenant = Session::new(2);
        tenant.set_session_authorization_oid(tenant_oid);

        let err = tenant
            .execute(&db, "create foreign data wrapper tenant_fdw")
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                message, hint, sqlstate, ..
            } => {
                assert_eq!(
                    message,
                    "permission denied to create foreign-data wrapper \"tenant_fdw\""
                );
                assert_eq!(
                    hint.as_deref(),
                    Some("Must be superuser to create a foreign-data wrapper.")
                );
                assert_eq!(sqlstate, "42501");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }
    }

    #[test]
    fn fdw_option_and_owner_errors_match_postgres_text() {
        let base = temp_dir("option_and_owner_errors");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create foreign data wrapper fdw1")
            .unwrap();
        session.execute(&db, "create role target").unwrap();

        let missing_option = session
            .execute(&db, "alter foreign data wrapper fdw1 options (drop missing)")
            .unwrap_err();
        match missing_option {
            ExecError::DetailedError {
                message, sqlstate, ..
            } => {
                assert_eq!(message, "option \"missing\" not found");
                assert_eq!(sqlstate, "42704");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }

        let owner_err = session
            .execute(&db, "alter foreign data wrapper fdw1 owner to target")
            .unwrap_err();
        match owner_err {
            ExecError::DetailedError {
                message,
                hint,
                sqlstate,
                ..
            } => {
                assert_eq!(
                    message,
                    "permission denied to change owner of foreign-data wrapper \"fdw1\""
                );
                assert_eq!(
                    hint.as_deref(),
                    Some("The owner of a foreign-data wrapper must be a superuser.")
                );
                assert_eq!(sqlstate, "42501");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }
    }
}
