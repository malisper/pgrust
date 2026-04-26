use super::super::*;
use super::privilege::{acl_grants_privilege, effective_acl_grantee_names};
use crate::backend::executor::StatementResult;
use crate::backend::parser::{
    AlterForeignDataWrapperOwnerStatement, AlterForeignDataWrapperRenameStatement,
    AlterForeignDataWrapperStatement, AlterForeignServerOwnerStatement,
    AlterForeignServerRenameStatement, AlterForeignServerStatement,
    AlterForeignTableOptionsStatement, AlterGenericOptionAction, AlterUserMappingStatement,
    ColumnConstraint, CommentOnForeignDataWrapperStatement, CommentOnForeignServerStatement,
    CreateForeignDataWrapperStatement, CreateForeignServerStatement, CreateForeignTableStatement,
    CreateTableStatement, CreateUserMappingStatement, DropForeignDataWrapperStatement,
    DropForeignServerStatement, DropUserMappingStatement, ImportForeignSchemaStatement, ParseError,
    TableConstraint, UserMappingUser,
};
use crate::backend::utils::cache::catcache::CatCache;
use crate::backend::utils::misc::notices::{push_notice, push_notice_with_detail, push_warning};
use crate::include::catalog::{
    BOOL_TYPE_OID, FDW_HANDLER_TYPE_OID, OID_TYPE_OID, PgForeignDataWrapperRow, PgForeignServerRow,
    PgForeignTableRow, PgUserMappingRow, TEXT_ARRAY_TYPE_OID,
};
use crate::pgrust::database::ddl::{
    ensure_can_set_role, ensure_relation_owner, lookup_heap_relation_for_alter_table,
};

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

pub(super) fn format_fdw_options(
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

fn unsupported_foreign_table_constraint(kind: &'static str) -> ExecError {
    ExecError::DetailedError {
        message: format!("{kind} constraints are not supported on foreign tables"),
        detail: None,
        hint: None,
        sqlstate: "0A000",
    }
}

fn validate_foreign_table_constraints(stmt: &CreateTableStatement) -> Result<(), ExecError> {
    for column in stmt.columns() {
        for constraint in &column.constraints {
            match constraint {
                ColumnConstraint::PrimaryKey { .. } => {
                    return Err(unsupported_foreign_table_constraint("primary key"));
                }
                ColumnConstraint::Unique { .. } => {
                    return Err(unsupported_foreign_table_constraint("unique"));
                }
                ColumnConstraint::References { .. } => {
                    return Err(unsupported_foreign_table_constraint("foreign key"));
                }
                ColumnConstraint::NotNull { .. } | ColumnConstraint::Check { .. } => {}
            }
        }
    }
    for constraint in stmt.constraints() {
        match constraint {
            TableConstraint::PrimaryKey { .. } | TableConstraint::PrimaryKeyUsingIndex { .. } => {
                return Err(unsupported_foreign_table_constraint("primary key"));
            }
            TableConstraint::Unique { .. } | TableConstraint::UniqueUsingIndex { .. } => {
                return Err(unsupported_foreign_table_constraint("unique"));
            }
            TableConstraint::ForeignKey { .. } => {
                return Err(unsupported_foreign_table_constraint("foreign key"));
            }
            TableConstraint::Exclusion { .. } => {
                return Err(unsupported_foreign_table_constraint("exclusion"));
            }
            TableConstraint::NotNull { .. } | TableConstraint::Check { .. } => {}
        }
    }
    for override_ in stmt.partition_column_overrides() {
        for constraint in &override_.constraints {
            match constraint {
                ColumnConstraint::PrimaryKey { .. } => {
                    return Err(unsupported_foreign_table_constraint("primary key"));
                }
                ColumnConstraint::Unique { .. } => {
                    return Err(unsupported_foreign_table_constraint("unique"));
                }
                ColumnConstraint::References { .. } => {
                    return Err(unsupported_foreign_table_constraint("foreign key"));
                }
                ColumnConstraint::NotNull { .. } | ColumnConstraint::Check { .. } => {}
            }
        }
    }
    Ok(())
}

fn resolve_fdw_proc_oid(
    catalog: &dyn crate::backend::parser::CatalogLookup,
    name: &str,
    expected_return_type_oid: u32,
    expected_pronargs: i16,
    object_label: &'static str,
) -> Result<u32, ExecError> {
    let normalized = normalize_foreign_data_wrapper_name(name).map_err(ExecError::Parse)?;
    let expected_argtypes = match object_label {
        "validator" => format!("{TEXT_ARRAY_TYPE_OID} {OID_TYPE_OID}"),
        _ => String::new(),
    };
    let display_signature = match object_label {
        "validator" => format!("{name}(text[], oid)"),
        _ => format!("{name}()"),
    };
    let row = catalog
        .proc_rows_by_name(&normalized)
        .into_iter()
        .find(|row| {
            row.proname.eq_ignore_ascii_case(&normalized)
                && row.pronargs == expected_pronargs
                && row.proargtypes == expected_argtypes
        })
        .ok_or_else(|| ExecError::DetailedError {
            message: format!("function {display_signature} does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42883",
        })?;
    if expected_return_type_oid == FDW_HANDLER_TYPE_OID
        && row.prorettype != expected_return_type_oid
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
    ensure_current_user_is_superuser(db, client_id, fdw_name).map_err(|_| {
        ExecError::DetailedError {
            message: format!("permission denied to {action} foreign-data wrapper \"{fdw_name}\""),
            detail: None,
            hint: Some(format!(
                "Must be superuser to {action} a foreign-data wrapper."
            )),
            sqlstate: "42501",
        }
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

fn lookup_foreign_server(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    name: &str,
) -> Result<Option<PgForeignServerRow>, ExecError> {
    let normalized = normalize_foreign_data_wrapper_name(name).map_err(ExecError::Parse)?;
    Ok(db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .foreign_server_rows()
        .into_iter()
        .find(|row| row.srvname.eq_ignore_ascii_case(&normalized)))
}

fn lookup_user_mapping(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    user_oid: u32,
    server_oid: u32,
) -> Result<Option<PgUserMappingRow>, ExecError> {
    Ok(db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .user_mapping_row_by_user_server(user_oid, server_oid))
}

fn user_mapping_user_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    user: &UserMappingUser,
    missing_ok: bool,
) -> Result<Option<u32>, ExecError> {
    let auth = db.auth_state(client_id);
    match user {
        UserMappingUser::CurrentUser | UserMappingUser::User => Ok(Some(auth.current_user_oid())),
        UserMappingUser::Public => Ok(Some(0)),
        UserMappingUser::Role(name) => {
            let auth_catalog = db
                .auth_catalog(client_id, txn_ctx)
                .map_err(map_catalog_error)?;
            let normalized = normalize_foreign_data_wrapper_name(name).map_err(ExecError::Parse)?;
            match auth_catalog.role_by_name(&normalized) {
                Some(row) => Ok(Some(row.oid)),
                None if missing_ok => Ok(None),
                None => Err(ExecError::DetailedError {
                    message: format!("role \"{name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                }),
            }
        }
    }
}

fn user_mapping_user_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    user_oid: u32,
) -> Result<String, ExecError> {
    if user_oid == 0 {
        return Ok("public".into());
    }
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    Ok(auth_catalog
        .role_by_oid(user_oid)
        .map(|row| row.rolname.clone())
        .unwrap_or_else(|| user_oid.to_string()))
}

fn user_mapping_object_name(
    db: &Database,
    client_id: ClientId,
    mapping: &PgUserMappingRow,
    server_name: &str,
) -> Result<String, ExecError> {
    let user_name = user_mapping_user_name(db, client_id, None, mapping.umuser)?;
    Ok(format!(
        "user mapping for {user_name} on server {server_name}"
    ))
}

fn foreign_table_object_name(catcache: &CatCache, relation_oid: u32) -> String {
    let table_name = catcache
        .class_by_oid(relation_oid)
        .map(|row| row.relname.clone())
        .unwrap_or_else(|| relation_oid.to_string());
    format!("foreign table {table_name}")
}

fn push_drop_cascade_notices(notices: Vec<String>) {
    match notices.as_slice() {
        [] => {}
        [notice] => push_notice(notice.clone()),
        notices => push_notice_with_detail(
            format!("drop cascades to {} other objects", notices.len()),
            notices.join("\n"),
        ),
    }
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

fn ensure_foreign_server_owner(
    db: &Database,
    client_id: ClientId,
    owner_oid: u32,
    server_name: &str,
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
        message: format!("must be owner of foreign server {server_name}"),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

fn current_user_has_foreign_usage(
    db: &Database,
    client_id: ClientId,
    owner_oid: u32,
    acl: Option<&[String]>,
    txn_ctx: CatalogTxnContext,
) -> Result<bool, ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    if auth_catalog
        .role_by_oid(auth.current_user_oid())
        .is_some_and(|row| row.rolsuper)
        || auth.has_effective_membership(owner_oid, &auth_catalog)
    {
        return Ok(true);
    }
    let effective_names = effective_acl_grantee_names(&auth, &auth_catalog);
    Ok(acl
        .map(|acl| acl_grants_privilege(acl, &effective_names, 'U'))
        .unwrap_or(false))
}

fn ensure_foreign_data_wrapper_usage(
    db: &Database,
    client_id: ClientId,
    row: &PgForeignDataWrapperRow,
    txn_ctx: CatalogTxnContext,
) -> Result<(), ExecError> {
    if current_user_has_foreign_usage(db, client_id, row.fdwowner, row.fdwacl.as_deref(), txn_ctx)?
    {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("permission denied for foreign-data wrapper {}", row.fdwname),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

fn ensure_foreign_server_usage(
    db: &Database,
    client_id: ClientId,
    row: &PgForeignServerRow,
    txn_ctx: CatalogTxnContext,
) -> Result<(), ExecError> {
    if current_user_has_foreign_usage(db, client_id, row.srvowner, row.srvacl.as_deref(), txn_ctx)?
    {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("permission denied for foreign server {}", row.srvname),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

fn role_has_direct_foreign_usage(
    role: &crate::include::catalog::PgAuthIdRow,
    owner_oid: u32,
    acl: Option<&[String]>,
) -> bool {
    if role.rolsuper || role.oid == owner_oid {
        return true;
    }
    acl.unwrap_or_default().iter().any(|item| {
        let Some((grantee, rest)) = item.split_once('=') else {
            return false;
        };
        let Some((privileges, _)) = rest.split_once('/') else {
            return false;
        };
        (grantee.is_empty() || grantee == role.rolname) && privileges.contains('U')
    })
}

fn validate_postgresql_option(
    option_name: &str,
    valid: &[&str],
    context_hint: &str,
) -> Result<(), ExecError> {
    if valid.iter().any(|valid_name| option_name == *valid_name) {
        return Ok(());
    }
    let hint = if option_name == "username" && valid.contains(&"user") {
        Some("Perhaps you meant the option \"user\".".into())
    } else {
        Some(context_hint.into())
    };
    Err(ExecError::DetailedError {
        message: format!("invalid option \"{option_name}\""),
        detail: None,
        hint,
        sqlstate: "42601",
    })
}

fn fdw_validator_proc_name(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    fdwvalidator: u32,
) -> Result<Option<String>, ExecError> {
    if fdwvalidator == 0 {
        return Ok(None);
    }
    Ok(db
        .backend_catcache(client_id, txn_ctx)
        .map_err(map_catalog_error)?
        .proc_by_oid(fdwvalidator)
        .map(|row| row.proname.clone()))
}

fn validate_foreign_server_options(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    fdwvalidator: u32,
    options: &[String],
) -> Result<(), ExecError> {
    if !fdw_validator_proc_name(db, client_id, txn_ctx, fdwvalidator)?
        .as_deref()
        .is_some_and(|name| name.eq_ignore_ascii_case("postgresql_fdw_validator"))
    {
        return Ok(());
    }
    for option in options {
        if let Some((name, _)) = option.split_once('=') {
            validate_postgresql_option(
                name,
                &[
                    "host",
                    "hostaddr",
                    "port",
                    "dbname",
                    "connect_timeout",
                    "service",
                ],
                "Valid options in this context are: service, connect_timeout, dbname, host, hostaddr, and port.",
            )?;
        }
    }
    Ok(())
}

fn validate_user_mapping_options(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    fdwvalidator: u32,
    options: &[String],
) -> Result<(), ExecError> {
    if !fdw_validator_proc_name(db, client_id, txn_ctx, fdwvalidator)?
        .as_deref()
        .is_some_and(|name| name.eq_ignore_ascii_case("postgresql_fdw_validator"))
    {
        return Ok(());
    }
    for option in options {
        if let Some((name, _)) = option.split_once('=') {
            validate_postgresql_option(
                name,
                &["user", "password", "sslpassword"],
                "Valid options in this context are: user, password, and sslpassword.",
            )?;
        }
    }
    Ok(())
}

pub(super) fn alter_option_map(
    existing_options: Option<Vec<String>>,
    options: &[crate::backend::parser::AlterGenericOption],
) -> Result<Option<Vec<String>>, ExecError> {
    let mut option_list = existing_options
        .unwrap_or_default()
        .into_iter()
        .filter_map(|option| {
            option
                .split_once('=')
                .map(|(name, value)| (name.to_string(), value.to_string()))
        })
        .collect::<Vec<_>>();
    for option in options {
        let key = option.name.to_ascii_lowercase();
        match option.action {
            AlterGenericOptionAction::Add => {
                if option_list.iter().any(|(name, _)| name == &key) {
                    return Err(ExecError::DetailedError {
                        message: format!("option \"{}\" provided more than once", option.name),
                        detail: None,
                        hint: None,
                        sqlstate: "42710",
                    });
                }
                option_list.push((key, option.value.clone().unwrap_or_default()));
            }
            AlterGenericOptionAction::Set => {
                let Some(value) = option.value.clone() else {
                    continue;
                };
                let Some((_, slot)) = option_list.iter_mut().find(|(name, _)| name == &key) else {
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
                let Some(index) = option_list.iter().position(|(name, _)| name == &key) else {
                    return Err(ExecError::DetailedError {
                        message: format!("option \"{}\" not found", option.name),
                        detail: None,
                        hint: None,
                        sqlstate: "42704",
                    });
                };
                option_list.remove(index);
            }
        }
    }
    Ok((!option_list.is_empty()).then(|| {
        option_list
            .into_iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
    }))
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
    pub(crate) fn execute_create_foreign_server_stmt(
        &self,
        client_id: ClientId,
        stmt: &CreateForeignServerStatement,
    ) -> Result<StatementResult, ExecError> {
        let normalized =
            normalize_foreign_data_wrapper_name(&stmt.fdw_name).map_err(ExecError::Parse)?;
        let fdw =
            lookup_foreign_data_wrapper(self, client_id, None, &normalized)?.ok_or_else(|| {
                ExecError::DetailedError {
                    message: format!("foreign-data wrapper \"{}\" does not exist", stmt.fdw_name),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                }
            })?;
        let server_name =
            normalize_foreign_data_wrapper_name(&stmt.server_name).map_err(ExecError::Parse)?;
        if lookup_foreign_server(self, client_id, None, &server_name)?.is_some() {
            if stmt.if_not_exists {
                push_notice(format!(
                    "server \"{}\" already exists, skipping",
                    stmt.server_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!("server \"{}\" already exists", stmt.server_name),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }
        ensure_foreign_data_wrapper_usage(self, client_id, &fdw, None)?;
        let options = format_fdw_options(&stmt.options)?;
        validate_foreign_server_options(
            self,
            client_id,
            None,
            fdw.fdwvalidator,
            options.as_deref().unwrap_or(&[]),
        )?;
        let row = PgForeignServerRow {
            oid: 0,
            srvname: server_name,
            srvowner: self.auth_state(client_id).current_user_oid(),
            srvfdw: fdw.oid,
            srvtype: stmt.server_type.clone(),
            srvversion: stmt.version.clone(),
            srvacl: None,
            srvoptions: options,
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
            .create_foreign_server_mvcc(row, &ctx)
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

    pub(crate) fn execute_create_foreign_table_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &CreateForeignTableStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let create_stmt = &stmt.create_table;
        let (table_name, namespace_oid, persistence) = self
            .normalize_create_table_stmt_with_search_path(
                client_id,
                Some((xid, cid)),
                create_stmt,
                configured_search_path,
            )?;
        if persistence != TablePersistence::Permanent {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "permanent foreign table",
                actual: "temporary table".into(),
            }));
        }
        validate_foreign_table_constraints(create_stmt)?;
        let server_name =
            normalize_foreign_data_wrapper_name(&stmt.server_name).map_err(ExecError::Parse)?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let server = lookup_foreign_server(self, client_id, Some((xid, cid)), &server_name)?
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("server \"{}\" does not exist", stmt.server_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        ensure_foreign_server_usage(self, client_id, &server, Some((xid, cid)))?;
        let mut lowered = lower_create_table_with_catalog(create_stmt, &catalog, persistence)?;
        let column_options = stmt
            .column_options
            .iter()
            .map(|(column_name, options)| {
                format_fdw_options(options).map(|options| (column_name.clone(), options))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (column_name, options) in &column_options {
            let column = lowered
                .relation_desc
                .columns
                .iter_mut()
                .find(|column| column.name.eq_ignore_ascii_case(column_name))
                .ok_or_else(|| ExecError::Parse(ParseError::UnknownColumn(column_name.clone())))?;
            column.fdw_options = options.clone();
        }
        if create_stmt.if_not_exists
            && catalog
                .lookup_any_relation(&table_name)
                .is_some_and(|relation| relation.namespace_oid == namespace_oid)
        {
            return Ok(StatementResult::AffectedRows(0));
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
        let (entry, effect) = self
            .catalog
            .write()
            .create_relation_mvcc_with_relkind(
                table_name.clone(),
                lowered.relation_desc.clone(),
                namespace_oid,
                self.database_oid,
                'p',
                'f',
                self.auth_state(client_id).current_user_oid(),
                None,
                &ctx,
            )
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        let relation = crate::backend::parser::BoundRelation {
            rel: entry.rel,
            relation_oid: entry.relation_oid,
            toast: None,
            namespace_oid: entry.namespace_oid,
            owner_oid: entry.owner_oid,
            of_type_oid: entry.of_type_oid,
            relpersistence: entry.relpersistence,
            relkind: entry.relkind,
            relispopulated: entry.relispopulated,
            relispartition: entry.relispartition,
            relpartbound: entry.relpartbound.clone(),
            desc: entry.desc.clone(),
            partitioned_table: entry.partitioned_table.clone(),
        };
        self.install_create_table_constraints_in_transaction(
            client_id,
            xid,
            cid.saturating_add(1),
            &table_name,
            &relation,
            &lowered,
            configured_search_path,
            catalog_effects,
        )?;
        let options = format_fdw_options(&stmt.options)?;
        let foreign_table_row = PgForeignTableRow {
            ftrelid: entry.relation_oid,
            ftserver: server.oid,
            ftoptions: options,
        };
        let effect = self
            .catalog
            .write()
            .create_foreign_table_mvcc(foreign_table_row, &ctx)
            .map_err(map_catalog_error)?;
        self.apply_catalog_mutation_effect_immediate(&effect)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_alter_foreign_table_options_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterForeignTableOptionsStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let interrupts = self.interrupt_state(client_id);
        let catalog = self.lazy_catalog_lookup(client_id, None, configured_search_path);
        let Some(relation) =
            lookup_heap_relation_for_alter_table(&catalog, &stmt.table_name, stmt.if_exists)?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        if relation.relkind != 'f' {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: stmt.table_name.clone(),
                expected: "foreign table",
            }));
        }
        self.table_locks.lock_table_interruptible(
            relation.rel,
            TableLockMode::AccessExclusive,
            client_id,
            interrupts.as_ref(),
        )?;
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_foreign_table_options_stmt_in_transaction_with_search_path(
            client_id,
            stmt,
            xid,
            0,
            configured_search_path,
            &mut catalog_effects,
        );
        let result = self.finish_txn(client_id, xid, result, &catalog_effects, &[], &[]);
        guard.disarm();
        self.table_locks.unlock_table(relation.rel, client_id);
        result
    }

    pub(crate) fn execute_alter_foreign_table_options_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterForeignTableOptionsStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        let Some(relation) =
            lookup_heap_relation_for_alter_table(&catalog, &stmt.table_name, stmt.if_exists)?
        else {
            return Ok(StatementResult::AffectedRows(0));
        };
        if relation.relkind != 'f' {
            return Err(ExecError::Parse(ParseError::WrongObjectType {
                name: stmt.table_name.clone(),
                expected: "foreign table",
            }));
        }
        let _ = stmt.only;
        ensure_relation_owner(self, client_id, &relation, &stmt.table_name)?;
        let existing = catalog
            .foreign_table_rows()
            .into_iter()
            .find(|row| row.ftrelid == relation.relation_oid)
            .ok_or_else(|| ExecError::DetailedError {
                message: format!(
                    "foreign table \"{}\" is missing catalog row",
                    stmt.table_name
                ),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        let ftoptions = alter_option_map(existing.ftoptions.clone(), &stmt.options)?;
        let replacement = PgForeignTableRow {
            ftoptions,
            ..existing.clone()
        };
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
            .replace_foreign_table_mvcc(&existing, replacement, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }

    pub(crate) fn execute_import_foreign_schema_stmt(
        &self,
        client_id: ClientId,
        stmt: &ImportForeignSchemaStatement,
    ) -> Result<StatementResult, ExecError> {
        let server =
            lookup_foreign_server(self, client_id, None, &stmt.server_name)?.ok_or_else(|| {
                ExecError::DetailedError {
                    message: format!("server \"{}\" does not exist", stmt.server_name),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                }
            })?;
        ensure_foreign_server_usage(self, client_id, &server, None)?;
        let fdw = self
            .backend_catcache(client_id, None)
            .map_err(map_catalog_error)?
            .foreign_data_wrapper_rows()
            .into_iter()
            .find(|row| row.oid == server.srvfdw)
            .ok_or_else(|| ExecError::DetailedError {
                message: "catalog error".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        if fdw.fdwhandler == 0 {
            return Err(ExecError::DetailedError {
                message: format!("foreign-data wrapper \"{}\" has no handler", fdw.fdwname),
                detail: None,
                hint: None,
                sqlstate: "HV00N",
            });
        }
        let _ = (
            &stmt.remote_schema,
            &stmt.restriction,
            &stmt.local_schema,
            &stmt.options,
        );
        Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "IMPORT FOREIGN SCHEMA".into(),
        )))
    }

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

    pub(crate) fn execute_alter_foreign_server_stmt(
        &self,
        client_id: ClientId,
        stmt: &AlterForeignServerStatement,
    ) -> Result<StatementResult, ExecError> {
        if stmt.version.is_none() && stmt.options.is_empty() {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "VERSION or OPTIONS",
                actual: "end of statement".into(),
            }));
        }
        let existing = lookup_foreign_server(self, client_id, None, &stmt.server_name)?
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("server \"{}\" does not exist", stmt.server_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        ensure_foreign_server_owner(self, client_id, existing.srvowner, &stmt.server_name, None)?;
        let fdw = self
            .backend_catcache(client_id, None)
            .map_err(map_catalog_error)?
            .foreign_data_wrapper_rows()
            .into_iter()
            .find(|row| row.oid == existing.srvfdw)
            .ok_or_else(|| ExecError::DetailedError {
                message: "catalog error".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        let options = alter_option_map(existing.srvoptions.clone(), &stmt.options)?;
        validate_foreign_server_options(
            self,
            client_id,
            None,
            fdw.fdwvalidator,
            options.as_deref().unwrap_or(&[]),
        )?;
        let replacement = PgForeignServerRow {
            srvversion: stmt.version.clone().unwrap_or(existing.srvversion.clone()),
            srvoptions: options,
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
            .replace_foreign_server_mvcc(&existing, replacement, &ctx)
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

    pub(crate) fn execute_alter_foreign_server_owner_stmt(
        &self,
        client_id: ClientId,
        stmt: &AlterForeignServerOwnerStatement,
    ) -> Result<StatementResult, ExecError> {
        let existing = lookup_foreign_server(self, client_id, None, &stmt.server_name)?
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("server \"{}\" does not exist", stmt.server_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
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
        let current_is_superuser = auth_catalog
            .role_by_oid(self.auth_state(client_id).current_user_oid())
            .is_some_and(|row| row.rolsuper);
        if !current_is_superuser {
            ensure_foreign_server_owner(
                self,
                client_id,
                existing.srvowner,
                &stmt.server_name,
                None,
            )?;
            ensure_can_set_role(self, client_id, new_owner.oid, &new_owner.rolname)?;
            let fdw = self
                .backend_catcache(client_id, None)
                .map_err(map_catalog_error)?
                .foreign_data_wrapper_rows()
                .into_iter()
                .find(|row| row.oid == existing.srvfdw)
                .ok_or_else(|| ExecError::DetailedError {
                    message: "catalog error".into(),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                })?;
            if !role_has_direct_foreign_usage(&new_owner, fdw.fdwowner, fdw.fdwacl.as_deref()) {
                return Err(ExecError::DetailedError {
                    message: format!("permission denied for foreign-data wrapper {}", fdw.fdwname),
                    detail: None,
                    hint: None,
                    sqlstate: "42501",
                });
            }
        }
        let replacement = PgForeignServerRow {
            srvowner: new_owner.oid,
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
            .replace_foreign_server_mvcc(&existing, replacement, &ctx)
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

    pub(crate) fn execute_alter_foreign_server_rename_stmt(
        &self,
        client_id: ClientId,
        stmt: &AlterForeignServerRenameStatement,
    ) -> Result<StatementResult, ExecError> {
        let existing = lookup_foreign_server(self, client_id, None, &stmt.server_name)?
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("server \"{}\" does not exist", stmt.server_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        ensure_foreign_server_owner(self, client_id, existing.srvowner, &stmt.server_name, None)?;
        let new_name =
            normalize_foreign_data_wrapper_name(&stmt.new_name).map_err(ExecError::Parse)?;
        if lookup_foreign_server(self, client_id, None, &new_name)?.is_some() {
            return Err(ExecError::DetailedError {
                message: format!("server \"{}\" already exists", stmt.new_name),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }
        let replacement = PgForeignServerRow {
            srvname: new_name,
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
            .replace_foreign_server_mvcc(&existing, replacement, &ctx)
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

    pub(crate) fn execute_drop_foreign_server_stmt(
        &self,
        client_id: ClientId,
        stmt: &DropForeignServerStatement,
    ) -> Result<StatementResult, ExecError> {
        let Some(existing) = lookup_foreign_server(self, client_id, None, &stmt.server_name)?
        else {
            if stmt.if_exists {
                push_notice(format!(
                    "server \"{}\" does not exist, skipping",
                    stmt.server_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!("server \"{}\" does not exist", stmt.server_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };
        ensure_foreign_server_owner(self, client_id, existing.srvowner, &stmt.server_name, None)?;
        let catcache = self
            .backend_catcache(client_id, None)
            .map_err(map_catalog_error)?;
        let dependent_mappings = catcache
            .user_mapping_rows()
            .into_iter()
            .filter(|row| row.umserver == existing.oid)
            .collect::<Vec<_>>();
        let dependent_tables = catcache
            .foreign_table_rows()
            .into_iter()
            .filter(|row| row.ftserver == existing.oid)
            .collect::<Vec<_>>();
        if !stmt.cascade && (!dependent_mappings.is_empty() || !dependent_tables.is_empty()) {
            if let Some(first) = dependent_mappings.first() {
                let username = user_mapping_user_name(self, client_id, None, first.umuser)?;
                return Err(ExecError::DetailedError {
                    message: format!(
                        "cannot drop server {} because other objects depend on it",
                        existing.srvname
                    ),
                    detail: Some(format!(
                        "user mapping for {username} on server {} depends on server {}",
                        existing.srvname, existing.srvname
                    )),
                    hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
                    sqlstate: "2BP01",
                });
            }
            let first = &dependent_tables[0];
            let table_name = catcache
                .class_by_oid(first.ftrelid)
                .map(|row| row.relname.clone())
                .unwrap_or_else(|| first.ftrelid.to_string());
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot drop server {} because other objects depend on it",
                    existing.srvname
                ),
                detail: Some(format!(
                    "foreign table {table_name} depends on server {}",
                    existing.srvname
                )),
                hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
                sqlstate: "2BP01",
            });
        }
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
        let mut effects = Vec::new();
        if stmt.cascade {
            let mut notices = Vec::new();
            for mapping in &dependent_mappings {
                notices.push(format!(
                    "drop cascades to {}",
                    user_mapping_object_name(self, client_id, mapping, &existing.srvname)?
                ));
            }
            for table in &dependent_tables {
                notices.push(format!(
                    "drop cascades to {}",
                    foreign_table_object_name(&catcache, table.ftrelid)
                ));
            }
            push_drop_cascade_notices(notices);

            for table in dependent_tables {
                let (_, effect) = self
                    .catalog
                    .write()
                    .drop_relation_by_oid_mvcc(table.ftrelid, &ctx)
                    .map_err(map_catalog_error)?;
                effects.push(effect);
            }
            for mapping in dependent_mappings {
                let effect = self
                    .catalog
                    .write()
                    .drop_user_mapping_mvcc(&mapping, &ctx)
                    .map_err(map_catalog_error)?;
                effects.push(effect);
            }
        }
        let effect = self
            .catalog
            .write()
            .drop_foreign_server_mvcc(&existing, &ctx)
            .map_err(map_catalog_error)?;
        effects.push(effect);
        let result = self.finish_txn(
            client_id,
            xid,
            Ok(StatementResult::AffectedRows(0)),
            &effects,
            &[],
            &[],
        );
        guard.disarm();
        result
    }

    pub(crate) fn execute_create_user_mapping_stmt(
        &self,
        client_id: ClientId,
        stmt: &CreateUserMappingStatement,
    ) -> Result<StatementResult, ExecError> {
        let user_oid =
            user_mapping_user_oid(self, client_id, None, &stmt.user, false)?.unwrap_or(0);
        let server =
            lookup_foreign_server(self, client_id, None, &stmt.server_name)?.ok_or_else(|| {
                ExecError::DetailedError {
                    message: format!("server \"{}\" does not exist", stmt.server_name),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                }
            })?;
        if lookup_user_mapping(self, client_id, None, user_oid, server.oid)?.is_some() {
            let user_name = user_mapping_user_name(self, client_id, None, user_oid)?;
            if stmt.if_not_exists {
                push_notice(format!(
                    "user mapping for \"{user_name}\" already exists for server \"{}\", skipping",
                    stmt.server_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!(
                    "user mapping for \"{user_name}\" already exists for server \"{}\"",
                    stmt.server_name
                ),
                detail: None,
                hint: None,
                sqlstate: "42710",
            });
        }
        if user_oid == self.auth_state(client_id).current_user_oid() {
            ensure_foreign_server_usage(self, client_id, &server, None)?;
        } else {
            ensure_foreign_server_owner(self, client_id, server.srvowner, &stmt.server_name, None)?;
        }
        let catcache = self
            .backend_catcache(client_id, None)
            .map_err(map_catalog_error)?;
        let fdw = catcache
            .foreign_data_wrapper_rows()
            .into_iter()
            .find(|row| row.oid == server.srvfdw)
            .ok_or_else(|| ExecError::DetailedError {
                message: "catalog error".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        let options = format_fdw_options(&stmt.options)?;
        validate_user_mapping_options(
            self,
            client_id,
            None,
            fdw.fdwvalidator,
            options.as_deref().unwrap_or(&[]),
        )?;
        let row = PgUserMappingRow {
            oid: 0,
            umuser: user_oid,
            umserver: server.oid,
            umoptions: options,
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
            .create_user_mapping_mvcc(row, &ctx)
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

    pub(crate) fn execute_alter_user_mapping_stmt(
        &self,
        client_id: ClientId,
        stmt: &AlterUserMappingStatement,
    ) -> Result<StatementResult, ExecError> {
        let server =
            lookup_foreign_server(self, client_id, None, &stmt.server_name)?.ok_or_else(|| {
                ExecError::DetailedError {
                    message: format!("server \"{}\" does not exist", stmt.server_name),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                }
            })?;
        let user_oid =
            user_mapping_user_oid(self, client_id, None, &stmt.user, false)?.unwrap_or(0);
        let existing = lookup_user_mapping(self, client_id, None, user_oid, server.oid)?
            .ok_or_else(|| {
                let user_name =
                    user_mapping_user_name(self, client_id, None, user_oid).unwrap_or_default();
                ExecError::DetailedError {
                    message: format!(
                        "user mapping for \"{user_name}\" does not exist for server \"{}\"",
                        stmt.server_name
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "42704",
                }
            })?;
        ensure_foreign_server_owner(self, client_id, server.srvowner, &stmt.server_name, None)?;
        let catcache = self
            .backend_catcache(client_id, None)
            .map_err(map_catalog_error)?;
        let fdw = catcache
            .foreign_data_wrapper_rows()
            .into_iter()
            .find(|row| row.oid == server.srvfdw)
            .ok_or_else(|| ExecError::DetailedError {
                message: "catalog error".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            })?;
        let options = alter_option_map(existing.umoptions.clone(), &stmt.options)?;
        validate_user_mapping_options(
            self,
            client_id,
            None,
            fdw.fdwvalidator,
            options.as_deref().unwrap_or(&[]),
        )?;
        let replacement = PgUserMappingRow {
            umoptions: options,
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
            .replace_user_mapping_mvcc(&existing, replacement, &ctx)
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

    pub(crate) fn execute_drop_user_mapping_stmt(
        &self,
        client_id: ClientId,
        stmt: &DropUserMappingStatement,
    ) -> Result<StatementResult, ExecError> {
        let Some(server) = lookup_foreign_server(self, client_id, None, &stmt.server_name)? else {
            if stmt.if_exists {
                push_notice(format!(
                    "server \"{}\" does not exist, skipping",
                    stmt.server_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!("server \"{}\" does not exist", stmt.server_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };
        let Some(user_oid) =
            user_mapping_user_oid(self, client_id, None, &stmt.user, stmt.if_exists)?
        else {
            if stmt.if_exists {
                if let UserMappingUser::Role(role_name) = &stmt.user {
                    push_notice(format!("role \"{role_name}\" does not exist, skipping"));
                }
            }
            return Ok(StatementResult::AffectedRows(0));
        };
        let Some(existing) = lookup_user_mapping(self, client_id, None, user_oid, server.oid)?
        else {
            let user_name = user_mapping_user_name(self, client_id, None, user_oid)?;
            if stmt.if_exists {
                push_notice(format!(
                    "user mapping for \"{user_name}\" does not exist for server \"{}\", skipping",
                    stmt.server_name
                ));
                return Ok(StatementResult::AffectedRows(0));
            }
            return Err(ExecError::DetailedError {
                message: format!(
                    "user mapping for \"{user_name}\" does not exist for server \"{}\"",
                    stmt.server_name
                ),
                detail: None,
                hint: None,
                sqlstate: "42704",
            });
        };
        ensure_foreign_server_owner(self, client_id, server.srvowner, &stmt.server_name, None)?;
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
            .drop_user_mapping_mvcc(&existing, &ctx)
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
        let options = alter_option_map(existing.fdwoptions.clone(), &stmt.options)?;
        if !stmt.options.is_empty() {
            validate_fdw_options(
                self,
                client_id,
                None,
                fdwvalidator,
                options.as_deref().unwrap_or(&[]),
            )?;
        }
        if stmt.handler_name.is_some() && fdwhandler != existing.fdwhandler {
            push_warning(
                "changing the foreign-data wrapper handler can change behavior of existing foreign tables",
            );
        }
        if stmt.validator_name.is_some() && fdwvalidator != existing.fdwvalidator {
            push_warning(
                "changing the foreign-data wrapper validator can cause the options for dependent objects to become invalid",
            );
        }
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
                push_notice(format!(
                    "foreign-data wrapper \"{}\" does not exist, skipping",
                    stmt.fdw_name
                ));
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
        let catcache = self
            .backend_catcache(client_id, None)
            .map_err(map_catalog_error)?;
        let dependent_servers = catcache
            .foreign_server_rows()
            .into_iter()
            .filter(|row| row.srvfdw == existing.oid)
            .collect::<Vec<_>>();
        let dependent_server_oids = dependent_servers
            .iter()
            .map(|row| row.oid)
            .collect::<std::collections::BTreeSet<_>>();
        let dependent_tables = catcache
            .foreign_table_rows()
            .into_iter()
            .filter(|row| dependent_server_oids.contains(&row.ftserver))
            .collect::<Vec<_>>();
        let dependent_mappings = catcache
            .user_mapping_rows()
            .into_iter()
            .filter(|row| dependent_server_oids.contains(&row.umserver))
            .collect::<Vec<_>>();
        if !stmt.cascade && !dependent_servers.is_empty() {
            let mut detail_lines = Vec::new();
            for server in &dependent_servers {
                detail_lines.push(format!(
                    "server {} depends on foreign-data wrapper {}",
                    server.srvname, existing.fdwname
                ));
            }
            for mapping in &dependent_mappings {
                let server_name = dependent_servers
                    .iter()
                    .find(|server| server.oid == mapping.umserver)
                    .map(|server| server.srvname.as_str())
                    .unwrap_or("<unknown>");
                detail_lines.push(format!(
                    "{} depends on server {server_name}",
                    user_mapping_object_name(self, client_id, mapping, server_name)?
                ));
            }
            for table in &dependent_tables {
                let server_name = dependent_servers
                    .iter()
                    .find(|server| server.oid == table.ftserver)
                    .map(|server| server.srvname.as_str())
                    .unwrap_or("<unknown>");
                detail_lines.push(format!(
                    "{} depends on server {server_name}",
                    foreign_table_object_name(&catcache, table.ftrelid)
                ));
            }
            return Err(ExecError::DetailedError {
                message: format!(
                    "cannot drop foreign-data wrapper {} because other objects depend on it",
                    existing.fdwname
                ),
                detail: Some(detail_lines.join("\n")),
                hint: Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
                sqlstate: "2BP01",
            });
        }
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
        let mut effects = Vec::new();
        if stmt.cascade {
            let mut notices = Vec::new();
            for server in &dependent_servers {
                notices.push(format!("drop cascades to server {}", server.srvname));
            }
            for mapping in &dependent_mappings {
                let server_name = dependent_servers
                    .iter()
                    .find(|server| server.oid == mapping.umserver)
                    .map(|server| server.srvname.as_str())
                    .unwrap_or("<unknown>");
                notices.push(format!(
                    "drop cascades to {}",
                    user_mapping_object_name(self, client_id, mapping, server_name)?
                ));
            }
            for table in &dependent_tables {
                notices.push(format!(
                    "drop cascades to {}",
                    foreign_table_object_name(&catcache, table.ftrelid)
                ));
            }
            push_drop_cascade_notices(notices);

            for table in dependent_tables {
                let (_, effect) = self
                    .catalog
                    .write()
                    .drop_relation_by_oid_mvcc(table.ftrelid, &ctx)
                    .map_err(map_catalog_error)?;
                effects.push(effect);
            }
            for mapping in dependent_mappings {
                let effect = self
                    .catalog
                    .write()
                    .drop_user_mapping_mvcc(&mapping, &ctx)
                    .map_err(map_catalog_error)?;
                effects.push(effect);
            }
            for server in dependent_servers {
                let effect = self
                    .catalog
                    .write()
                    .drop_foreign_server_mvcc(&server, &ctx)
                    .map_err(map_catalog_error)?;
                effects.push(effect);
            }
        }
        let effect = self
            .catalog
            .write()
            .drop_foreign_data_wrapper_mvcc(&existing, &ctx)
            .map_err(map_catalog_error)?;
        effects.push(effect);
        let result = self.finish_txn(
            client_id,
            xid,
            Ok(StatementResult::AffectedRows(0)),
            &effects,
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

    pub(crate) fn execute_comment_on_foreign_server_stmt(
        &self,
        client_id: ClientId,
        stmt: &CommentOnForeignServerStatement,
    ) -> Result<StatementResult, ExecError> {
        let existing = lookup_foreign_server(self, client_id, None, &stmt.server_name)?
            .ok_or_else(|| ExecError::DetailedError {
                message: format!("server \"{}\" does not exist", stmt.server_name),
                detail: None,
                hint: None,
                sqlstate: "42704",
            })?;
        ensure_foreign_server_owner(self, client_id, existing.srvowner, &stmt.server_name, None)?;
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
            .comment_foreign_server_mvcc(existing.oid, stmt.comment.as_deref(), &ctx)
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
    use crate::backend::executor::ExecError;
    use crate::backend::parser::ParseError;
    use crate::backend::utils::misc::notices::{
        clear_notices as clear_backend_notices, take_notices as take_backend_notices,
    };
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
                message,
                hint,
                sqlstate,
                ..
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
            .execute(
                &db,
                "alter foreign data wrapper fdw1 options (drop missing)",
            )
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

    #[test]
    fn fdw_function_lookup_errors_include_expected_signature() {
        let base = temp_dir("function_lookup_errors");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        let handler_err = session
            .execute(
                &db,
                "create foreign data wrapper bad handler missing_handler",
            )
            .unwrap_err();
        match handler_err {
            ExecError::DetailedError {
                message, sqlstate, ..
            } => {
                assert_eq!(message, "function missing_handler() does not exist");
                assert_eq!(sqlstate, "42883");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }

        let validator_err = session
            .execute(
                &db,
                "create foreign data wrapper bad validator missing_validator",
            )
            .unwrap_err();
        match validator_err {
            ExecError::DetailedError {
                message, sqlstate, ..
            } => {
                assert_eq!(
                    message,
                    "function missing_validator(text[], oid) does not exist"
                );
                assert_eq!(sqlstate, "42883");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }
    }

    #[test]
    fn alter_fdw_options_preserves_existing_order() {
        let base = temp_dir("option_order");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(
                &db,
                "create foreign data wrapper order_fdw options (b '1', c '2')",
            )
            .unwrap();
        session
            .execute(&db, "alter foreign data wrapper order_fdw options (a '0')")
            .unwrap();
        session
            .execute(
                &db,
                "alter foreign data wrapper order_fdw options (set b '3', add d '4')",
            )
            .unwrap();

        let row = db
            .backend_catcache(1, None)
            .unwrap()
            .foreign_data_wrapper_rows()
            .into_iter()
            .find(|row| row.fdwname == "order_fdw")
            .unwrap();
        assert_eq!(
            row.fdwoptions,
            Some(vec!["b=3".into(), "c=2".into(), "a=0".into(), "d=4".into()])
        );
    }

    #[test]
    fn create_user_mapping_reports_missing_role_before_missing_server() {
        let base = temp_dir("user_mapping_missing_role");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);

        let err = session
            .execute(
                &db,
                "create user mapping for missing_mapping_role server missing_mapping_server",
            )
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                message, sqlstate, ..
            } => {
                assert_eq!(message, r#"role "missing_mapping_role" does not exist"#);
                assert_eq!(sqlstate, "42704");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }
    }

    #[test]
    fn create_index_on_foreign_table_reports_unsupported_operation() {
        let base = temp_dir("foreign_table_index");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create foreign data wrapper idx_fdw")
            .unwrap();
        session
            .execute(&db, "create server idx_srv foreign data wrapper idx_fdw")
            .unwrap();
        session
            .execute(&db, "create foreign table idx_ft (a int4) server idx_srv")
            .unwrap();

        let err = session
            .execute(&db, "create index idx_ft_a_idx on idx_ft(a)")
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                message,
                detail,
                sqlstate,
                ..
            } => {
                assert_eq!(message, r#"cannot create index on relation "idx_ft""#);
                assert_eq!(
                    detail.as_deref(),
                    Some("This operation is not supported for foreign tables.")
                );
                assert_eq!(sqlstate, "42809");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }
    }

    #[test]
    fn scanning_foreign_table_requires_fdw_handler() {
        let base = temp_dir("foreign_table_handler");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create foreign data wrapper scan_fdw")
            .unwrap();
        session
            .execute(&db, "create server scan_srv foreign data wrapper scan_fdw")
            .unwrap();
        session
            .execute(&db, "create foreign table scan_ft (a int4) server scan_srv")
            .unwrap();

        for sql in ["select * from scan_ft", "explain select * from scan_ft"] {
            let err = session.execute(&db, sql).unwrap_err();
            match err {
                ExecError::Parse(ParseError::DetailedError {
                    message, sqlstate, ..
                }) => {
                    assert_eq!(message, r#"foreign-data wrapper "scan_fdw" has no handler"#);
                    assert_eq!(sqlstate, "HV00N");
                }
                other => panic!("expected missing handler error for {sql}, got {other:?}"),
            }
        }
    }

    #[test]
    fn drop_foreign_data_cascade_reports_dependent_objects() {
        let base = temp_dir("cascade_notices");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(&db, "create foreign data wrapper cascade_fdw")
            .unwrap();
        session
            .execute(
                &db,
                "create server cascade_srv foreign data wrapper cascade_fdw",
            )
            .unwrap();
        session
            .execute(&db, "create user mapping for public server cascade_srv")
            .unwrap();

        let err = session
            .execute(&db, "drop foreign data wrapper cascade_fdw")
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                detail: Some(detail),
                ..
            } => {
                assert_eq!(
                    detail,
                    "server cascade_srv depends on foreign-data wrapper cascade_fdw\n\
                     user mapping for public on server cascade_srv depends on server cascade_srv"
                );
            }
            other => panic!("expected dependency detail, got {other:?}"),
        }

        clear_backend_notices();
        session
            .execute(&db, "drop foreign data wrapper cascade_fdw cascade")
            .unwrap();
        let notices = take_backend_notices();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].message, "drop cascades to 2 other objects");
        assert_eq!(
            notices[0].detail.as_deref(),
            Some(
                "drop cascades to server cascade_srv\n\
                 drop cascades to user mapping for public on server cascade_srv"
            )
        );

        session
            .execute(&db, "create foreign data wrapper cascade_fdw2")
            .unwrap();
        session
            .execute(
                &db,
                "create server cascade_srv2 foreign data wrapper cascade_fdw2",
            )
            .unwrap();
        session
            .execute(&db, "create user mapping for public server cascade_srv2")
            .unwrap();
        clear_backend_notices();
        session
            .execute(&db, "drop server cascade_srv2 cascade")
            .unwrap();
        let notices = take_backend_notices();
        assert_eq!(notices.len(), 1);
        assert_eq!(
            notices[0].message,
            "drop cascades to user mapping for public on server cascade_srv2"
        );
        assert_eq!(notices[0].detail, None);
    }

    #[test]
    fn drop_function_reports_foreign_data_wrapper_dependency() {
        let base = temp_dir("function_dependency");
        let db = Database::open(&base, 16).unwrap();
        let mut session = Session::new(1);
        session
            .execute(
                &db,
                "create foreign data wrapper dep_fdw handler pg_rust_test_fdw_handler",
            )
            .unwrap();

        let err = session
            .execute(&db, "drop function pg_rust_test_fdw_handler()")
            .unwrap_err();
        match err {
            ExecError::DetailedError {
                message,
                detail,
                hint,
                sqlstate,
            } => {
                assert_eq!(
                    message,
                    "cannot drop function pg_rust_test_fdw_handler() because other objects depend on it"
                );
                assert_eq!(
                    detail.as_deref(),
                    Some(
                        "foreign-data wrapper dep_fdw depends on function pg_rust_test_fdw_handler()"
                    )
                );
                assert_eq!(
                    hint.as_deref(),
                    Some("Use DROP ... CASCADE to drop the dependent objects too.")
                );
                assert_eq!(sqlstate, "2BP01");
            }
            other => panic!("expected detailed error, got {other:?}"),
        }
    }
}
