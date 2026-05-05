use super::super::*;
use super::privilege::{routine_kind_matches, routine_kind_name};
use crate::backend::catalog::store::{CatalogMutationEffect, CatalogWriteContext};
use crate::backend::executor::expr_reg::format_type_text;
use crate::backend::parser::{
    AlterRoutineAction, AlterRoutineOption, AlterRoutineStatement, CatalogLookup, FunctionParallel,
    FunctionVolatility, ParseError, RoutineKind, RoutineSignature, parse_type_name,
    resolve_raw_type_name,
};
use crate::backend::utils::misc::guc::normalize_guc_name;
use crate::include::catalog::{BOOTSTRAP_SUPERUSER_OID, INTERNAL_TYPE_OID, PgProcRow};
use crate::pgrust::database::ddl::ensure_can_set_role;

fn normalize_ident(name: &str) -> String {
    name.trim().trim_matches('"').to_ascii_lowercase()
}

fn set_proc_config(row: &mut PgProcRow, name: &str, value: &str) {
    let normalized = normalize_guc_name(name);
    let config = row.proconfig.get_or_insert_with(Vec::new);
    config.retain(|entry| {
        entry
            .split_once('=')
            .map(|(entry_name, _)| !entry_name.eq_ignore_ascii_case(&normalized))
            .unwrap_or(true)
    });
    config.push(format!("{normalized}={value}"));
}

fn reset_proc_config(row: &mut PgProcRow, name: &str) {
    let normalized = normalize_guc_name(name);
    if let Some(config) = row.proconfig.as_mut() {
        config.retain(|entry| {
            entry
                .split_once('=')
                .map(|(entry_name, _)| !entry_name.eq_ignore_ascii_case(&normalized))
                .unwrap_or(true)
        });
        if config.is_empty() {
            row.proconfig = None;
        }
    }
}

fn current_user_is_superuser(catalog: &dyn CatalogLookup) -> bool {
    let current_user_oid = catalog.current_user_oid();
    catalog
        .authid_rows()
        .into_iter()
        .find(|row| row.oid == current_user_oid)
        .map(|row| row.rolsuper)
        .unwrap_or(current_user_oid == BOOTSTRAP_SUPERUSER_OID)
}

fn routine_arg_type_oid(
    catalog: &dyn CatalogLookup,
    arg: &str,
) -> Result<(Option<u8>, u32), ExecError> {
    let mut text = arg.trim();
    let lower = text.to_ascii_lowercase();
    let mut mode = None;
    for (keyword, code) in [
        ("inout", b'b'),
        ("variadic", b'v'),
        ("in", b'i'),
        ("out", b'o'),
    ] {
        if lower == keyword || lower.starts_with(&format!("{keyword} ")) {
            mode = Some(code);
            text = text[keyword.len()..].trim_start();
            break;
        }
    }
    match routine_arg_type_oid_inner(catalog, text) {
        Ok(type_oid) => return Ok((mode, type_oid)),
        Err(first_err) => {
            if let Some(type_text) = strip_routine_arg_name(text)
                && let Ok(type_oid) = routine_arg_type_oid_inner(catalog, type_text)
            {
                return Ok((mode, type_oid));
            }
            Err(first_err)
        }
    }
}

fn routine_arg_type_oid_inner(catalog: &dyn CatalogLookup, text: &str) -> Result<u32, ExecError> {
    let raw_type = parse_type_name(text).map_err(ExecError::Parse)?;
    let sql_type = resolve_raw_type_name(&raw_type, catalog).map_err(ExecError::Parse)?;
    catalog
        .type_oid_for_sql_type(sql_type)
        .ok_or_else(|| ExecError::Parse(ParseError::UnsupportedType(text.to_string())))
}

fn strip_routine_arg_name(text: &str) -> Option<&str> {
    let text = text.trim_start();
    let rest = if let Some(rest) = text.strip_prefix('"') {
        let mut escaped = false;
        let mut end = None;
        for (index, ch) in rest.char_indices() {
            if ch != '"' {
                escaped = false;
                continue;
            }
            if escaped {
                escaped = false;
                continue;
            }
            if rest[index + ch.len_utf8()..].starts_with('"') {
                escaped = true;
                continue;
            }
            end = Some(index + ch.len_utf8());
            break;
        }
        rest.get(end?..)?
    } else {
        let end = text
            .char_indices()
            .take_while(|(_, ch)| ch.is_ascii_alphanumeric() || *ch == '_')
            .map(|(index, ch)| index + ch.len_utf8())
            .last()?;
        text.get(end..)?
    };
    let rest = rest.trim_start();
    (!rest.is_empty()).then_some(rest)
}

fn parse_proc_argtype_oids(argtypes: &str) -> Vec<u32> {
    argtypes
        .split_whitespace()
        .filter_map(|part| part.parse::<u32>().ok())
        .collect()
}

fn routine_signature_matches(
    row: &PgProcRow,
    arg_specs: &[(Option<u8>, u32)],
    kind: RoutineKind,
) -> bool {
    routine_kind_matches(kind, row.prokind) && routine_signature_matches_any_kind(row, arg_specs)
}

fn routine_signature_matches_any_kind(row: &PgProcRow, arg_specs: &[(Option<u8>, u32)]) -> bool {
    if arg_specs.is_empty() {
        return true;
    }
    if row.prokind == 'p' && row.proallargtypes.is_some() {
        let all_types = row.proallargtypes.as_deref().unwrap_or_default();
        let modes = row.proargmodes.as_deref().unwrap_or_default();
        return all_types.len() == arg_specs.len()
            && all_types.iter().enumerate().all(|(index, oid)| {
                let (mode, desired_oid) = arg_specs[index];
                *oid == desired_oid
                    && mode
                        .map(|mode| mode == modes.get(index).copied().unwrap_or(b'i'))
                        .unwrap_or(true)
            });
    }
    let input_oids = parse_proc_argtype_oids(&row.proargtypes);
    let callable_specs = arg_specs
        .iter()
        .filter(|(mode, _)| !matches!(mode, Some(b'o')))
        .collect::<Vec<_>>();
    input_oids.len() == callable_specs.len()
        && input_oids
            .iter()
            .zip(callable_specs)
            .all(|(oid, (_, desired_oid))| *oid == *desired_oid)
}

fn routine_signature_display(
    catalog: &dyn CatalogLookup,
    signature: &RoutineSignature,
    arg_specs: &[(Option<u8>, u32)],
) -> String {
    let arg_types = if arg_specs.is_empty() {
        signature.arg_types.join(", ")
    } else {
        arg_specs
            .iter()
            .map(|(_, oid)| format_type_text(*oid, None, catalog))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!("{}({arg_types})", signature.routine_name)
}

fn routine_row_display(catalog: &dyn CatalogLookup, name: &str, row: &PgProcRow) -> String {
    let arg_types = parse_proc_argtype_oids(&row.proargtypes)
        .into_iter()
        .map(|oid| format_type_text(oid, None, catalog))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{name}({arg_types})")
}

fn namespace_name(catalog: &dyn CatalogLookup, namespace_oid: u32) -> String {
    catalog
        .namespace_row_by_oid(namespace_oid)
        .map(|row| row.nspname)
        .unwrap_or_else(|| namespace_oid.to_string())
}

fn duplicate_routine_error(catalog: &dyn CatalogLookup, row: &PgProcRow) -> ExecError {
    let display = routine_row_display(catalog, &row.proname, row);
    ExecError::DetailedError {
        message: format!(
            "function {display} already exists in schema \"{}\"",
            namespace_name(catalog, row.pronamespace)
        ),
        detail: None,
        hint: None,
        sqlstate: "42723",
    }
}

fn wrong_routine_kind_message(kind: RoutineKind, signature: &str) -> String {
    match kind {
        RoutineKind::Aggregate => format!("function {signature} is not an aggregate"),
        RoutineKind::Function => format!("{signature} is not a function"),
        RoutineKind::Procedure => format!("{signature} is not a procedure"),
        RoutineKind::Routine => format!("{signature} is not a routine"),
    }
}

fn routine_prokind_name(prokind: char) -> &'static str {
    match prokind {
        'a' => "aggregate",
        'p' => "procedure",
        _ => "function",
    }
}

fn resolve_routine(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    configured_search_path: Option<&[String]>,
    kind: RoutineKind,
    signature: &RoutineSignature,
) -> Result<PgProcRow, ExecError> {
    let catalog = db.lazy_catalog_lookup(client_id, txn_ctx, configured_search_path);
    let arg_specs = signature
        .arg_types
        .iter()
        .map(|arg| routine_arg_type_oid(&catalog, arg))
        .collect::<Result<Vec<_>, _>>()?;
    let schema_oid = match &signature.schema_name {
        Some(schema_name) => Some(
            db.visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("schema \"{schema_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "3F000",
                })?,
        ),
        None => None,
    };
    let normalized = normalize_ident(&signature.routine_name);
    let proc_rows = catalog.proc_rows_by_name(&normalized);
    let all_signature_candidates = if let Some(schema_oid) = schema_oid {
        proc_rows
            .into_iter()
            .filter(|row| {
                row.pronamespace == schema_oid
                    && routine_signature_matches_any_kind(row, &arg_specs)
            })
            .collect::<Vec<_>>()
    } else {
        let mut visible_candidates = Vec::new();
        for schema in db.effective_search_path(client_id, configured_search_path) {
            match schema.as_str() {
                "" | "$user" | "pg_temp" => continue,
                schema if schema.starts_with("pg_temp_") => continue,
                _ => {}
            }
            let Some(namespace_oid) = db.visible_namespace_oid_by_name(client_id, txn_ctx, &schema)
            else {
                continue;
            };
            visible_candidates = proc_rows
                .iter()
                .filter(|row| {
                    row.pronamespace == namespace_oid
                        && routine_signature_matches_any_kind(row, &arg_specs)
                })
                .cloned()
                .collect::<Vec<_>>();
            if !visible_candidates.is_empty() {
                break;
            }
        }
        visible_candidates
    };
    let candidates = all_signature_candidates
        .iter()
        .filter(|row| routine_signature_matches(row, &arg_specs, kind))
        .collect::<Vec<_>>();
    match candidates.as_slice() {
        [row] => Ok((*row).clone()),
        [] if kind != RoutineKind::Routine && !all_signature_candidates.is_empty() => {
            let signature = routine_signature_display(&catalog, signature, &arg_specs);
            Err(ExecError::DetailedError {
                message: wrong_routine_kind_message(kind, &signature),
                detail: None,
                hint: None,
                sqlstate: "42809",
            })
        }
        [] => Err(ExecError::DetailedError {
            message: format!(
                "{} {} does not exist",
                routine_kind_name(kind),
                routine_signature_display(&catalog, signature, &arg_specs)
            ),
            detail: None,
            hint: None,
            sqlstate: "42883",
        }),
        _ => Err(ExecError::DetailedError {
            message: format!(
                "{} name \"{}\" is not unique",
                routine_kind_name(kind),
                routine_signature_display(&catalog, signature, &arg_specs)
            ),
            detail: None,
            hint: Some(format!(
                "Specify the argument list to select the {} unambiguously.",
                routine_kind_name(kind)
            )),
            sqlstate: "42725",
        }),
    }
}

fn duplicate_routine_exists(
    catalog: &dyn CatalogLookup,
    row: &PgProcRow,
    new_name: &str,
    new_namespace: u32,
) -> bool {
    catalog
        .proc_rows_by_name(new_name)
        .into_iter()
        .any(|candidate| {
            candidate.oid != row.oid
                && candidate.pronamespace == new_namespace
                && candidate.proargtypes == row.proargtypes
        })
}

fn ensure_routine_owner(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    routine_name: &str,
    owner_oid: u32,
) -> Result<(), ExecError> {
    let auth = db.auth_state(client_id);
    let auth_catalog = db
        .auth_catalog(client_id, txn_ctx)
        .map_err(map_catalog_error)?;
    if auth.can_set_role(owner_oid, &auth_catalog) {
        return Ok(());
    }
    Err(ExecError::DetailedError {
        message: format!("must be owner of function {routine_name}"),
        detail: None,
        hint: None,
        sqlstate: "42501",
    })
}

fn support_signature_name(signature: &RoutineSignature) -> String {
    match &signature.schema_name {
        Some(schema_name) => format!("{schema_name}.{}", signature.routine_name),
        None => signature.routine_name.clone(),
    }
}

fn resolve_support_proc_oid(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    catalog: &dyn CatalogLookup,
    signature: &RoutineSignature,
) -> Result<u32, ExecError> {
    let name = support_signature_name(signature);
    let arg_specs = if signature.arg_types.is_empty() {
        vec![INTERNAL_TYPE_OID]
    } else {
        signature
            .arg_types
            .iter()
            .map(|arg| routine_arg_type_oid(catalog, arg).map(|(_, oid)| oid))
            .collect::<Result<Vec<_>, _>>()?
    };
    let (schema_name, base_name) = name
        .rsplit_once('.')
        .map(|(schema, proc_name)| (Some(schema), proc_name))
        .unwrap_or((None, name.as_str()));
    let namespace_oid = match schema_name {
        Some(schema_name) => Some(
            db.visible_namespace_oid_by_name(client_id, txn_ctx, schema_name)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("schema \"{schema_name}\" does not exist"),
                    detail: None,
                    hint: None,
                    sqlstate: "3F000",
                })?,
        ),
        None => None,
    };
    let matches = catalog
        .proc_rows_by_name(base_name)
        .into_iter()
        .filter(|row| {
            row.prokind == 'f'
                && parse_proc_argtype_oids(&row.proargtypes) == arg_specs
                && namespace_oid
                    .map(|namespace_oid| row.pronamespace == namespace_oid)
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [row] => Ok(row.oid),
        [] => Err(ExecError::DetailedError {
            message: format!("function {name}(internal) does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42883",
        }),
        _ => Err(ExecError::DetailedError {
            message: format!("function name {name}(internal) is ambiguous"),
            detail: None,
            hint: None,
            sqlstate: "42725",
        }),
    }
}

fn apply_routine_options(
    db: &Database,
    client_id: ClientId,
    txn_ctx: CatalogTxnContext,
    catalog: &dyn CatalogLookup,
    row: &mut PgProcRow,
    options: &[AlterRoutineOption],
) -> Result<(), ExecError> {
    for option in options {
        match option {
            AlterRoutineOption::Strict(strict) => {
                if row.prokind == 'p' {
                    return Err(ExecError::DetailedError {
                        message: "invalid attribute in procedure definition".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42P13",
                    });
                }
                row.proisstrict = *strict;
            }
            AlterRoutineOption::Volatility(volatility) => {
                row.provolatile = match volatility {
                    FunctionVolatility::Volatile => 'v',
                    FunctionVolatility::Stable => 's',
                    FunctionVolatility::Immutable => 'i',
                };
            }
            AlterRoutineOption::SecurityDefiner(value) => row.prosecdef = *value,
            AlterRoutineOption::Leakproof(value) => {
                if *value && !row.proleakproof && !current_user_is_superuser(catalog) {
                    return Err(ExecError::DetailedError {
                        message: "only superuser can define a leakproof function".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42501",
                    });
                }
                row.proleakproof = *value;
            }
            AlterRoutineOption::Parallel(parallel) => {
                row.proparallel = match parallel {
                    FunctionParallel::Safe => 's',
                    FunctionParallel::Restricted => 'r',
                    FunctionParallel::Unsafe => 'u',
                };
            }
            AlterRoutineOption::Cost(cost) => {
                row.procost = cost
                    .parse::<f64>()
                    .map_err(|_| ExecError::Parse(ParseError::InvalidNumeric(cost.clone())))?;
            }
            AlterRoutineOption::Rows(rows) => {
                row.prorows = rows
                    .parse::<f64>()
                    .map_err(|_| ExecError::Parse(ParseError::InvalidNumeric(rows.clone())))?;
            }
            AlterRoutineOption::Support(signature) => {
                row.prosupport =
                    resolve_support_proc_oid(db, client_id, txn_ctx, catalog, signature)?;
            }
            AlterRoutineOption::SetConfig { name, value } => set_proc_config(row, name, value),
            AlterRoutineOption::ResetConfig(name) => reset_proc_config(row, name),
            AlterRoutineOption::ResetAll => row.proconfig = None,
        }
    }
    Ok(())
}

impl Database {
    pub(crate) fn execute_alter_routine_stmt_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterRoutineStatement,
        configured_search_path: Option<&[String]>,
    ) -> Result<StatementResult, ExecError> {
        let xid = self.txns.write().begin();
        let guard = AutoCommitGuard::new(&self.txns, &self.txn_waiter, xid);
        let mut catalog_effects = Vec::new();
        let result = self.execute_alter_routine_stmt_in_transaction_with_search_path(
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

    pub(crate) fn execute_alter_routine_stmt_in_transaction_with_search_path(
        &self,
        client_id: ClientId,
        stmt: &AlterRoutineStatement,
        xid: TransactionId,
        cid: CommandId,
        configured_search_path: Option<&[String]>,
        catalog_effects: &mut Vec<CatalogMutationEffect>,
    ) -> Result<StatementResult, ExecError> {
        let old_row = resolve_routine(
            self,
            client_id,
            Some((xid, cid)),
            configured_search_path,
            stmt.kind,
            &stmt.signature,
        )?;
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        ensure_routine_owner(
            self,
            client_id,
            Some((xid, cid)),
            &old_row.proname,
            old_row.proowner,
        )?;
        let mut updated = old_row.clone();
        match &stmt.action {
            AlterRoutineAction::Options(options) => apply_routine_options(
                self,
                client_id,
                Some((xid, cid)),
                &catalog,
                &mut updated,
                options,
            )?,
            AlterRoutineAction::Rename { new_name } => {
                updated.proname = normalize_ident(new_name);
            }
            AlterRoutineAction::SetSchema { new_schema } => {
                updated.pronamespace = self
                    .visible_namespace_oid_by_name(client_id, Some((xid, cid)), new_schema)
                    .ok_or_else(|| ExecError::DetailedError {
                        message: format!("schema \"{new_schema}\" does not exist"),
                        detail: None,
                        hint: None,
                        sqlstate: "3F000",
                    })?;
            }
            AlterRoutineAction::OwnerTo { new_owner } => {
                let auth_catalog = self
                    .auth_catalog(client_id, Some((xid, cid)))
                    .map_err(map_catalog_error)?;
                updated.proowner = auth_catalog
                    .role_by_name(new_owner)
                    .map(|row| row.oid)
                    .ok_or_else(|| {
                        ExecError::Parse(crate::backend::commands::rolecmds::role_management_error(
                            format!("role \"{new_owner}\" does not exist"),
                        ))
                    })?;
                ensure_can_set_role(self, client_id, updated.proowner, new_owner)?;
            }
            AlterRoutineAction::DependsOnExtension { .. } => {
                return Ok(StatementResult::AffectedRows(0));
            }
        }
        if duplicate_routine_exists(&catalog, &old_row, &updated.proname, updated.pronamespace) {
            return Err(duplicate_routine_error(&catalog, &updated));
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
        let updated_for_error = updated.clone();
        let (_oid, effect) = if old_row.prokind == 'a' {
            let old_aggregate = catalog.aggregate_by_fnoid(old_row.oid).ok_or_else(|| {
                ExecError::DetailedError {
                    message: format!("aggregate row for procedure {} does not exist", old_row.oid),
                    detail: None,
                    hint: None,
                    sqlstate: "XX000",
                }
            })?;
            self.catalog
                .write()
                .replace_aggregate_mvcc(
                    &old_row,
                    &old_aggregate,
                    updated,
                    old_aggregate.clone(),
                    &ctx,
                )
                .map_err(|err| match err {
                    crate::backend::catalog::CatalogError::UniqueViolation(_) => {
                        duplicate_routine_error(&catalog, &updated_for_error)
                    }
                    other => map_catalog_error(other),
                })?
        } else {
            self.catalog
                .write()
                .replace_proc_mvcc(&old_row, updated, &ctx)
                .map_err(|err| match err {
                    crate::backend::catalog::CatalogError::UniqueViolation(_) => {
                        duplicate_routine_error(&catalog, &updated_for_error)
                    }
                    other => map_catalog_error(other),
                })?
        };
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
