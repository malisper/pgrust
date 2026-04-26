use super::super::*;
use super::privilege::{routine_kind_matches, routine_kind_name};
use crate::backend::catalog::store::{CatalogMutationEffect, CatalogWriteContext};
use crate::backend::executor::expr_reg::format_type_text;
use crate::backend::parser::{
    AlterRoutineAction, AlterRoutineOption, AlterRoutineStatement, CatalogLookup, FunctionParallel,
    FunctionVolatility, ParseError, RoutineKind, RoutineSignature, parse_type_name,
    resolve_raw_type_name,
};
use crate::include::catalog::PgProcRow;

fn normalize_ident(name: &str) -> String {
    name.trim().trim_matches('"').to_ascii_lowercase()
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
    let all_signature_candidates = catalog
        .proc_rows_by_name(&normalized)
        .into_iter()
        .filter(|row| {
            schema_oid
                .map(|schema_oid| row.pronamespace == schema_oid)
                .unwrap_or(true)
                && routine_signature_matches_any_kind(row, &arg_specs)
        })
        .collect::<Vec<_>>();
    let candidates = all_signature_candidates
        .iter()
        .filter(|row| routine_signature_matches(row, &arg_specs, kind))
        .collect::<Vec<_>>();
    match candidates.as_slice() {
        [row] => Ok((*row).clone()),
        [] if kind != RoutineKind::Routine && !all_signature_candidates.is_empty() => {
            let signature = routine_signature_display(&catalog, signature, &arg_specs);
            Err(ExecError::DetailedError {
                message: format!("{signature} is not a {}", routine_kind_name(kind)),
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
                && candidate.prokind == row.prokind
                && candidate.proargtypes == row.proargtypes
        })
}

fn apply_routine_options(
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
            AlterRoutineOption::Leakproof(value) => row.proleakproof = *value,
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
            AlterRoutineOption::Support(_)
            | AlterRoutineOption::SetConfig { .. }
            | AlterRoutineOption::ResetConfig(_)
            | AlterRoutineOption::ResetAll => {
                // :HACK: pg_proc.proconfig and dependency-on-extension metadata are not yet
                // represented in PgProcRow. Accept the grammar and leave catalog shape stable.
            }
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
        let mut updated = old_row.clone();
        match &stmt.action {
            AlterRoutineAction::Options(options) => apply_routine_options(&mut updated, options)?,
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
            }
            AlterRoutineAction::DependsOnExtension { .. } => {
                return Ok(StatementResult::AffectedRows(0));
            }
        }
        let catalog = self.lazy_catalog_lookup(client_id, Some((xid, cid)), configured_search_path);
        if duplicate_routine_exists(&catalog, &old_row, &updated.proname, updated.pronamespace) {
            return Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "unique routine signature",
                actual: format!(
                    "{} {}({}) already exists",
                    routine_kind_name(stmt.kind),
                    updated.proname,
                    updated.proargtypes
                ),
            }));
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
        let (_oid, effect) = self
            .catalog
            .write()
            .replace_proc_mvcc(&old_row, updated, &ctx)
            .map_err(map_catalog_error)?;
        catalog_effects.push(effect);
        Ok(StatementResult::AffectedRows(0))
    }
}
