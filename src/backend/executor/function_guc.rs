use std::collections::{HashMap, HashSet};

use crate::backend::catalog::roles::find_role_by_name;
use crate::backend::commands::rolecmds::role_management_error;
use crate::backend::executor::{ExecError, ExecutorContext};
use crate::backend::parser::CatalogLookup;
use crate::backend::utils::misc::guc::{normalize_function_guc_assignment, normalize_guc_name};
use crate::include::catalog::PgProcRow;
use crate::pgrust::auth::{AuthCatalog, AuthState};

pub(crate) fn parsed_proconfig(config: Option<&[String]>) -> Vec<(String, String)> {
    config
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let (name, value) = entry.split_once('=')?;
            Some((name.to_string(), value.to_string()))
        })
        .collect()
}

pub(crate) fn apply_function_guc(
    ctx: &mut ExecutorContext,
    name: &str,
    value: Option<&str>,
) -> Result<String, ExecError> {
    let normalized = normalize_guc_name(name);
    if normalized == "role" {
        apply_function_role_guc(ctx, value)?;
        return Ok(normalized);
    }
    if let Some(value) = value {
        let (normalized, stored_value) =
            normalize_function_guc_assignment(&normalized, value, false, true)
                .map_err(ExecError::Parse)?;
        ctx.gucs.insert(normalized.clone(), stored_value);
        Ok(normalized)
    } else {
        ctx.gucs.remove(&normalized);
        Ok(normalized)
    }
}

fn apply_function_role_guc(
    ctx: &mut ExecutorContext,
    value: Option<&str>,
) -> Result<(), ExecError> {
    let Some(value) = value else {
        ctx.current_user_oid = ctx.session_user_oid;
        ctx.active_role_oid = None;
        return Ok(());
    };
    let value = value.trim().trim_matches('\'').trim_matches('"');
    if value.eq_ignore_ascii_case("none") || value.eq_ignore_ascii_case("default") {
        ctx.current_user_oid = ctx.session_user_oid;
        ctx.active_role_oid = None;
        return Ok(());
    }

    let catalog = ctx
        .catalog
        .as_ref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "function SET role requires executor catalog context".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        })?;
    let authid_rows = catalog.authid_rows();
    let auth_members_rows = catalog.auth_members_rows();
    let auth_catalog = AuthCatalog::new(authid_rows, auth_members_rows);
    let target = find_role_by_name(auth_catalog.roles(), value)
        .cloned()
        .ok_or_else(|| {
            ExecError::Parse(role_management_error(format!(
                "role \"{value}\" does not exist"
            )))
        })?;
    let auth = AuthState::from_executor_identity(
        ctx.session_user_oid,
        ctx.current_user_oid,
        ctx.active_role_oid,
    );
    if !auth.can_set_role_from_session(target.oid, &auth_catalog) {
        return Err(ExecError::Parse(role_management_error(format!(
            "permission denied to set role \"{}\"",
            target.rolname
        ))));
    }

    ctx.current_user_oid = target.oid;
    ctx.active_role_oid = Some(target.oid);
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SavedFunctionIdentity {
    current_user_oid: u32,
    active_role_oid: Option<u32>,
}

pub(crate) fn save_function_identity(ctx: &ExecutorContext) -> SavedFunctionIdentity {
    SavedFunctionIdentity {
        current_user_oid: ctx.current_user_oid,
        active_role_oid: ctx.active_role_oid,
    }
}

pub(crate) fn restore_function_identity(ctx: &mut ExecutorContext, saved: SavedFunctionIdentity) {
    ctx.current_user_oid = saved.current_user_oid;
    ctx.active_role_oid = saved.active_role_oid;
}

pub(crate) fn apply_security_definer_identity(ctx: &mut ExecutorContext, owner_oid: u32) {
    ctx.current_user_oid = owner_oid;
}

pub(crate) fn restore_function_gucs(
    ctx: &mut ExecutorContext,
    saved_gucs: HashMap<String, String>,
    restore_names: impl IntoIterator<Item = String>,
) {
    for name in restore_names {
        if let Some(value) = saved_gucs.get(&name) {
            ctx.gucs.insert(name, value.clone());
        } else {
            ctx.gucs.remove(&name);
        }
    }
}

pub(crate) fn execute_with_sql_function_context<T>(
    row: &PgProcRow,
    ctx: &mut ExecutorContext,
    f: impl FnOnce(&mut ExecutorContext) -> Result<T, ExecError>,
) -> Result<T, ExecError> {
    let entries = parsed_proconfig(row.proconfig.as_deref());
    if entries.is_empty() && !row.prosecdef {
        return f(ctx);
    }
    let saved_identity = save_function_identity(ctx);
    if row.prosecdef {
        apply_security_definer_identity(ctx, row.proowner);
    }
    let saved_gucs = ctx.gucs.clone();
    let mut restore_names = HashSet::new();
    for (name, value) in entries {
        let normalized = match apply_function_guc(ctx, &name, Some(&value)) {
            Ok(normalized) => normalized,
            Err(err) => {
                ctx.gucs = saved_gucs;
                restore_function_identity(ctx, saved_identity);
                return Err(err);
            }
        };
        restore_names.insert(normalized);
    }
    let result = f(ctx);
    restore_function_gucs(ctx, saved_gucs, restore_names);
    restore_function_identity(ctx, saved_identity);
    result
}
