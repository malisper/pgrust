use std::collections::HashMap;

use crate::backend::catalog::roles::find_role_by_name;
use crate::backend::commands::rolecmds::role_management_error;
use crate::backend::executor::{ExecError, ExecutorContext};
use crate::backend::parser::CatalogLookup;
use crate::backend::utils::misc::guc::{normalize_function_guc_assignment, normalize_guc_name};
use crate::include::catalog::PgProcRow;
use crate::pgrust::auth::{AuthCatalog, AuthState};
pub(crate) use pgrust_executor::parsed_proconfig;

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

pub(crate) type SavedFunctionIdentity = pgrust_executor::SavedFunctionIdentity;

pub(crate) fn save_function_identity(ctx: &ExecutorContext) -> SavedFunctionIdentity {
    pgrust_executor::save_function_identity_state(ctx.current_user_oid, ctx.active_role_oid)
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
    pgrust_executor::restore_function_gucs(&mut ctx.gucs, &saved_gucs, restore_names);
}

impl pgrust_executor::FunctionGucContext for ExecutorContext {
    type Error = ExecError;

    fn save_identity(&self) -> SavedFunctionIdentity {
        save_function_identity(self)
    }

    fn restore_identity(&mut self, saved: SavedFunctionIdentity) {
        restore_function_identity(self, saved);
    }

    fn apply_security_definer_identity(&mut self, owner_oid: u32) {
        apply_security_definer_identity(self, owner_oid);
    }

    fn gucs(&self) -> &HashMap<String, String> {
        &self.gucs
    }

    fn gucs_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.gucs
    }

    fn apply_function_guc(&mut self, name: &str, value: Option<&str>) -> Result<String, ExecError> {
        apply_function_guc(self, name, value)
    }
}

pub(crate) fn execute_with_sql_function_context<T>(
    row: &PgProcRow,
    ctx: &mut ExecutorContext,
    f: impl FnOnce(&mut ExecutorContext) -> Result<T, ExecError>,
) -> Result<T, ExecError> {
    pgrust_executor::execute_with_sql_function_context(row, ctx, f)
}
