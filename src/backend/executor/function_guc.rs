use std::collections::{HashMap, HashSet};

use crate::backend::executor::{ExecError, ExecutorContext};
use crate::backend::utils::misc::guc::{normalize_function_guc_assignment, normalize_guc_name};

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
    gucs: &mut HashMap<String, String>,
    name: &str,
    value: Option<&str>,
) -> Result<String, ExecError> {
    let normalized = normalize_guc_name(name);
    if let Some(value) = value {
        let (normalized, stored_value) =
            normalize_function_guc_assignment(&normalized, value, false, true)
                .map_err(ExecError::Parse)?;
        gucs.insert(normalized.clone(), stored_value);
        Ok(normalized)
    } else {
        gucs.remove(&normalized);
        Ok(normalized)
    }
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

pub(crate) fn execute_with_sql_function_gucs<T>(
    config: Option<&[String]>,
    ctx: &mut ExecutorContext,
    f: impl FnOnce(&mut ExecutorContext) -> Result<T, ExecError>,
) -> Result<T, ExecError> {
    let entries = parsed_proconfig(config);
    if entries.is_empty() {
        return f(ctx);
    }
    let saved_gucs = ctx.gucs.clone();
    let mut restore_names = HashSet::new();
    for (name, value) in entries {
        let normalized = apply_function_guc(&mut ctx.gucs, &name, Some(&value))?;
        restore_names.insert(normalized);
    }
    let result = f(ctx);
    restore_function_gucs(ctx, saved_gucs, restore_names);
    result
}
