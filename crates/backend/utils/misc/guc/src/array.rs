use elog::ereport;
use types_error::{ErrorLevel, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT, ERROR, WARNING};
use types_guc::{GucContext, GucSource};

use crate::store::with_store_mut;
use crate::{set_config_option, valid_custom_variable_name, GucAction, ParseLongOption, GUC_ACTION_SET};

// GUC option arrays travel as flat "name=value" string lists; the text[]
// image lives with the catalog owners (pg_db_role_setting et al.).

fn find_option_normalized(name: &str, make_placeholder: bool) -> PgResult<Option<(String, GucContext, bool)>> {
    with_store_mut(|reg| -> PgResult<Option<(String, GucContext, bool)>> {
        if let Some(var) = reg.find_option(name) {
            let gen = var.gen();
            return Ok(Some((
                var.name().to_string(),
                gen.context,
                gen.flags & types_guc::GUC_CUSTOM_PLACEHOLDER != 0,
            )));
        }
        if make_placeholder && crate::assignable_custom_variable_name(name, true)? {
            reg.add_placeholder_variable(name)?;
            let var = reg.find_option(name).expect("placeholder just added");
            return Ok(Some((var.name().to_string(), var.gen().context, true)));
        }
        Ok(None)
    })
    .expect("GUC registry not initialized")
}

pub fn validate_option_array_item(
    name: &str,
    value: Option<&str>,
    skip_if_no_permissions: bool,
) -> PgResult<bool> {
    let reset_custom = value.is_none() && valid_custom_variable_name(name);
    let found = find_option_normalized(name, true)?;

    if found.is_none() && !reset_custom {
        if skip_if_no_permissions {
            return Ok(false);
        }
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("unrecognized configuration parameter \"{name}\""))
            .into_error()
            .into());
    }

    let user_id = miscinit::GetUserId();
    let is_placeholder = found.as_ref().map(|f| f.2).unwrap_or(true);
    if is_placeholder {
        if superuser_seams::superuser::call()?
            || aclchk_seams::pg_parameter_aclcheck_set::call(name, user_id)?
        {
            return Ok(true);
        }
        if skip_if_no_permissions {
            return Ok(false);
        }
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("permission denied to set parameter \"{name}\""))
            .into_error()
            .into());
    }

    let (_, context, _) = found.as_ref().expect("non-placeholder is found");
    match context {
        GucContext::PGC_USERSET => {}
        GucContext::PGC_SUSET
            if superuser_seams::superuser::call()?
                || aclchk_seams::pg_parameter_aclcheck_set::call(name, user_id)? => {}
        _ if skip_if_no_permissions => return Ok(false),
        _ => {}
    }

    set_config_option(
        name,
        value,
        *context,
        GucSource::PGC_S_TEST,
        GUC_ACTION_SET,
        false,
        ErrorLevel(0),
        false,
    )?;
    Ok(true)
}

pub fn GUCArrayAdd(array: &[String], name: &str, value: &str) -> PgResult<Vec<String>> {
    validate_option_array_item(name, Some(value), false)?;

    let name = match find_option_normalized(name, false)? {
        Some((canon, _, _)) => canon,
        None => name.to_string(),
    };

    let newval = format!("{name}={value}");
    let prefix_len = name.len() + 1;

    let mut out: Vec<String> = array.to_vec();
    for entry in out.iter_mut() {
        if entry.as_bytes().len() >= prefix_len && entry.as_bytes()[..prefix_len] == newval.as_bytes()[..prefix_len] {
            *entry = newval;
            return Ok(out);
        }
    }
    out.push(newval);
    Ok(out)
}

pub fn GUCArrayDelete(array: &[String], name: &str) -> PgResult<Option<Vec<String>>> {
    validate_option_array_item(name, None, false)?;

    let name = match find_option_normalized(name, false)? {
        Some((canon, _, _)) => canon,
        None => name.to_string(),
    };

    let mut out: Vec<String> = Vec::new();
    for entry in array {
        if entry.strip_prefix(name.as_str()).is_some_and(|rest| rest.starts_with('=')) {
            continue;
        }
        out.push(entry.clone());
    }
    Ok(if out.is_empty() { None } else { Some(out) })
}

pub fn GUCArrayReset(array: &[String]) -> PgResult<Option<Vec<String>>> {
    if superuser_seams::superuser::call()? {
        return Ok(None);
    }

    let mut out: Vec<String> = Vec::new();
    for entry in array {
        let name = entry.split('=').next().unwrap_or(entry);
        if validate_option_array_item(name, None, true)? {
            continue;
        }
        out.push(entry.clone());
    }
    Ok(if out.is_empty() { None } else { Some(out) })
}

pub fn TransformGUCArray(array: &[String]) -> PgResult<Vec<(String, String)>> {
    let mut out = Vec::with_capacity(array.len());
    for entry in array {
        let (name, value) = ParseLongOption(entry);
        match value {
            Some(value) => out.push((name, value)),
            None => {
                ereport(WARNING)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("could not parse setting for parameter \"{name}\""))
                    .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "TransformGUCArray"))?;
            }
        }
    }
    Ok(out)
}

pub fn ProcessGUCArray(
    array: &[String],
    context: GucContext,
    source: GucSource,
    action: GucAction,
) -> PgResult<()> {
    for (name, value) in TransformGUCArray(array)? {
        set_config_option(
            &name,
            Some(&value),
            context,
            source,
            action,
            true,
            ErrorLevel(0),
            false,
        )?;
    }
    Ok(())
}
