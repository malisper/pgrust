//! GUC option-array helpers (`proconfig` / `pg_db_role_setting.setconfig`):
//! `GUCArrayAdd`, `GUCArrayDelete`, `GUCArrayReset`, and the private
//! `validate_option_array_item` (guc.c lines 6494-6796).
//!
//! The repo carries a proconfig `text[]` as an owned `Vec<String>` of
//! `"name=value"` entries (the value-model the consumer seams use), rather than
//! the C `ArrayType *`. The element-by-element `array_ref`/`array_set` loops of
//! the C therefore become plain `Vec` iteration; everything else (the
//! permission/validation logic in `validate_option_array_item`, the obsolete-name
//! normalization via `find_option`, the superuser shortcut in `GUCArrayReset`)
//! mirrors the C 1:1.

use backend_utils_error::ereport;
use types_acl::{AclResult, ACL_SET};
use types_error::{PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERROR};
use types_guc::{GUC_CUSTOM_PLACEHOLDER, PGC_SUSET, PGC_S_TEST, PGC_USERSET};

use crate::live::{set_config_option_global, with_store};
use crate::process_config::valid_custom_variable_name;
use crate::GUC_ACTION_SET;

/// `superuser()` (superuser.c) via the misc-more-installed seam.
fn superuser() -> PgResult<bool> {
    backend_utils_misc_superuser_seams::superuser::call()
}

/// `GetUserId()` (miscinit.c).
fn get_user_id() -> types_core::Oid {
    backend_utils_init_miscinit_seams::get_user_id::call()
}

/// `pg_parameter_aclcheck(name, GetUserId(), ACL_SET) == ACLCHECK_OK` (aclchk.c).
fn parameter_acl_set_ok(name: &str) -> PgResult<bool> {
    Ok(crate::seam::pg_parameter_aclcheck::call(name, get_user_id(), ACL_SET)?
        == AclResult::AclcheckOk)
}

/// `find_option(name, false, true, WARNING)` returning the canonical (modern)
/// spelling of `name` — guc.c normalizes obsolete GUC names before building the
/// `"name=value"` item. Returns the input unchanged when the variable is
/// unknown (matching the C `if (record) name = record->name`).
fn normalize_name(name: &str) -> String {
    with_store(|reg| reg.find_option(name).map(|v| v.name_pub().to_string()))
        .flatten()
        .unwrap_or_else(|| name.to_string())
}

/// `"name="` prefix-match: an array entry `entry` is the setting for `name`
/// when it begins with `name` immediately followed by `'='`.
fn entry_is_for(entry: &str, name: &str) -> bool {
    entry.len() > name.len()
        && entry.as_bytes()[name.len()] == b'='
        && &entry[..name.len()] == name
}

/// `ArrayType *GUCArrayAdd(ArrayType *array, const char *name, const char *value)`
/// (guc.c:6494). Append or replace the `name=value` entry.
pub fn GUCArrayAdd(
    array: Option<Vec<String>>,
    name: &str,
    value: &str,
) -> PgResult<Vec<String>> {
    // test if the option is valid and we're allowed to set it
    let _ = validate_option_array_item(name, Some(value), false)?;

    // normalize name (converts obsolete GUC names to modern spellings)
    let name = normalize_name(name);

    // build new item for array
    let newval = format!("{name}={value}");

    match array {
        Some(mut a) => {
            // Find an existing entry matching up to and including '='; replace
            // it. Otherwise append after the end (C: index = ARR_DIMS+1).
            let mut replaced = false;
            for entry in a.iter_mut() {
                if entry_is_for(entry, &name) {
                    *entry = newval.clone();
                    replaced = true;
                    break;
                }
            }
            if !replaced {
                a.push(newval);
            }
            Ok(a)
        }
        None => Ok(vec![newval]),
    }
}

/// `ArrayType *GUCArrayDelete(ArrayType *array, const char *name)` (guc.c:6572).
/// Drop the `name=...` entry; `None` if the array becomes empty.
pub fn GUCArrayDelete(
    array: Option<Vec<String>>,
    name: &str,
) -> PgResult<Option<Vec<String>>> {
    // test if the option is valid and we're allowed to set it
    let _ = validate_option_array_item(name, None, false)?;

    // normalize name (converts obsolete GUC names to modern spellings)
    let name = normalize_name(name);

    // if array is currently null, then surely nothing to delete
    let Some(a) = array else {
        return Ok(None);
    };

    let mut newarray: Vec<String> = Vec::new();
    for entry in a {
        // ignore entry if it's what we want to delete
        if entry_is_for(&entry, &name) {
            continue;
        }
        newarray.push(entry);
    }

    if newarray.is_empty() {
        Ok(None)
    } else {
        Ok(Some(newarray))
    }
}

/// `ArrayType *GUCArrayReset(ArrayType *array)` (guc.c:6642). Delete all
/// settings the caller's permission level allows: superuser removes everything,
/// a regular user only the entries they may set.
pub fn GUCArrayReset(array: Vec<String>) -> PgResult<Option<Vec<String>>> {
    // (The seam takes a non-null array; the C `if (!array) return NULL` is the
    // caller's `Some`/`None` discriminator.)

    // if we're superuser, we can delete everything, so just do it
    if superuser()? {
        return Ok(None);
    }

    let mut newarray: Vec<String> = Vec::new();
    for entry in array {
        // C: split at the first '=' to get the bare name (val[..eqsgn]).
        let val = match entry.split_once('=') {
            Some((name, _)) => name,
            None => &entry[..],
        };

        // skip if we have permission to delete it
        if validate_option_array_item(val, None, true)? {
            continue;
        }

        // else add it to the output array
        newarray.push(entry);
    }

    if newarray.is_empty() {
        Ok(None)
    } else {
        Ok(Some(newarray))
    }
}

/// `static bool validate_option_array_item(const char *name, const char *value,
/// bool skipIfNoPermissions)` (guc.c:6714).
///
/// Returns `Ok(true)` if OK, `Ok(false)` when `skip_if_no_permissions` is true
/// and the user lacks permission; all other error cases `Err` (the C
/// `ereport(ERROR)` paths, including the one inside `set_config_option`).
fn validate_option_array_item(
    name: &str,
    value: Option<&str>,
    skip_if_no_permissions: bool,
) -> PgResult<bool> {
    // (See the long comment in the C original for the three cases.)
    let reset_custom = value.is_none() && valid_custom_variable_name(name);

    // find_option(name, create_placeholders=true, skip_errors=skip||reset_custom,
    // elevel=ERROR). The repo's registry has no placeholder-creation path, so a
    // custom (unknown) name resolves to "not found"; combined with reset_custom
    // this reproduces the C control flow (gconf == NULL && reset_custom -> fall
    // through to the placeholder permissions branch).
    let known_context = with_store(|reg| {
        reg.find_option(name)
            .map(|v| (v.gen().context, v.gen().flags))
    })
    .flatten();

    if known_context.is_none() && !reset_custom {
        // not known, failed to make a placeholder
        return Ok(false);
    }

    let is_placeholder = match known_context {
        None => true, // unknown but reset_custom -> placeholder case
        Some((_, flags)) => (flags & GUC_CUSTOM_PLACEHOLDER) != 0,
    };

    if is_placeholder {
        // We cannot do any meaningful check on the value, so only permissions
        // are useful to check.
        if superuser()? || parameter_acl_set_ok(name)? {
            return Ok(true);
        }
        if skip_if_no_permissions {
            return Ok(false);
        }
        return Err(permission_denied(name));
    }

    let context = known_context.expect("non-placeholder implies a known variable").0;

    // manual permissions check so we can avoid an error being thrown
    if context == PGC_USERSET {
        /* ok */
    } else if context == PGC_SUSET && (superuser()? || parameter_acl_set_ok(name)?) {
        /* ok */
    } else if skip_if_no_permissions {
        return Ok(false);
    }
    // if a permissions error should be thrown, let set_config_option do it

    // test for permissions and valid option value
    let _ = set_config_option_global(
        name,
        value,
        if superuser()? { PGC_SUSET } else { PGC_USERSET },
        PGC_S_TEST,
        get_user_id(),
        GUC_ACTION_SET,
        false,
        ERROR,
        false,
    )?;

    Ok(true)
}

/// `ereport(ERROR, errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
/// errmsg("permission denied to set parameter \"%s\"", name))`.
fn permission_denied(name: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
        .errmsg(format!("permission denied to set parameter \"{name}\""))
        .into_error()
}
