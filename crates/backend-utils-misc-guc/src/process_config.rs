//! The apply core of `ProcessConfigFileInternal` (`guc.c`).
//!
//! The configuration-file *parser* lives in `backend-utils-misc-guc-file`
//! (`ParseConfigFile`/`ParseConfigFp`). What it produces is a list of parsed
//! `name = value` items. *Applying* those settings to the live GUC registry —
//! marking each extant variable as present/absent in the file, reverting removed
//! variables to their boot defaults, and feeding each present setting through
//! `set_config_option` at `PGC_S_FILE` — is the GUC core's job, so it lives
//! here, in the crate that owns the process-global [`GucRegistry`] store.
//!
//! Faithful 1:1 port of `ProcessConfigFileInternal` (guc.c) from the point where
//! the file has been parsed onward: the `head` list of `ConfigVariable`s is
//! presented as a slice of [`ConfigItem`]s.

use backend_utils_error::{ereport, emit_error_report_for, message_level_is_interesting};
use types_core::{TimestampTz, BOOTSTRAP_SUPERUSERID};
use types_error::{
    ErrorLevel, PgResult, ERRCODE_CANT_CHANGE_RUNTIME_PARAM, ERRCODE_CONFIG_FILE_ERROR,
    ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_guc::{
    GucContext, PGC_BACKEND, PGC_POSTMASTER, PGC_SIGHUP, PGC_S_DEFAULT, PGC_S_DYNAMIC_DEFAULT,
    PGC_S_FILE,
};

use crate::live::{self, set_config_option_global};
use crate::model::{GUC_IS_IN_FILE, GUC_PENDING_RESTART};
use crate::GUC_ACTION_SET;

/// The GUC core's minimal view of one parsed config-file entry (the relevant
/// fields of the parser's `struct ConfigVariable`).
#[derive(Clone, Debug)]
pub struct ConfigItem {
    pub name: String,
    pub value: String,
    pub filename: String,
    pub sourceline: i32,
    pub ignore: bool,
    pub applied: bool,
    pub errmsg: Option<String>,
}

/// `ProcessConfigFileInternal`'s apply phase (guc.c), operating on the parsed
/// `head` list (`items`, with parse-error records already removed) and the live
/// global GUC store.
///
/// Returns `Ok(true)` when no error was detected, `Ok(false)` when an error was
/// recorded below ERROR, and `Err` when `elevel >= ERROR` and an error fired.
pub fn apply_config_variables(
    items: &mut [ConfigItem],
    context: GucContext,
    apply_settings: bool,
    elevel: ErrorLevel,
    conf_file_with_error: &mut String,
    reload_time: TimestampTz,
) -> PgResult<bool> {
    let mut error = false;
    let mut applying = false;

    // Mark all extant GUC variables as not present in the config file.
    live::with_store_mut(|reg| {
        for idx in 0..reg.len() {
            reg[idx].gen_mut().status &= !GUC_IS_IN_FILE;
        }
    });

    // Check if all the supplied option names are valid.
    for i in 0..items.len() {
        if items[i].ignore {
            continue;
        }

        let found = live::with_store_mut(|reg| match reg.find_index_pub(&items[i].name) {
            Some(idx) => {
                let already = reg[idx].gen().status & GUC_IS_IN_FILE != 0;
                reg[idx].gen_mut().status |= GUC_IS_IN_FILE;
                Some(already)
            }
            None => None,
        })
        .flatten();

        match found {
            Some(true) => {
                // Duplicate entry: mark the earlier occurrence(s) as dead.
                let name = items[i].name.clone();
                for pitem in items.iter_mut().take(i) {
                    if !pitem.ignore && crate::guc_name_eq(&pitem.name, &name) {
                        pitem.ignore = true;
                    }
                }
            }
            Some(false) => {}
            None => {
                if !valid_custom_variable_name(&items[i].name) {
                    report(
                        elevel,
                        ereport(elevel)
                            .errcode(ERRCODE_UNDEFINED_OBJECT)
                            .errmsg(format!(
                                "unrecognized configuration parameter \"{}\" in file \"{}\" line {}",
                                items[i].name, items[i].filename, items[i].sourceline
                            ))
                            .into_error(),
                    )?;
                    items[i].errmsg = Some("unrecognized configuration parameter".to_string());
                    error = true;
                    conf_file_with_error.clone_from(&items[i].filename);
                }
            }
        }
    }

    if error {
        return bail_out(context, elevel, error, applying, apply_settings, conf_file_with_error);
    }

    applying = true;

    // Variables removed from the config file: revert to boot defaults.
    struct Removed {
        name: String,
        needs_restart: bool,
        do_reset: bool,
    }
    let removed: Vec<Removed> = live::with_store_mut(|reg| {
        let mut out = Vec::new();
        for idx in 0..reg.len() {
            let gen = reg[idx].gen();
            if gen.reset_source != PGC_S_FILE || (gen.status & GUC_IS_IN_FILE != 0) {
                continue;
            }
            let name = gen.name.to_string();
            if gen.context < PGC_SIGHUP {
                reg[idx].gen_mut().status |= GUC_PENDING_RESTART;
                out.push(Removed { name, needs_restart: true, do_reset: false });
                continue;
            }
            if !apply_settings {
                continue;
            }
            let g = reg[idx].gen_mut();
            if g.reset_source == PGC_S_FILE {
                g.reset_source = PGC_S_DEFAULT;
            }
            if g.source == PGC_S_FILE {
                g.source = PGC_S_DEFAULT;
            }
            let mut stack = g.stack.as_deref_mut();
            while let Some(s) = stack {
                if s.source == PGC_S_FILE {
                    s.source = PGC_S_DEFAULT;
                }
                stack = s.prev.as_deref_mut();
            }
            out.push(Removed { name, needs_restart: false, do_reset: true });
        }
        out
    })
    .unwrap_or_default();

    for r in removed {
        if r.needs_restart {
            report(
                elevel,
                ereport(elevel)
                    .errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM)
                    .errmsg(format!(
                        "parameter \"{}\" cannot be changed without restarting the server",
                        r.name
                    ))
                    .into_error(),
            )?;
            error = true;
            continue;
        }
        if !r.do_reset {
            continue;
        }
        // Re-apply the wired-in default (i.e., the boot_val).
        let scres = set_config_option_global(
            &r.name,
            None,
            context,
            PGC_S_DEFAULT,
            BOOTSTRAP_SUPERUSERID,
            GUC_ACTION_SET,
            true,
            ErrorLevel(0),
            false,
        )?;
        if scres > 0 && context == PGC_SIGHUP {
            report(
                elevel,
                ereport(elevel)
                    .errmsg(format!(
                        "parameter \"{}\" removed from configuration file, reset to default",
                        r.name
                    ))
                    .into_error(),
            )?;
        }
    }

    // Restore env/dynamic defaults: the only one performable with the present
    // substrate is the client_encoding dynamic default (the env/timezone re-init
    // is a GUC-core init step that is its own port; a SIGHUP-only no-op unless
    // one of those env/tz GUCs was just removed from the file).
    if context == PGC_SIGHUP && apply_settings {
        let enc = live::get_string("client_encoding").flatten();
        if let Some(enc) = enc {
            let _ = set_config_option_global(
                "client_encoding",
                Some(&enc),
                PGC_BACKEND,
                PGC_S_DYNAMIC_DEFAULT,
                BOOTSTRAP_SUPERUSERID,
                GUC_ACTION_SET,
                true,
                ErrorLevel(0),
                false,
            );
        }
    }

    // Now apply the values from the config file.
    for item in items.iter_mut() {
        if item.ignore {
            continue;
        }

        let report_changes = context == PGC_SIGHUP
            && apply_settings
            && !crate::seam::is_under_postmaster::call();
        let pre_value = if report_changes {
            Some(get_config_value(&item.name))
        } else {
            None
        };

        let scres = set_config_option_global(
            &item.name,
            Some(&item.value),
            context,
            PGC_S_FILE,
            BOOTSTRAP_SUPERUSERID,
            GUC_ACTION_SET,
            apply_settings,
            ErrorLevel(0),
            false,
        )?;

        if scres > 0 {
            if let Some(pre) = pre_value.as_ref() {
                let post = get_config_value(&item.name);
                if pre != &post {
                    report(
                        elevel,
                        ereport(elevel)
                            .errmsg(format!(
                                "parameter \"{}\" changed to \"{}\"",
                                item.name, item.value
                            ))
                            .into_error(),
                    )?;
                }
            }
            item.applied = true;
        } else if scres == 0 {
            error = true;
            item.errmsg = Some("setting could not be applied".to_string());
            conf_file_with_error.clone_from(&item.filename);
        } else {
            item.applied = true;
        }

        if scres != 0 && apply_settings {
            set_config_sourcefile(&item.name, &item.filename, item.sourceline);
        }
    }

    if apply_settings {
        live::set_pg_reload_time(reload_time);
    }

    bail_out(context, elevel, error, applying, apply_settings, conf_file_with_error)
}

/// The `bail_out:` label tail of `ProcessConfigFileInternal` (guc.c).
fn bail_out(
    context: GucContext,
    elevel: ErrorLevel,
    error: bool,
    applying: bool,
    apply_settings: bool,
    conf_file_with_error: &str,
) -> PgResult<bool> {
    if error && apply_settings {
        if context == PGC_POSTMASTER {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_CONFIG_FILE_ERROR)
                .errmsg(format!(
                    "configuration file \"{conf_file_with_error}\" contains errors"
                ))
                .into_error());
        } else if applying {
            report(
                elevel,
                ereport(elevel)
                    .errcode(ERRCODE_CONFIG_FILE_ERROR)
                    .errmsg(format!(
                        "configuration file \"{conf_file_with_error}\" contains errors; unaffected changes were applied"
                    ))
                    .into_error(),
            )?;
        } else {
            report(
                elevel,
                ereport(elevel)
                    .errcode(ERRCODE_CONFIG_FILE_ERROR)
                    .errmsg(format!(
                        "configuration file \"{conf_file_with_error}\" contains errors; no changes were applied"
                    ))
                    .into_error(),
            )?;
        }
    }
    Ok(!error)
}

/// Emit a report at a sub-ERROR `elevel`, or throw when `elevel >= ERROR`.
fn report(elevel: ErrorLevel, error: types_error::PgError) -> PgResult<()> {
    if elevel >= ERROR {
        Err(error)
    } else {
        if message_level_is_interesting(elevel) {
            emit_error_report_for(&error);
        }
        Ok(())
    }
}

/// `GetConfigOption(name, true, false)` for the change-reporting path.
fn get_config_value(name: &str) -> String {
    live::with_store(|reg| {
        reg.find_option(name)
            .map(|record| crate::show_guc_option(record, false))
    })
    .flatten()
    .unwrap_or_default()
}

/// `set_config_sourcefile(name, filename, sourceline)` (guc.c).
fn set_config_sourcefile(name: &str, filename: &str, sourceline: i32) {
    live::with_store_mut(|reg| {
        if let Some(record) = reg.find_option_mut(name) {
            let gen = record.gen_mut();
            gen.sourcefile = Some(filename.to_string());
            gen.sourceline = sourceline;
        }
    });
}

/// `valid_custom_variable_name` (guc.c lines 1068-1106): two or more identifiers
/// separated by dots, with identifier rules matching scan.l.
fn valid_custom_variable_name(name: &str) -> bool {
    let mut saw_sep = false;
    let mut name_start = true;
    for &c in name.as_bytes() {
        if c == b'.' {
            if name_start {
                return false;
            }
            saw_sep = true;
            name_start = true;
        } else if c.is_ascii_alphabetic() || c == b'_' || c >= 0x80 {
            name_start = false;
        } else if !name_start && (c.is_ascii_digit() || c == b'$') {
            // okay as non-first character
        } else {
            return false;
        }
    }
    if name_start {
        return false;
    }
    saw_sep
}
