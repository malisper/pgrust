use elog::{emit_error_report_for, ereport, message_level_is_interesting};
use guc_file::{ConfigVariable, ParseConfigFile, CONF_FILE_START_DEPTH};
use types_error::{
    ErrorLevel, PgError, PgResult, ERRCODE_CANT_CHANGE_RUNTIME_PARAM, ERRCODE_CONFIG_FILE_ERROR,
    ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_guc::{
    GucContext, PGC_BACKEND, PGC_POSTMASTER, PGC_SIGHUP, PGC_S_DEFAULT, PGC_S_DYNAMIC_DEFAULT,
    PGC_S_FILE,
};

use crate::model::{GUC_IS_IN_FILE, GUC_PENDING_RESTART};
use crate::store::{self, set_pg_reload_time, with_store, with_store_mut};
use crate::GUC_ACTION_SET;

// PG_AUTOCONF_FILENAME (utils/guc.h).
pub const PG_AUTOCONF_FILENAME: &str = "postgresql.auto.conf";

// pg_timezone_abbrev_initialize (guc.c:1992).
pub fn pg_timezone_abbrev_initialize() -> PgResult<()> {
    crate::SetConfigOption(
        "timezone_abbreviations",
        Some("Default"),
        PGC_POSTMASTER,
        PGC_S_DYNAMIC_DEFAULT,
    )
}

// ProcessConfigFileInternal (guc.c:282). Ok(true) clean, Ok(false) recorded
// sub-ERROR errors, Err at elevel >= ERROR (or PGC_POSTMASTER apply errors).
pub fn process_config_file_internal(
    context: GucContext,
    apply_settings: bool,
    elevel: ErrorLevel,
) -> PgResult<bool> {
    Ok(process_config_file_internal_list(context, apply_settings, elevel)?.0)
}

// C's ProcessConfigFileInternal returns the parsed ConfigVariable list (its
// show_all_file_settings caller reads it); the boolean is the bail_out verdict.
pub fn process_config_file_internal_list(
    context: GucContext,
    apply_settings: bool,
    elevel: ErrorLevel,
) -> PgResult<(bool, Vec<ConfigVariable>)> {
    let config_file_name = store::get_string("config_file").flatten().unwrap_or_default();

    let mut conf_file_with_error = config_file_name.clone();
    let mut head: Vec<ConfigVariable> = Vec::new();

    if !ParseConfigFile(&config_file_name, true, None, 0, CONF_FILE_START_DEPTH, elevel, &mut head)? {
        let ok = bail_out(context, elevel, true, false, apply_settings, &conf_file_with_error)?;
        return Ok((ok, head));
    }

    // postgresql.auto.conf lives in the data directory; parse it after the
    // main file (so ALTER SYSTEM overrides) once DataDir is known.
    if init_small::globals::DataDir().is_some() {
        if !ParseConfigFile(PG_AUTOCONF_FILENAME, false, None, 0, CONF_FILE_START_DEPTH, elevel, &mut head)? {
            conf_file_with_error = PG_AUTOCONF_FILENAME.to_string();
            let ok = bail_out(context, elevel, true, false, apply_settings, &conf_file_with_error)?;
            return Ok((ok, head));
        }
    } else {
        // Without DataDir accept only the last data_directory item: anything
        // else could be overridden by the yet-unreadable auto file.
        let last_dd = head
            .iter()
            .rposition(|item| !item.ignore && item.name.as_deref() == Some("data_directory"));
        match last_dd {
            Some(idx) => {
                let keep = head.swap_remove(idx);
                head.clear();
                head.push(keep);
            }
            None => {
                // No data_directory: quick exit, PgReloadTime is set by the
                // subsequent full load.
                let ok =
                    bail_out(context, elevel, false, false, apply_settings, &conf_file_with_error)?;
                return Ok((ok, head));
            }
        }
    }

    let ok = apply_config_variables(
        &mut head,
        context,
        apply_settings,
        elevel,
        &mut conf_file_with_error,
    )?;
    Ok((ok, head))
}

// The apply phase of ProcessConfigFileInternal, over the parsed list and the
// live store.
pub fn apply_config_variables(
    items: &mut [ConfigVariable],
    context: GucContext,
    apply_settings: bool,
    elevel: ErrorLevel,
    conf_file_with_error: &mut String,
) -> PgResult<bool> {
    let mut error = false;
    let mut applying = false;

    with_store_mut(|reg| {
        for idx in 0..reg.len() {
            reg[idx].gen_mut().status &= !GUC_IS_IN_FILE;
        }
    });

    // Quasi-syntactic name validation + GUC_IS_IN_FILE marking + duplicate
    // pruning. Unknown custom names must be accepted without complaint so the
    // postmaster and backends agree.
    for i in 0..items.len() {
        if items[i].ignore {
            continue;
        }
        let name = items[i].name.clone().unwrap_or_default();

        let found = with_store_mut(|reg| {
            reg.find_index(&name).map(|idx| {
                let already = reg[idx].gen().status & GUC_IS_IN_FILE != 0;
                reg[idx].gen_mut().status |= GUC_IS_IN_FILE;
                already
            })
        })
        .flatten();

        match found {
            Some(true) => {
                // Duplicate entry: mark the earlier occurrence(s) dead. C
                // compares with strcmp (exact spelling), not guc_name_compare.
                for pitem in items.iter_mut().take(i) {
                    if !pitem.ignore && pitem.name.as_deref() == Some(name.as_str()) {
                        pitem.ignore = true;
                    }
                }
            }
            Some(false) => {}
            None => {
                if !crate::valid_custom_variable_name(&name) {
                    report(
                        elevel,
                        ereport(elevel)
                            .errcode(ERRCODE_UNDEFINED_OBJECT)
                            .errmsg(format!(
                                "unrecognized configuration parameter \"{}\" in file \"{}\" line {}",
                                name,
                                display_filename(&items[i]),
                                items[i].sourceline
                            ))
                            .into_error(),
                    )?;
                    items[i].errmsg = Some("unrecognized configuration parameter".to_string());
                    error = true;
                    *conf_file_with_error = display_filename(&items[i]);
                }
            }
        }
    }

    if error {
        return bail_out(context, elevel, error, applying, apply_settings, conf_file_with_error);
    }

    applying = true;

    // Variables removed from the file: revert reset (and perhaps effective)
    // values to boot defaults.
    struct Removed {
        name: String,
        needs_restart: bool,
        do_reset: bool,
    }
    let removed: Vec<Removed> = with_store_mut(|reg| {
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
            // Clear "file" sources to "default" so set_config_option can
            // override them.
            if reg[idx].gen().reset_source == PGC_S_FILE {
                reg[idx].gen_mut().reset_source = PGC_S_DEFAULT;
            }
            if reg[idx].gen().source == PGC_S_FILE {
                reg.set_source_pub(idx, PGC_S_DEFAULT);
            }
            let mut stack = reg[idx].gen_mut().stack.as_deref_mut();
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
        let scres = crate::set_config_option(
            &r.name,
            None,
            context,
            PGC_S_DEFAULT,
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

    // Restore env-var / dynamic defaults (a no-op unless one of those was just
    // removed from the file). Must not run during the postmaster's initial
    // load.
    if context == PGC_SIGHUP && apply_settings {
        store::initialize_guc_options_from_environment()?;
        pg_timezone_abbrev_initialize()?;
        // C: SetConfigOption("client_encoding", GetDatabaseEncodingName(),
        // PGC_BACKEND, PGC_S_DYNAMIC_DEFAULT); the name accessor lands with
        // the mbutils unit.
        if mbutils_seams::get_database_encoding_name::is_installed() {
            crate::SetConfigOption(
                "client_encoding",
                Some(mbutils_seams::get_database_encoding_name::call()),
                PGC_BACKEND,
                PGC_S_DYNAMIC_DEFAULT,
            )?;
        }
    }

    // Apply the values from the config file.
    for item in items.iter_mut() {
        if item.ignore {
            continue;
        }
        let name = item.name.clone().unwrap_or_default();
        let value = item.value.clone().unwrap_or_default();

        // In SIGHUP cases in the postmaster, report changes.
        let report_changes =
            context == PGC_SIGHUP && apply_settings && !init_small::globals::IsUnderPostmaster();
        let pre_value = report_changes.then(|| current_value(&name));

        let scres = crate::set_config_option(
            &name,
            Some(&value),
            context,
            PGC_S_FILE,
            GUC_ACTION_SET,
            apply_settings,
            ErrorLevel(0),
            false,
        )?;

        if scres > 0 {
            if let Some(pre) = pre_value.as_ref() {
                let post = current_value(&name);
                if *pre != post {
                    report(
                        elevel,
                        ereport(elevel)
                            .errmsg(format!("parameter \"{name}\" changed to \"{value}\""))
                            .into_error(),
                    )?;
                }
            }
            item.applied = true;
        } else if scres == 0 {
            error = true;
            item.errmsg = Some("setting could not be applied".to_string());
            *conf_file_with_error = display_filename(item);
        } else {
            item.applied = true;
        }

        // Update source location unless there was an error: even if the
        // active value didn't change, the reset value might have.
        if scres != 0 && apply_settings {
            set_config_sourcefile(&name, &display_filename(item), item.sourceline);
        }
    }

    if apply_settings {
        set_pg_reload_time(timestamp_seams::get_current_timestamp::call());
        // This thread has now consumed the reload (C parity: backends run
        // this between statements): adopt the current process-wide base as
        // its started-with view. The postmaster publishes the new base
        // BEFORE signaling children (process_pm_reload_request), so a
        // backend's pass adopts the post-reload base; the postmaster's own
        // pass runs pre-publish and its session base is unused.
        if context == PGC_SIGHUP {
            crate::layers::adopt_current_base();
        }
    }

    bail_out(context, elevel, error, applying, apply_settings, conf_file_with_error)
}

fn display_filename(item: &ConfigVariable) -> String {
    item.filename
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default()
}

// The bail_out: tail of ProcessConfigFileInternal.
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
            // During postmaster startup, any error is fatal.
            return Err(ereport(ERROR)
                .errcode(ERRCODE_CONFIG_FILE_ERROR)
                .errmsg(format!("configuration file \"{conf_file_with_error}\" contains errors"))
                .into_error()
                .into());
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

fn report(elevel: ErrorLevel, error: PgError) -> PgResult<()> {
    if elevel >= ERROR {
        Err(error.into())
    } else {
        if message_level_is_interesting(elevel) {
            emit_error_report_for(&error);
        }
        Ok(())
    }
}

// GetConfigOption(name, true, false) for the change-report diff; missing or
// NULL is the empty string.
fn current_value(name: &str) -> String {
    with_store(|reg| reg.find_option(name).map(|record| crate::show_guc_option(record, false)))
        .flatten()
        .unwrap_or_default()
}

// set_config_sourcefile (guc.c:4310).
fn set_config_sourcefile(name: &str, filename: &str, sourceline: i32) {
    with_store_mut(|reg| {
        if let Some(record) = reg.find_option_mut(name) {
            let gen = record.gen_mut();
            gen.sourcefile = Some(filename.to_string());
            gen.sourceline = sourceline;
        }
    });
}
