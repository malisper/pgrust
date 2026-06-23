#![allow(non_snake_case)]

//! Port of PostgreSQL's `help_config.c`
//! (`src/backend/utils/misc/help_config.c`).
//!
//! `GucInfoMain` (the `postgres --describe-config` entry point) prints every
//! *visible* built-in GUC as a tab-separated row and then `exit(0)`s. Options
//! whose flag bits include `GUC_NO_SHOW_ALL`, `GUC_NOT_IN_SAMPLE`, or
//! `GUC_DISALLOW_IN_FILE` are skipped (`displayStruct`).
//!
//! C `printMixedStruct` emits, per visible option:
//!
//! ```text
//! name \t context \t group \t TYPE \t reset \t min \t max \t short_desc \t long_desc
//! ```
//!
//! C `printf`s to stdout and never pallocs, so this port returns the rendered
//! text (an owned `String`) rather than writing to stdout-and-exit: the caller
//! decides where the output goes. The C `union mixedStruct` overlaying the five
//! `config_*` shapes is replaced by the `GucSetting` enum from
//! `backend-utils-misc-guc-tables`, which carries each variant's own fields.
//!
//! No external seams: the GUC metadata table (`build_guc_variables` /
//! `get_guc_variables` in C) is supplied directly by
//! `backend-utils-misc-guc-tables::all_settings`, and the rendered text is
//! returned rather than written, so no stdout/stderr sink is needed.

use std::fmt::Write as _;

use utils_error::ereport;
use guc_tables::{
    all_settings, config_group_names, GucContext_Names, GucDefaultValue, GucEnumSetting,
    GucSetting,
};
use types_error::{PgError, PgResult, ERROR};
use types_guc::{GUC_DISALLOW_IN_FILE, GUC_NOT_IN_SAMPLE, GUC_NO_SHOW_ALL};

/// Flag bits that hide a GUC from the `--describe-config` listing
/// (`displayStruct`).
const HIDDEN_FLAGS: i32 = GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE;

/// One rendered GUC row: the nine tab-separated columns `printMixedStruct`
/// emits. `&'static str` for the columns straight out of the static metadata
/// table, `String` for the numeric columns that are formatted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GucInfoRow {
    pub name: &'static str,
    pub context: &'static str,
    pub group: &'static str,
    pub vartype: &'static str,
    pub reset_value: String,
    pub min_value: String,
    pub max_value: String,
    pub short_desc: &'static str,
    pub long_desc: &'static str,
}

/// `GucInfoMain` (`help_config.c:46`). The C version builds the GUC table,
/// prints every visible option, and `exit(0)`s; this returns the rendered text
/// so the caller can write it and exit.
pub fn GucInfoMain() -> PgResult<String> {
    render_guc_info()
}

/// Render the full `--describe-config` text: one [`GucInfoRow`] per visible
/// GUC, each as a tab-separated line terminated by `\n`.
pub fn render_guc_info() -> PgResult<String> {
    let mut output = String::new();
    for row in guc_info_rows()? {
        writeln!(
            output,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.name,
            row.context,
            row.group,
            row.vartype,
            row.reset_value,
            row.min_value,
            row.max_value,
            row.short_desc,
            row.long_desc
        )
        .expect("writing to a String cannot fail");
    }
    Ok(output)
}

/// Walk every built-in GUC (`get_guc_variables`), keep the visible ones
/// (`displayStruct`), and render each (`printMixedStruct`). Sorted by name so
/// the output matches `build_guc_variables`' alphabetized ordering.
pub fn guc_info_rows() -> PgResult<Vec<GucInfoRow>> {
    let mut settings: Vec<_> = all_settings().filter(|setting| displayStruct(*setting)).collect();
    settings.sort_by(|left, right| left.name().cmp(right.name()));

    settings.into_iter().map(printMixedStruct).collect()
}

/// `displayStruct` (`help_config.c:73`): true unless one of the hidden-flag
/// bits is set.
pub fn displayStruct(setting: GucSetting) -> bool {
    setting.flags() & HIDDEN_FLAGS == 0
}

/// `printMixedStruct` (`help_config.c:86`): turn one setting into its rendered
/// row. Looks up the context and group name tables and formats the
/// type-specific value columns.
pub fn printMixedStruct(setting: GucSetting) -> PgResult<GucInfoRow> {
    let context = GucContext_Names
        .get(context_index(setting))
        .copied()
        .unwrap_or("");
    let group = config_group_names
        .get(setting.group() as usize)
        .copied()
        .unwrap_or("");
    let short_desc = short_desc(setting).unwrap_or("");
    let long_desc = long_desc(setting).unwrap_or("");
    let (vartype, reset_value, min_value, max_value) = value_columns(setting)?;

    Ok(GucInfoRow {
        name: setting.name(),
        context,
        group,
        vartype,
        reset_value,
        min_value,
        max_value,
        short_desc,
        long_desc,
    })
}

/// `config_enum_lookup_by_value` (guc.c:3023, invoked from `printMixedStruct`).
/// Returns the option *name* whose `val` matches the enum setting's value, and
/// `elog(ERROR)`s if none matches.
///
/// `options.entries()` resolves both the inline arrays and the six `extern
/// const struct config_enum_entry ...[]` arrays owned by other C modules; an
/// external array whose owning unit has not landed panics loudly there.
pub fn config_enum_lookup_by_value(setting: GucEnumSetting, val: i32) -> PgResult<&'static str> {
    setting
        .options
        .entries()
        .iter()
        .find(|option| option.val == val)
        .map(|option| option.name)
        .map_or_else(|| enum_lookup_error(setting, val), Ok)
}

/// Format the `(TYPE, reset, min, max)` columns for one setting, matching the C
/// `switch (vartype)` in `printMixedStruct`. Bool/string/enum leave min/max
/// empty, as the C code prints `\t\t\t` for them.
///
/// The `reset` column for BOOL/INT/REAL is the C struct's `reset_val`, **not**
/// its `boot_val`. `printMixedStruct` is reached only from `GucInfoMain`
/// (`postgres --describe-config`), which calls `build_guc_variables()`
/// (guc.c:903) and nothing else. `build_guc_variables` sets `vartype` and the
/// hash table but never touches `reset_val`; `reset_val` is populated only at
/// runtime by `InitializeOneGUCOption` (guc.c:1644), which this path never
/// reaches. The static `ConfigureNames*` initializers leave the trailing
/// `reset_val` field unspecified, so it is zero-initialized. C therefore prints
/// the zero-initialized `reset_val` for every option: `FALSE` for every BOOL,
/// `0` for every INT, `0` for every REAL — regardless of the boot default. We
/// reproduce that here with constant zero defaults rather than reading
/// `boot_val`. `min`/`max` (set by the static initializers) and the STRING/ENUM
/// `boot_val` columns are read from the real fields.
fn value_columns(setting: GucSetting) -> PgResult<(&'static str, String, String, String)> {
    match setting {
        // C: `(reset_val == 0) ? "FALSE" : "TRUE"` with reset_val zero-initialized.
        GucSetting::Bool(_) => Ok((
            "BOOLEAN",
            "FALSE".to_string(),
            String::new(),
            String::new(),
        )),
        // C: `printf("%d", reset_val)` with reset_val zero-initialized.
        GucSetting::Int(setting) => Ok((
            "INTEGER",
            0_i32.to_string(),
            setting.min.to_string(),
            setting.max.to_string(),
        )),
        // C: `printf("%g", reset_val)` with reset_val zero-initialized.
        GucSetting::Real(setting) => Ok((
            "REAL",
            format_real(0.0),
            format_real(setting.min),
            format_real(setting.max),
        )),
        GucSetting::String(setting) => Ok((
            "STRING",
            format_string_default(setting.boot_val)?.to_string(),
            String::new(),
            String::new(),
        )),
        GucSetting::Enum(setting) => Ok((
            "ENUM",
            config_enum_lookup_by_value(setting, enum_value(setting.boot_val)?)?.to_string(),
            String::new(),
            String::new(),
        )),
    }
}

/// Format the reset value for a STRING row (C: `boot_val ? boot_val : ""`).
fn format_string_default(value: GucDefaultValue) -> PgResult<&'static str> {
    match value {
        GucDefaultValue::String(value) => Ok(value.unwrap_or("")),
        _ => Err(type_error()),
    }
}

/// Extract the integer value of an enum setting's boot value (the C
/// `_enum.boot_val` passed to `config_enum_lookup_by_value`).
fn enum_value(value: GucDefaultValue) -> PgResult<i32> {
    match value {
        GucDefaultValue::Enum(value) => Ok(value),
        _ => Err(type_error()),
    }
}

/// Render an `f64` like C's `%g`, normalizing the `-0` that Rust's default
/// formatting can produce back to `0` (C `%g` prints `0` for negative zero).
fn format_real(value: f64) -> String {
    let text = format!("{value}");
    if text == "-0" {
        "0".to_string()
    } else {
        text
    }
}

/// Index into `GucContext_Names` for this setting (C: `generic.context`).
fn context_index(setting: GucSetting) -> usize {
    setting.context() as i32 as usize
}

/// The `short_desc` field common to all five shapes (C: `generic.short_desc`).
fn short_desc(setting: GucSetting) -> Option<&'static str> {
    match setting {
        GucSetting::Bool(setting) => setting.short_desc,
        GucSetting::Int(setting) => setting.short_desc,
        GucSetting::Real(setting) => setting.short_desc,
        GucSetting::String(setting) => setting.short_desc,
        GucSetting::Enum(setting) => setting.short_desc,
    }
}

/// The `long_desc` field common to all five shapes (C: `generic.long_desc`).
fn long_desc(setting: GucSetting) -> Option<&'static str> {
    match setting {
        GucSetting::Bool(setting) => setting.long_desc,
        GucSetting::Int(setting) => setting.long_desc,
        GucSetting::Real(setting) => setting.long_desc,
        GucSetting::String(setting) => setting.long_desc,
        GucSetting::Enum(setting) => setting.long_desc,
    }
}

/// `elog(ERROR, "could not find enum option %d for %s", ...)`
/// (`config_enum_lookup_by_value`, guc.c:3033).
fn enum_lookup_error(setting: GucEnumSetting, val: i32) -> PgResult<&'static str> {
    Err(ereport(ERROR)
        .errmsg(format!(
            "could not find enum option {} for {}",
            val, setting.name
        ))
        .into_error())
}

/// The internal error the C `printMixedStruct` would `write_stderr` for a
/// `config_*` shape whose stored value type does not match its declared
/// `vartype`. The `GucSetting` enum makes that mismatch impossible in practice;
/// surfacing it as an error never silently invents a wrong column.
fn type_error() -> PgError {
    ereport(ERROR)
        .errmsg("GUC setting value type does not match setting type")
        .into_error()
}

#[cfg(test)]
mod tests {
    use super::*;

    use guc_tables::GucEnumOptions;

    fn find_option(name: &str) -> Option<GucSetting> {
        all_settings().find(|setting| setting.name() == name)
    }

    /// Render only the settings whose enum option arrays are resolvable in
    /// isolation. Six enum GUCs (`archive_mode`, `wal_level`, ...) reference
    /// `extern const struct config_enum_entry ...[]` arrays owned by other
    /// units; their `GucEnumOptions::External` slots panic until those units
    /// install them (the production `printMixedStruct` path likewise relies on
    /// the owner being present), so they are skipped here.
    fn renderable_rows() -> Vec<GucInfoRow> {
        all_settings()
            .filter(|setting| displayStruct(*setting))
            .filter(|setting| !matches!(setting.options(), Some(GucEnumOptions::External(_))))
            .map(|setting| printMixedStruct(setting).unwrap())
            .collect()
    }

    #[test]
    fn hides_settings_postgres_does_not_show_all() {
        let setting = find_option("default_with_oids").unwrap();
        assert_ne!(setting.flags() & GUC_NO_SHOW_ALL, 0);
        assert!(!displayStruct(setting));
    }

    #[test]
    fn renders_tab_separated_rows() {
        let rows = renderable_rows();
        assert!(!rows.is_empty());

        let mut output = String::new();
        for row in &rows {
            use std::fmt::Write as _;
            writeln!(
                output,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                row.name,
                row.context,
                row.group,
                row.vartype,
                row.reset_value,
                row.min_value,
                row.max_value,
                row.short_desc,
                row.long_desc
            )
            .unwrap();
        }
        let first_line = output.lines().next().unwrap();
        assert_eq!(first_line.split('\t').count(), 9);
    }

    #[test]
    fn renders_common_setting_like_help_config() {
        let setting = printMixedStruct(find_option("archive_command").unwrap()).unwrap();
        assert_eq!(setting.context, "sighup");
        assert_eq!(setting.vartype, "STRING");
        assert_eq!(setting.reset_value, "");
        assert_eq!(
            setting.short_desc,
            "Sets the shell command that will be called to archive a WAL file."
        );
    }

    #[test]
    fn enum_lookup_uses_visible_option_name() {
        let setting = match find_option("bytea_output").unwrap() {
            GucSetting::Enum(setting) => setting,
            other => panic!("unexpected setting kind: {:?}", other.value_kind()),
        };

        let val = match setting.boot_val {
            GucDefaultValue::Enum(val) => val,
            other => panic!("unexpected boot value: {other:?}"),
        };
        assert_eq!(config_enum_lookup_by_value(setting, val).unwrap(), "hex");
    }

    #[test]
    fn renders_common_setting_full_line() {
        let row = printMixedStruct(find_option("archive_command").unwrap()).unwrap();
        let line = format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.name,
            row.context,
            row.group,
            row.vartype,
            row.reset_value,
            row.min_value,
            row.max_value,
            row.short_desc,
            row.long_desc
        );
        assert!(line.starts_with("archive_command\tsighup\tWrite-Ahead Log / Archiving\tSTRING\t\t\t\t"));
    }
}
