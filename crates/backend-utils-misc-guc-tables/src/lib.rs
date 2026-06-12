#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! Port of PostgreSQL's `guc_tables.c` — the static GUC variable definitions.
//!
//! The C file stores `config_bool` / `config_int` / `config_real` /
//! `config_string` / `config_enum` arrays whose entries point at global
//! variables and at subsystem hook functions. This crate carries that
//! metadata: name, context, group, descriptions, flags, boot value, range,
//! the C storage variable (by name — runtime GUC storage is owned by the
//! consuming subsystems as `thread_local`s, per the repo's backend-global
//! rule), and the check/assign/show hooks (by name), plus the parallel name
//! tables (`GucContext_Names`, `GucSource_Names`, `config_group_names`,
//! `config_type_names`).
//!
//! Hook *behavior* belongs to the subsystems that own the hooks; it is
//! abstracted behind the [`GucHookProvider`] trait so this metadata registry
//! does not depend on every subsystem. The GUC core (when it lands) supplies
//! a provider dispatching the named hooks.
//!
//! Entries compiled out of the proven build configuration (`LOCK_DEBUG`,
//! `BTREE_BUILD_STATS`, `WAL_DEBUG`, `TRACE_SYNCSCAN`,
//! `DEBUG_NODE_TESTS_ENABLED` debug GUCs) are excluded, matching the c2rust
//! ground-truth build.

use types_error::{PgError, PgResult};
use types_guc::*;

pub mod consts;
mod tables;

pub use tables::*;

/// `GucContext_Names` (`guc_tables.c`). Indexed by [`GucContext`].
pub static GucContext_Names: &[&str] = &[
    "internal",
    "postmaster",
    "sighup",
    "superuser-backend",
    "backend",
    "superuser",
    "user",
];

/// `GucSource_Names` (`guc_tables.c`). Indexed by [`GucSource`].
pub static GucSource_Names: &[&str] = &[
    "default",
    "default",
    "environment variable",
    "configuration file",
    "command line",
    "global",
    "database",
    "user",
    "database user",
    "client",
    "override",
    "interactive",
    "test",
    "session",
];

/// `config_group_names` (`guc_tables.c`). Indexed by [`config_group`].
pub static config_group_names: &[&str] = &[
    "Ungrouped",
    "File Locations",
    "Connections and Authentication / Connection Settings",
    "Connections and Authentication / TCP Settings",
    "Connections and Authentication / Authentication",
    "Connections and Authentication / SSL",
    "Resource Usage / Memory",
    "Resource Usage / Disk",
    "Resource Usage / Kernel Resources",
    "Resource Usage / Background Writer",
    "Resource Usage / I/O",
    "Resource Usage / Worker Processes",
    "Write-Ahead Log / Settings",
    "Write-Ahead Log / Checkpoints",
    "Write-Ahead Log / Archiving",
    "Write-Ahead Log / Recovery",
    "Write-Ahead Log / Archive Recovery",
    "Write-Ahead Log / Recovery Target",
    "Write-Ahead Log / Summarization",
    "Replication / Sending Servers",
    "Replication / Primary Server",
    "Replication / Standby Servers",
    "Replication / Subscribers",
    "Query Tuning / Planner Method Configuration",
    "Query Tuning / Planner Cost Constants",
    "Query Tuning / Genetic Query Optimizer",
    "Query Tuning / Other Planner Options",
    "Reporting and Logging / Where to Log",
    "Reporting and Logging / When to Log",
    "Reporting and Logging / What to Log",
    "Reporting and Logging / Process Title",
    "Statistics / Monitoring",
    "Statistics / Cumulative Query and Index Statistics",
    "Vacuuming / Automatic Vacuuming",
    "Vacuuming / Cost-Based Vacuum Delay",
    "Vacuuming / Default Behavior",
    "Vacuuming / Freezing",
    "Client Connection Defaults / Statement Behavior",
    "Client Connection Defaults / Locale and Formatting",
    "Client Connection Defaults / Shared Library Preloading",
    "Client Connection Defaults / Other Defaults",
    "Lock Management",
    "Version and Platform Compatibility / Previous PostgreSQL Versions",
    "Version and Platform Compatibility / Other Platforms and Clients",
    "Error Handling",
    "Preset Options",
    "Customized Options",
    "Developer Options",
];

/// `config_type_names` (`guc_tables.c`). Indexed by [`config_type`].
pub static config_type_names: &[&str] = &["bool", "integer", "real", "string", "enum"];

/// Dispatches a GUC's *named* `check_*`/`assign_*`/`show_*` hook to the
/// subsystem that owns it.
///
/// `guc_tables.c` stores a raw C function pointer in each `config_*` entry.
/// In the idiomatic split the metadata table only knows the hook *name* (the
/// C symbol); a provider supplied by the GUC core runs the behavior keyed by
/// that name. The default impl is a no-op / allow-everything provider,
/// matching a `config_*` entry whose hook pointer is `NULL`.
pub trait GucHookProvider {
    fn check_bool(&self, _hook: &str, _newval: bool, _source: GucSource) -> PgResult<bool> {
        Ok(true)
    }

    fn assign_bool(&self, _hook: &str, _newval: bool) -> PgResult<()> {
        Ok(())
    }

    fn check_int(&self, _hook: &str, _newval: i32, _source: GucSource) -> PgResult<bool> {
        Ok(true)
    }

    fn assign_int(&self, _hook: &str, _newval: i32) -> PgResult<()> {
        Ok(())
    }

    fn check_real(&self, _hook: &str, _newval: f64, _source: GucSource) -> PgResult<bool> {
        Ok(true)
    }

    fn assign_real(&self, _hook: &str, _newval: f64) -> PgResult<()> {
        Ok(())
    }

    fn check_string(&self, _hook: &str, _newval: &str, _source: GucSource) -> PgResult<bool> {
        Ok(true)
    }

    fn assign_string(&self, _hook: &str, _newval: &str) -> PgResult<()> {
        Ok(())
    }

    fn show_string(&self, _hook: &str) -> PgResult<Option<String>> {
        Ok(None)
    }

    fn check_enum(&self, _hook: &str, _newval: i32, _source: GucSource) -> PgResult<bool> {
        Ok(true)
    }

    fn assign_enum(&self, _hook: &str, _newval: i32) -> PgResult<()> {
        Ok(())
    }

    fn show_enum(&self, _hook: &str) -> PgResult<Option<String>> {
        Ok(None)
    }
}

/// A provider that runs no hooks (every `config_*` entry behaves as if its
/// hook pointer were `NULL`).
#[derive(Debug, Default)]
pub struct NoopGucHookProvider;

impl GucHookProvider for NoopGucHookProvider {}

/// Iterate over every built-in GUC setting (bool, int, real, string, enum),
/// in the order the C arrays are declared.
pub fn all_settings() -> impl Iterator<Item = GucSetting> {
    ConfigureNamesBool
        .iter()
        .copied()
        .map(GucSetting::Bool)
        .chain(ConfigureNamesInt.iter().copied().map(GucSetting::Int))
        .chain(ConfigureNamesReal.iter().copied().map(GucSetting::Real))
        .chain(ConfigureNamesString.iter().copied().map(GucSetting::String))
        .chain(ConfigureNamesEnum.iter().copied().map(GucSetting::Enum))
}

/// Look up a built-in GUC by name (a metadata-only `find_option`; the GUC
/// core's runtime `find_option` additionally consults the custom-placeholder
/// hash).
pub fn find_option(name: &str) -> Option<GucSetting> {
    all_settings().find(|setting| setting.name() == name)
}

/// Dispatch the `setting`'s named check hook (if any) through `provider`,
/// passing the candidate `value` and the `source` it came from. Returns the
/// hook's verdict; a hookless setting accepts unconditionally.
pub fn check_setting(
    provider: &impl GucHookProvider,
    setting: GucSetting,
    value: GucDefaultValue,
    source: GucSource,
) -> PgResult<bool> {
    match (setting, value) {
        (GucSetting::Bool(setting), GucDefaultValue::Bool(value)) => match setting.check_hook {
            Some(hook) => provider.check_bool(hook, value, source),
            None => Ok(true),
        },
        (GucSetting::Int(setting), GucDefaultValue::Int(value)) => match setting.check_hook {
            Some(hook) => provider.check_int(hook, value, source),
            None => Ok(true),
        },
        (GucSetting::Real(setting), GucDefaultValue::Real(value)) => match setting.check_hook {
            Some(hook) => provider.check_real(hook, value, source),
            None => Ok(true),
        },
        (GucSetting::String(setting), GucDefaultValue::String(value)) => match setting.check_hook {
            Some(hook) => provider.check_string(hook, value.unwrap_or_default(), source),
            None => Ok(true),
        },
        (GucSetting::Enum(setting), GucDefaultValue::Enum(value)) => match setting.check_hook {
            Some(hook) => provider.check_enum(hook, value, source),
            None => Ok(true),
        },
        _ => Err(PgError::error(
            "GUC setting value type does not match setting type",
        )),
    }
}

/// Dispatch the `setting`'s named assign hook (if any) through `provider`
/// with the new `value`. A hookless setting is a no-op.
pub fn assign_setting(
    provider: &impl GucHookProvider,
    setting: GucSetting,
    value: GucDefaultValue,
) -> PgResult<()> {
    match (setting, value) {
        (GucSetting::Bool(setting), GucDefaultValue::Bool(value)) => match setting.assign_hook {
            Some(hook) => provider.assign_bool(hook, value),
            None => Ok(()),
        },
        (GucSetting::Int(setting), GucDefaultValue::Int(value)) => match setting.assign_hook {
            Some(hook) => provider.assign_int(hook, value),
            None => Ok(()),
        },
        (GucSetting::Real(setting), GucDefaultValue::Real(value)) => match setting.assign_hook {
            Some(hook) => provider.assign_real(hook, value),
            None => Ok(()),
        },
        (GucSetting::String(setting), GucDefaultValue::String(value)) => {
            match setting.assign_hook {
                Some(hook) => provider.assign_string(hook, value.unwrap_or_default()),
                None => Ok(()),
            }
        }
        (GucSetting::Enum(setting), GucDefaultValue::Enum(value)) => match setting.assign_hook {
            Some(hook) => provider.assign_enum(hook, value),
            None => Ok(()),
        },
        _ => Err(PgError::error(
            "GUC setting value type does not match setting type",
        )),
    }
}

#[cfg(test)]
mod tests;
