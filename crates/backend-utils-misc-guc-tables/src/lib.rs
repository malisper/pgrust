#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! Port of PostgreSQL's `guc_tables.c` — the static GUC variable definitions.
//!
//! The C file stores `config_bool` / `config_int` / `config_real` /
//! `config_string` / `config_enum` arrays whose entries point at global
//! variables, subsystem hook functions, and (for six enum GUCs) option
//! arrays defined in other translation units. This crate carries that
//! metadata — name, context, group, descriptions, flags, boot value, range —
//! plus the parallel name tables (`GucContext_Names`, `GucSource_Names`,
//! `config_group_names`, `config_type_names`).
//!
//! The cross-unit pointers become typed slots ([`slots`]): each storage
//! variable ([`vars`]), hook function ([`hooks`]), and extern option array
//! ([`option_sets`]) is a named slot the owning unit installs from its
//! `init_seams()`. Using a slot before its owner installed it panics with
//! the C symbol name. The GUC *machinery* (guc.c: find_option, the
//! check/assign call paths) is not this crate's content and lands with the
//! GUC core.
//!
//! Entries compiled out of the proven build configuration (`LOCK_DEBUG`,
//! `BTREE_BUILD_STATS`, `WAL_DEBUG`, `TRACE_SYNCSCAN`,
//! `DEBUG_NODE_TESTS_ENABLED` debug GUCs) are excluded, matching the c2rust
//! ground-truth build.

pub mod consts;
pub mod hooks;
pub mod option_sets;
mod slots;
mod tables;
pub mod vars;

pub use slots::*;
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

/// `cluster_name` (guc_tables.c) — the runtime value of the `cluster_name`
/// string GUC. The string is stored in `conf->variable` (`vars::cluster_name`),
/// installed by guc.c when the GUC machinery lands; until then the slot is
/// unset and the boot value `""` (the C `boot_val`) applies.
fn cluster_name_impl() -> String {
    if vars::cluster_name.installed() {
        vars::cluster_name.read().unwrap_or_default()
    } else {
        // boot_val for `cluster_name` (guc_tables.c) is "".
        String::new()
    }
}

// `restrict_nonsystem_relation_kind` (`tcop/tcopprot.h`) — the parsed bitmask
// the `assign_restrict_nonsystem_relation_kind` hook stores into the C int
// global `restrict_nonsystem_relation_kind`. That global lives in
// tcop/postgres.c and is written by the assign hook through the GUC core; this
// crate carries only the GUC-table metadata, not the int storage. The reader
// seam is therefore installed by the `backend-tcop-postgres` owner (it owns the
// int storage + the check/assign hooks), not here.

/// Install this unit's GUC-table seams. The string/enum/etc. var accessor
/// slots themselves are installed by their owning subsystems; these seams read
/// those slots (falling back to the C `boot_val` while a slot is unset).
pub fn init_seams() {
    backend_utils_misc_guc_tables_seams::cluster_name::set(cluster_name_impl);
}

#[cfg(test)]
mod tests;
