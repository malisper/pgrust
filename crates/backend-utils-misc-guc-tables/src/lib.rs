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

pub mod backing;
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
    // `PostPortNumber` (postmaster.c GUC) — served from the GUC slot it lives
    // in (read by the lock-file writer via the port-path seam). Until the GUC
    // machinery installs the slot, the C `boot_val` (DEF_PGPORT == 5432)
    // applies — the same boot-val fallback `cluster_name_impl` uses.
    backend_port_path_seams::post_port_number::set(post_port_number_impl);

    install_guc_tables_owned_vars();

    // `log_transaction_sample_rate` (`double log_xact_sample_rate`,
    // guc_tables.c) — read by xact.c. The var is owned and installed here
    // (install_guc_tables_owned_vars, above), so the accessor reads its slot;
    // boot_val is 0.0 until set.
    backend_utils_misc_guc_file_seams::log_xact_sample_rate::set(
        log_xact_sample_rate_impl,
    );
}

fn log_xact_sample_rate_impl() -> f64 {
    if vars::log_xact_sample_rate.installed() {
        vars::log_xact_sample_rate.read()
    } else {
        // boot_val for `log_transaction_sample_rate` (guc_tables.c) is 0.0.
        0.0
    }
}

/// Install the `GucVarAccessors` for every GUC whose C `conf->variable`
/// (`guc_tables.c`) is a global defined in guc_tables.c itself — bool/int/
/// real/string/enum statics on lines 515-648 of the C file. Their backing
/// lives in [`backing`] (seeded to each entry's C `boot_val`); the GUC
/// machinery reads/writes through these accessors. GUCs whose
/// `conf->variable` is owned by another subsystem (e.g. `NBuffers`,
/// `max_wal_senders`) are installed by that subsystem, not here.
fn install_guc_tables_owned_vars() {
    use crate::GucVarAccessors;

    // --- bool ---
    vars::AllowAlterSystem.install(GucVarAccessors {
        get: backing::AllowAlterSystem,
        set: backing::set_AllowAlterSystem,
    });
    vars::log_duration.install(GucVarAccessors {
        get: backing::log_duration,
        set: backing::set_log_duration,
    });
    vars::Debug_print_plan.install(GucVarAccessors {
        get: backing::Debug_print_plan,
        set: backing::set_Debug_print_plan,
    });
    vars::Debug_print_parse.install(GucVarAccessors {
        get: backing::Debug_print_parse,
        set: backing::set_Debug_print_parse,
    });
    vars::Debug_print_rewritten.install(GucVarAccessors {
        get: backing::Debug_print_rewritten,
        set: backing::set_Debug_print_rewritten,
    });
    vars::Debug_pretty_print.install(GucVarAccessors {
        get: backing::Debug_pretty_print,
        set: backing::set_Debug_pretty_print,
    });
    // (debug_copy_parse_plan_trees / debug_write_read_parse_plan_trees /
    // debug_raw_expression_coverage_test are DEBUG_NODE_TESTS_ENABLED GUCs,
    // compiled out of the proven build — no `vars` slot, not installed.)
    vars::log_parser_stats.install(GucVarAccessors {
        get: backing::log_parser_stats,
        set: backing::set_log_parser_stats,
    });
    vars::log_planner_stats.install(GucVarAccessors {
        get: backing::log_planner_stats,
        set: backing::set_log_planner_stats,
    });
    vars::log_executor_stats.install(GucVarAccessors {
        get: backing::log_executor_stats,
        set: backing::set_log_executor_stats,
    });
    vars::log_statement_stats.install(GucVarAccessors {
        get: backing::log_statement_stats,
        set: backing::set_log_statement_stats,
    });
    // (log_btree_build_stats is a BTREE_BUILD_STATS GUC, compiled out of the
    // proven build — no `vars` slot, not installed.)
    vars::row_security.install(GucVarAccessors {
        get: backing::row_security,
        set: backing::set_row_security,
    });
    vars::check_function_bodies.install(GucVarAccessors {
        get: backing::check_function_bodies,
        set: backing::set_check_function_bodies,
    });
    vars::default_with_oids.install(GucVarAccessors {
        get: backing::default_with_oids,
        set: backing::set_default_with_oids,
    });
    vars::current_role_is_superuser.install(GucVarAccessors {
        get: backing::current_role_is_superuser,
        set: backing::set_current_role_is_superuser,
    });
    vars::assert_enabled.install(GucVarAccessors {
        get: backing::assert_enabled,
        set: backing::set_assert_enabled,
    });
    vars::in_hot_standby_guc.install(GucVarAccessors {
        get: backing::in_hot_standby_guc,
        set: backing::set_in_hot_standby_guc,
    });
    vars::data_checksums.install(GucVarAccessors {
        get: backing::data_checksums,
        set: backing::set_data_checksums,
    });
    vars::integer_datetimes.install(GucVarAccessors {
        get: backing::integer_datetimes,
        set: backing::set_integer_datetimes,
    });

    // --- int ---
    vars::log_parameter_max_length.install(GucVarAccessors {
        get: backing::log_parameter_max_length,
        set: backing::set_log_parameter_max_length,
    });
    vars::log_parameter_max_length_on_error.install(GucVarAccessors {
        get: backing::log_parameter_max_length_on_error,
        set: backing::set_log_parameter_max_length_on_error,
    });
    vars::log_temp_files.install(GucVarAccessors {
        get: backing::log_temp_files,
        set: backing::set_log_temp_files,
    });
    vars::temp_file_limit.install(GucVarAccessors {
        get: backing::temp_file_limit,
        set: backing::set_temp_file_limit,
    });
    vars::num_temp_buffers.install(GucVarAccessors {
        get: backing::num_temp_buffers,
        set: backing::set_num_temp_buffers,
    });
    vars::ssl_renegotiation_limit.install(GucVarAccessors {
        get: backing::ssl_renegotiation_limit,
        set: backing::set_ssl_renegotiation_limit,
    });
    vars::huge_page_size.install(GucVarAccessors {
        get: backing::huge_page_size,
        set: backing::set_huge_page_size,
    });
    vars::max_function_args.install(GucVarAccessors {
        get: backing::max_function_args,
        set: backing::set_max_function_args,
    });
    vars::max_index_keys.install(GucVarAccessors {
        get: backing::max_index_keys,
        set: backing::set_max_index_keys,
    });
    vars::max_identifier_length.install(GucVarAccessors {
        get: backing::max_identifier_length,
        set: backing::set_max_identifier_length,
    });
    vars::block_size.install(GucVarAccessors {
        get: backing::block_size,
        set: backing::set_block_size,
    });
    vars::segment_size.install(GucVarAccessors {
        get: backing::segment_size,
        set: backing::set_segment_size,
    });
    vars::wal_block_size.install(GucVarAccessors {
        get: backing::wal_block_size,
        set: backing::set_wal_block_size,
    });
    vars::server_version_num.install(GucVarAccessors {
        get: backing::server_version_num,
        set: backing::set_server_version_num,
    });
    vars::shared_memory_size_mb.install(GucVarAccessors {
        get: backing::shared_memory_size_mb,
        set: backing::set_shared_memory_size_mb,
    });
    vars::shared_memory_size_in_huge_pages.install(GucVarAccessors {
        get: backing::shared_memory_size_in_huge_pages,
        set: backing::set_shared_memory_size_in_huge_pages,
    });
    vars::num_os_semaphores.install(GucVarAccessors {
        get: backing::num_os_semaphores,
        set: backing::set_num_os_semaphores,
    });

    // postmaster.c-owned connection-limit GUCs (backed here until postmaster
    // lands as a GUC owner). Read by InitPostgres/CheckRequiredParameterValues.
    vars::SuperuserReservedConnections.install(GucVarAccessors {
        get: backing::SuperuserReservedConnections,
        set: backing::set_SuperuserReservedConnections,
    });
    vars::ReservedConnections.install(GucVarAccessors {
        get: backing::ReservedConnections,
        set: backing::set_ReservedConnections,
    });
    // postgres.c-owned `post_auth_delay` GUC (backed here until that unit lands
    // as a GUC owner). Read by InitPostgres's post-auth delay apply.
    vars::PostAuthDelay.install(GucVarAccessors {
        get: backing::PostAuthDelay,
        set: backing::set_PostAuthDelay,
    });

    // --- enum (stored as int) ---
    vars::huge_pages.install(GucVarAccessors {
        get: backing::huge_pages,
        set: backing::set_huge_pages,
    });
    vars::huge_pages_status.install(GucVarAccessors {
        get: backing::huge_pages_status,
        set: backing::set_huge_pages_status,
    });

    // --- real ---
    vars::phony_random_seed.install(GucVarAccessors {
        get: backing::phony_random_seed,
        set: backing::set_phony_random_seed,
    });
    vars::log_statement_sample_rate.install(GucVarAccessors {
        get: backing::log_statement_sample_rate,
        set: backing::set_log_statement_sample_rate,
    });
    vars::log_xact_sample_rate.install(GucVarAccessors {
        get: backing::log_xact_sample_rate,
        set: backing::set_log_xact_sample_rate,
    });

    // --- string ---
    vars::event_source.install(GucVarAccessors {
        get: backing::event_source,
        set: backing::set_event_source,
    });
    vars::client_encoding_string.install(GucVarAccessors {
        get: backing::client_encoding_string,
        set: backing::set_client_encoding_string,
    });
    vars::datestyle_string.install(GucVarAccessors {
        get: backing::datestyle_string,
        set: backing::set_datestyle_string,
    });
    vars::server_encoding_string.install(GucVarAccessors {
        get: backing::server_encoding_string,
        set: backing::set_server_encoding_string,
    });
    vars::server_version_string.install(GucVarAccessors {
        get: backing::server_version_string,
        set: backing::set_server_version_string,
    });
    vars::role_string.install(GucVarAccessors {
        get: backing::role_string,
        set: backing::set_role_string,
    });
    vars::session_authorization_string.install(GucVarAccessors {
        get: backing::session_authorization_string,
        set: backing::set_session_authorization_string,
    });
    vars::syslog_ident_str.install(GucVarAccessors {
        get: backing::syslog_ident_str,
        set: backing::set_syslog_ident_str,
    });
    vars::timezone_string.install(GucVarAccessors {
        get: backing::timezone_string,
        set: backing::set_timezone_string,
    });
    vars::log_timezone_string.install(GucVarAccessors {
        get: backing::log_timezone_string,
        set: backing::set_log_timezone_string,
    });
    vars::timezone_abbreviations_string.install(GucVarAccessors {
        get: backing::timezone_abbreviations_string,
        set: backing::set_timezone_abbreviations_string,
    });
    vars::data_directory.install(GucVarAccessors {
        get: backing::data_directory,
        set: backing::set_data_directory,
    });
    vars::ConfigFileName.install(GucVarAccessors {
        get: backing::ConfigFileName,
        set: backing::set_ConfigFileName,
    });
    vars::HbaFileName.install(GucVarAccessors {
        get: backing::HbaFileName,
        set: backing::set_HbaFileName,
    });
    vars::IdentFileName.install(GucVarAccessors {
        get: backing::IdentFileName,
        set: backing::set_IdentFileName,
    });
    vars::external_pid_file.install(GucVarAccessors {
        get: backing::external_pid_file,
        set: backing::set_external_pid_file,
    });
    vars::application_name.install(GucVarAccessors {
        get: backing::application_name,
        set: backing::set_application_name,
    });
    vars::backtrace_functions.install(GucVarAccessors {
        get: backing::backtrace_functions,
        set: backing::set_backtrace_functions,
    });
    vars::debug_io_direct_string.install(GucVarAccessors {
        get: backing::debug_io_direct_string,
        set: backing::set_debug_io_direct_string,
    });
    vars::recovery_target_timeline_string.install(GucVarAccessors {
        get: backing::recovery_target_timeline_string,
        set: backing::set_recovery_target_timeline_string,
    });
    vars::recovery_target_string.install(GucVarAccessors {
        get: backing::recovery_target_string,
        set: backing::set_recovery_target_string,
    });
    vars::recovery_target_xid_string.install(GucVarAccessors {
        get: backing::recovery_target_xid_string,
        set: backing::set_recovery_target_xid_string,
    });
    vars::recovery_target_name_string.install(GucVarAccessors {
        get: backing::recovery_target_name_string,
        set: backing::set_recovery_target_name_string,
    });
    vars::recovery_target_lsn_string.install(GucVarAccessors {
        get: backing::recovery_target_lsn_string,
        set: backing::set_recovery_target_lsn_string,
    });
    vars::cluster_name.install(GucVarAccessors {
        get: backing::cluster_name,
        set: backing::set_cluster_name,
    });
}

/// `PostPortNumber` (guc_tables.c) — the runtime value of the `port` integer
/// GUC. Falls back to the C `boot_val` (`DEF_PGPORT` == 5432) while the slot is
/// unset (e.g. single-user mode that never configures a listen port).
fn post_port_number_impl() -> i32 {
    if vars::PostPortNumber.installed() {
        vars::PostPortNumber.read()
    } else {
        // boot_val for `port` (guc_tables.c) is DEF_PGPORT (5432).
        5432
    }
}

#[cfg(test)]
mod tests;
