#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

// Port of PostgreSQL guc_tables.c static GUC definitions. GUCs compiled out
// of the proven build (LOCK_DEBUG, BTREE_BUILD_STATS, WAL_DEBUG,
// TRACE_SYNCSCAN, DEBUG_NODE_TESTS_ENABLED) are excluded.

pub mod backing;
pub mod consts;
pub mod gather_fair;
pub mod hooks;
pub mod lane_pool;
pub mod parallel_engine;
pub mod runtime_pool;
pub mod option_sets;
pub mod session;
mod slots;
mod tables;
pub mod vars;

pub use slots::*;
pub use tables::*;

pub static GucContext_Names: &[&str] = &[
    "internal",
    "postmaster",
    "sighup",
    "superuser-backend",
    "backend",
    "superuser",
    "user",
];

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

pub static config_type_names: &[&str] = &["bool", "integer", "real", "string", "enum"];

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

pub fn init_seams() {
    install_guc_tables_owned_vars();
    option_sets::wal_level_options.install(WAL_LEVEL_OPTIONS);
    option_sets::recovery_target_action_options.install(RECOVERY_TARGET_ACTION_OPTIONS);
}

// wal_level_options[] (xlogdesc.c); archive/hot_standby are hidden aliases of
// replica.
const WAL_LEVEL_OPTIONS: &[types_guc::config_enum_entry] = &[
    types_guc::config_enum_entry { name: "minimal", val: 0, hidden: false },
    types_guc::config_enum_entry { name: "replica", val: consts::WAL_LEVEL_REPLICA, hidden: false },
    types_guc::config_enum_entry { name: "archive", val: consts::WAL_LEVEL_REPLICA, hidden: true },
    types_guc::config_enum_entry {
        name: "hot_standby",
        val: consts::WAL_LEVEL_REPLICA,
        hidden: true,
    },
    types_guc::config_enum_entry { name: "logical", val: 2, hidden: false },
];

// recovery_target_action_options[] (xlogrecovery.c).
const RECOVERY_TARGET_ACTION_OPTIONS: &[types_guc::config_enum_entry] = &[
    types_guc::config_enum_entry {
        name: "pause",
        val: consts::RECOVERY_TARGET_ACTION_PAUSE,
        hidden: false,
    },
    types_guc::config_enum_entry { name: "promote", val: 1, hidden: false },
    types_guc::config_enum_entry { name: "shutdown", val: 2, hidden: false },
];

// Accessors for GUCs whose C conf->variable is a global defined in
// guc_tables.c itself; vars owned by other subsystems are installed there.
fn install_guc_tables_owned_vars() {
    use crate::GucVarAccessors;

    vars::AllowAlterSystem.install(GucVarAccessors {
        get: backing::AllowAlterSystem,
        set: backing::set_AllowAlterSystem,
    });
    vars::log_duration.install(GucVarAccessors {
        get: backing::log_duration,
        set: backing::set_log_duration,
    });
    vars::Log_disconnections.install(GucVarAccessors {
        get: backing::Log_disconnections,
        set: backing::set_Log_disconnections,
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
    vars::row_security.install(GucVarAccessors {
        get: backing::row_security,
        set: backing::set_row_security,
    });
    vars::pgrust_lane_executor.install(GucVarAccessors {
        get: backing::pgrust_lane_executor,
        set: backing::set_pgrust_lane_executor,
    });
    vars::pgrust_condition_cache.install(GucVarAccessors {
        get: backing::pgrust_condition_cache,
        set: backing::set_pgrust_condition_cache,
    });
    vars::pgrust_condition_cache_size.install(GucVarAccessors {
        get: backing::pgrust_condition_cache_size,
        set: backing::set_pgrust_condition_cache_size,
    });
    vars::pgrust_parallel_engine.install(GucVarAccessors {
        get: backing::pgrust_parallel_engine,
        set: backing::set_pgrust_parallel_engine,
    });
    vars::pgrust_runtime_dop.install(GucVarAccessors {
        get: backing::pgrust_runtime_dop,
        set: backing::set_pgrust_runtime_dop,
    });
    vars::pgrust_runtime.install(GucVarAccessors {
        get: backing::pgrust_runtime,
        set: backing::set_pgrust_runtime,
    });
    vars::pgrust_mem_autotune.install(GucVarAccessors {
        get: backing::pgrust_mem_autotune,
        set: backing::set_pgrust_mem_autotune,
    });
    vars::pgrust_runtime_vacuum_pool.install(GucVarAccessors {
        get: backing::pgrust_runtime_vacuum_pool,
        set: backing::set_pgrust_runtime_vacuum_pool,
    });
    vars::pgrust_memory_watchdog.install(GucVarAccessors {
        get: backing::pgrust_memory_watchdog,
        set: backing::set_pgrust_memory_watchdog,
    });
    vars::pgrust_memory_watchdog_dump.install(GucVarAccessors {
        get: backing::pgrust_memory_watchdog_dump,
        set: backing::set_pgrust_memory_watchdog_dump,
    });
    vars::pgrust_memory_watchdog_interval.install(GucVarAccessors {
        get: backing::pgrust_memory_watchdog_interval,
        set: backing::set_pgrust_memory_watchdog_interval,
    });
    vars::pgrust_memory_watchdog_threshold.install(GucVarAccessors {
        get: backing::pgrust_memory_watchdog_threshold,
        set: backing::set_pgrust_memory_watchdog_threshold,
    });
    vars::pgrust_memory_watchdog_limit.install(GucVarAccessors {
        get: backing::pgrust_memory_watchdog_limit,
        set: backing::set_pgrust_memory_watchdog_limit,
    });
    vars::pgrust_memory_watchdog_test_hog.install(GucVarAccessors {
        get: backing::pgrust_memory_watchdog_test_hog,
        set: backing::set_pgrust_memory_watchdog_test_hog,
    });
    vars::pgrust_runtime_scan_pool.install(GucVarAccessors {
        get: backing::pgrust_runtime_scan_pool,
        set: backing::set_pgrust_runtime_scan_pool,
    });
    vars::pgrust_runtime_agg_pool.install(GucVarAccessors {
        get: backing::pgrust_runtime_agg_pool,
        set: backing::set_pgrust_runtime_agg_pool,
    });
    vars::pgrust_runtime_distinct_pool.install(GucVarAccessors {
        get: backing::pgrust_runtime_distinct_pool,
        set: backing::set_pgrust_runtime_distinct_pool,
    });
    vars::pgrust_runtime_hashjoin_pool.install(GucVarAccessors {
        get: backing::pgrust_runtime_hashjoin_pool,
        set: backing::set_pgrust_runtime_hashjoin_pool,
    });
    vars::pgrust_runtime_sort_pool.install(GucVarAccessors {
        get: backing::pgrust_runtime_sort_pool,
        set: backing::set_pgrust_runtime_sort_pool,
    });
    vars::pgrust_runtime_bitmap_pool.install(GucVarAccessors {
        get: backing::pgrust_runtime_bitmap_pool,
        set: backing::set_pgrust_runtime_bitmap_pool,
    });
    vars::pgrust_lane_parallel_pool.install(GucVarAccessors {
        get: backing::pgrust_lane_parallel_pool,
        set: backing::set_pgrust_lane_parallel_pool,
    });
    vars::pgrust_gather_fair_stride.install(GucVarAccessors {
        get: backing::pgrust_gather_fair_stride,
        set: backing::set_pgrust_gather_fair_stride,
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
    vars::SuperuserReservedConnections.install(GucVarAccessors {
        get: backing::SuperuserReservedConnections,
        set: backing::set_SuperuserReservedConnections,
    });
    vars::ReservedConnections.install(GucVarAccessors {
        get: backing::ReservedConnections,
        set: backing::set_ReservedConnections,
    });
    vars::EnableSSL.install(GucVarAccessors {
        get: backing::EnableSSL,
        set: backing::set_EnableSSL,
    });
    vars::SSLPreferServerCiphers.install(GucVarAccessors {
        get: backing::SSLPreferServerCiphers,
        set: backing::set_SSLPreferServerCiphers,
    });
    vars::ssl_passphrase_command_supports_reload.install(GucVarAccessors {
        get: backing::ssl_passphrase_command_supports_reload,
        set: backing::set_ssl_passphrase_command_supports_reload,
    });
    vars::ssl_min_protocol_version.install(GucVarAccessors {
        get: backing::ssl_min_protocol_version,
        set: backing::set_ssl_min_protocol_version,
    });
    vars::ssl_max_protocol_version.install(GucVarAccessors {
        get: backing::ssl_max_protocol_version,
        set: backing::set_ssl_max_protocol_version,
    });
    vars::ssl_library.install(GucVarAccessors {
        get: backing::ssl_library,
        set: backing::set_ssl_library,
    });
    vars::ssl_cert_file.install(GucVarAccessors {
        get: backing::ssl_cert_file,
        set: backing::set_ssl_cert_file,
    });
    vars::ssl_key_file.install(GucVarAccessors {
        get: backing::ssl_key_file,
        set: backing::set_ssl_key_file,
    });
    vars::ssl_ca_file.install(GucVarAccessors {
        get: backing::ssl_ca_file,
        set: backing::set_ssl_ca_file,
    });
    vars::ssl_crl_file.install(GucVarAccessors {
        get: backing::ssl_crl_file,
        set: backing::set_ssl_crl_file,
    });
    vars::ssl_crl_dir.install(GucVarAccessors {
        get: backing::ssl_crl_dir,
        set: backing::set_ssl_crl_dir,
    });
    vars::ssl_dh_params_file.install(GucVarAccessors {
        get: backing::ssl_dh_params_file,
        set: backing::set_ssl_dh_params_file,
    });
    vars::ssl_passphrase_command.install(GucVarAccessors {
        get: backing::ssl_passphrase_command,
        set: backing::set_ssl_passphrase_command,
    });
    vars::SSLCipherSuites.install(GucVarAccessors {
        get: backing::SSLCipherSuites,
        set: backing::set_SSLCipherSuites,
    });
    vars::SSLCipherList.install(GucVarAccessors {
        get: backing::SSLCipherList,
        set: backing::set_SSLCipherList,
    });
    vars::SSLECDHCurve.install(GucVarAccessors {
        get: backing::SSLECDHCurve,
        set: backing::set_SSLECDHCurve,
    });
    vars::restart_after_crash.install(GucVarAccessors {
        get: backing::restart_after_crash,
        set: backing::set_restart_after_crash,
    });
    vars::remove_temp_files_after_crash.install(GucVarAccessors {
        get: backing::remove_temp_files_after_crash,
        set: backing::set_remove_temp_files_after_crash,
    });
    vars::send_abort_for_crash.install(GucVarAccessors {
        get: backing::send_abort_for_crash,
        set: backing::set_send_abort_for_crash,
    });
    vars::send_abort_for_kill.install(GucVarAccessors {
        get: backing::send_abort_for_kill,
        set: backing::set_send_abort_for_kill,
    });
    vars::log_hostname.install(GucVarAccessors {
        get: backing::log_hostname,
        set: backing::set_log_hostname,
    });
    vars::summarize_wal.install(GucVarAccessors {
        get: backing::summarize_wal,
        set: backing::set_summarize_wal,
    });
    vars::PostPortNumber.install(GucVarAccessors {
        get: backing::PostPortNumber,
        set: backing::set_PostPortNumber,
    });
    vars::AuthenticationTimeout.install(GucVarAccessors {
        get: backing::AuthenticationTimeout,
        set: backing::set_AuthenticationTimeout,
    });
    vars::PreAuthDelay.install(GucVarAccessors {
        get: backing::PreAuthDelay,
        set: backing::set_PreAuthDelay,
    });
    vars::ListenAddresses.install(GucVarAccessors {
        get: backing::ListenAddresses,
        set: backing::set_ListenAddresses,
    });
    vars::Unix_socket_directories.install(GucVarAccessors {
        get: backing::Unix_socket_directories,
        set: backing::set_Unix_socket_directories,
    });
    vars::PostAuthDelay.install(GucVarAccessors {
        get: backing::PostAuthDelay,
        set: backing::set_PostAuthDelay,
    });
    vars::log_min_duration_sample.install(GucVarAccessors {
        get: backing::log_min_duration_sample,
        set: backing::set_log_min_duration_sample,
    });
    vars::log_min_duration_statement.install(GucVarAccessors {
        get: backing::log_min_duration_statement,
        set: backing::set_log_min_duration_statement,
    });

    vars::log_statement.install(GucVarAccessors {
        get: backing::log_statement,
        set: backing::set_log_statement,
    });
    vars::huge_pages.install(GucVarAccessors {
        get: backing::huge_pages,
        set: backing::set_huge_pages,
    });
    vars::compute_query_id.install(GucVarAccessors {
        get: backing::compute_query_id,
        set: backing::set_compute_query_id,
    });
    vars::huge_pages_status.install(GucVarAccessors {
        get: backing::huge_pages_status,
        set: backing::set_huge_pages_status,
    });

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
    vars::recovery_target_time_string.install(GucVarAccessors {
        get: backing::recovery_target_time_string,
        set: backing::set_recovery_target_time_string,
    });
    vars::recoveryRestoreCommand.install(GucVarAccessors {
        get: backing::recoveryRestoreCommand,
        set: backing::set_recoveryRestoreCommand,
    });
    vars::archiveCleanupCommand.install(GucVarAccessors {
        get: backing::archiveCleanupCommand,
        set: backing::set_archiveCleanupCommand,
    });
    vars::recoveryEndCommand.install(GucVarAccessors {
        get: backing::recoveryEndCommand,
        set: backing::set_recoveryEndCommand,
    });
    vars::PrimaryConnInfo.install(GucVarAccessors {
        get: backing::PrimaryConnInfo,
        set: backing::set_PrimaryConnInfo,
    });
    vars::PrimarySlotName.install(GucVarAccessors {
        get: backing::PrimarySlotName,
        set: backing::set_PrimarySlotName,
    });
    vars::recoveryTargetInclusive.install(GucVarAccessors {
        get: backing::recoveryTargetInclusive,
        set: backing::set_recoveryTargetInclusive,
    });
    vars::wal_receiver_create_temp_slot.install(GucVarAccessors {
        get: backing::wal_receiver_create_temp_slot,
        set: backing::set_wal_receiver_create_temp_slot,
    });
    vars::hot_standby_feedback.install(GucVarAccessors {
        get: backing::hot_standby_feedback,
        set: backing::set_hot_standby_feedback,
    });
    vars::wal_receiver_status_interval.install(GucVarAccessors {
        get: backing::wal_receiver_status_interval,
        set: backing::set_wal_receiver_status_interval,
    });
    vars::wal_receiver_timeout.install(GucVarAccessors {
        get: backing::wal_receiver_timeout,
        set: backing::set_wal_receiver_timeout,
    });
    vars::recoveryTargetAction.install(GucVarAccessors {
        get: backing::recoveryTargetAction,
        set: backing::set_recoveryTargetAction,
    });
    vars::recovery_min_apply_delay.install(GucVarAccessors {
        get: backing::recovery_min_apply_delay,
        set: backing::set_recovery_min_apply_delay,
    });
    vars::cluster_name.install(GucVarAccessors {
        get: backing::cluster_name,
        set: backing::set_cluster_name,
    });
}

#[cfg(test)]
mod tests;
