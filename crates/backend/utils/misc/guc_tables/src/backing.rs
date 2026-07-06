use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::RwLock;

// Session-settable vars (PGC_USERSET/SUSET/BACKEND + per-session INTERNAL
// like is_superuser) use per-session backings; postmaster/sighup-scope and
// compile-time-constant vars keep plain process-global cells.
#[allow(unused_imports)]
use crate::session_guc_string as session_string_var;

// Session-settable scalars, one cluster: hot per-query readers (tcop debug/
// log flags) share a single TLS base per function.
crate::session_guc_cluster!(BackingSessionGucs, BACKING_SESSION_GUCS:
    (log_duration_cell, bool, log_duration, set_log_duration, false),
    (Debug_print_plan_cell, bool, Debug_print_plan, set_Debug_print_plan, false),
    (Debug_print_parse_cell, bool, Debug_print_parse, set_Debug_print_parse, false),
    (Debug_print_rewritten_cell, bool, Debug_print_rewritten, set_Debug_print_rewritten, false),
    (Debug_pretty_print_cell, bool, Debug_pretty_print, set_Debug_pretty_print, true),
    (log_parser_stats_cell, bool, log_parser_stats, set_log_parser_stats, false),
    (log_planner_stats_cell, bool, log_planner_stats, set_log_planner_stats, false),
    (log_executor_stats_cell, bool, log_executor_stats, set_log_executor_stats, false),
    (log_statement_stats_cell, bool, log_statement_stats, set_log_statement_stats, false),
    (row_security_cell, bool, row_security, set_row_security, true),
    (check_function_bodies_cell, bool, check_function_bodies, set_check_function_bodies, true),
    (default_with_oids_cell, bool, default_with_oids, set_default_with_oids, false),
    (current_role_is_superuser_cell, bool, current_role_is_superuser, set_current_role_is_superuser, false),
    (in_hot_standby_guc_cell, bool, in_hot_standby_guc, set_in_hot_standby_guc, false),
    (log_parameter_max_length_cell, i32, log_parameter_max_length, set_log_parameter_max_length, -1),
    (log_parameter_max_length_on_error_cell, i32, log_parameter_max_length_on_error, set_log_parameter_max_length_on_error, 0),
    (log_temp_files_cell, i32, log_temp_files, set_log_temp_files, -1),
    (temp_file_limit_cell, i32, temp_file_limit, set_temp_file_limit, -1),
    (num_temp_buffers_cell, i32, num_temp_buffers, set_num_temp_buffers, 1024),
    (ssl_renegotiation_limit_cell, i32, ssl_renegotiation_limit, set_ssl_renegotiation_limit, 0),
    (PostAuthDelay_cell, i32, PostAuthDelay, set_PostAuthDelay, 0),
    (log_min_duration_sample_cell, i32, log_min_duration_sample, set_log_min_duration_sample, -1),
    (log_min_duration_statement_cell, i32, log_min_duration_statement, set_log_min_duration_statement, -1),
    (log_statement_cell, i32, log_statement, set_log_statement, 0),
    (compute_query_id_cell, i32, compute_query_id, set_compute_query_id, 2),
    (phony_random_seed_cell, f64, phony_random_seed, set_phony_random_seed, (0.0) as f64),
    (log_statement_sample_rate_cell, f64, log_statement_sample_rate, set_log_statement_sample_rate, (1.0) as f64),
    (log_xact_sample_rate_cell, f64, log_xact_sample_rate, set_log_xact_sample_rate, (0.0) as f64),
);

macro_rules! bool_var {
    ($cell:ident, $name:ident, $set:ident, $boot:expr) => {
        static $cell: AtomicBool = AtomicBool::new($boot);
        pub fn $name() -> bool {
            $cell.load(Ordering::Relaxed)
        }
        pub fn $set(v: bool) {
            $cell.store(v, Ordering::Relaxed);
        }
    };
}

macro_rules! int_var {
    ($cell:ident, $name:ident, $set:ident, $boot:expr) => {
        static $cell: AtomicI32 = AtomicI32::new($boot);
        pub fn $name() -> i32 {
            $cell.load(Ordering::Relaxed)
        }
        pub fn $set(v: i32) {
            $cell.store(v, Ordering::Relaxed);
        }
    };
}

macro_rules! string_var {
    ($cell:ident, $get:ident, $set:ident, $boot:expr) => {
        static $cell: RwLock<Option<String>> = RwLock::new(None);
        pub fn $get() -> Option<String> {
            let guard = $cell.read().unwrap();
            match &*guard {
                Some(s) => Some(s.clone()),
                None => {
                    let boot: Option<&'static str> = $boot;
                    boot.map(str::to_owned)
                }
            }
        }
        pub fn $set(v: Option<String>) {
            *$cell.write().unwrap() = v;
        }
    };
}

bool_var!(B_AllowAlterSystem, AllowAlterSystem, set_AllowAlterSystem, true);

bool_var!(B_assert_enabled, assert_enabled, set_assert_enabled, false);

bool_var!(B_data_checksums, data_checksums, set_data_checksums, false);
bool_var!(
    B_integer_datetimes,
    integer_datetimes,
    set_integer_datetimes,
    true
);

int_var!(I_huge_page_size, huge_page_size, set_huge_page_size, 0);
int_var!(I_max_function_args, max_function_args, set_max_function_args, 100); // FUNC_MAX_ARGS
int_var!(I_max_index_keys, max_index_keys, set_max_index_keys, 32); // INDEX_MAX_KEYS
int_var!(
    I_max_identifier_length,
    max_identifier_length,
    set_max_identifier_length,
    63
); // NAMEDATALEN-1
int_var!(I_block_size, block_size, set_block_size, 8192); // BLCKSZ
int_var!(I_segment_size, segment_size, set_segment_size, 131072); // RELSEG_SIZE
int_var!(I_wal_block_size, wal_block_size, set_wal_block_size, 8192); // XLOG_BLCKSZ
int_var!(
    I_server_version_num,
    server_version_num,
    set_server_version_num,
    180003
); // PG_VERSION_NUM
int_var!(
    I_shared_memory_size_mb,
    shared_memory_size_mb,
    set_shared_memory_size_mb,
    0
);
int_var!(
    I_shared_memory_size_in_huge_pages,
    shared_memory_size_in_huge_pages,
    set_shared_memory_size_in_huge_pages,
    -1
);
int_var!(I_num_os_semaphores, num_os_semaphores, set_num_os_semaphores, 0);

int_var!(
    I_SuperuserReservedConnections,
    SuperuserReservedConnections,
    set_SuperuserReservedConnections,
    3
);
int_var!(
    I_ReservedConnections,
    ReservedConnections,
    set_ReservedConnections,
    0
);
bool_var!(B_EnableSSL, EnableSSL, set_EnableSSL, false);
bool_var!(
    B_restart_after_crash,
    restart_after_crash,
    set_restart_after_crash,
    true
);
bool_var!(
    B_remove_temp_files_after_crash,
    remove_temp_files_after_crash,
    set_remove_temp_files_after_crash,
    true
);
bool_var!(
    B_send_abort_for_crash,
    send_abort_for_crash,
    set_send_abort_for_crash,
    false
);
bool_var!(
    B_send_abort_for_kill,
    send_abort_for_kill,
    set_send_abort_for_kill,
    false
);
bool_var!(B_log_hostname, log_hostname, set_log_hostname, false);
bool_var!(B_summarize_wal, summarize_wal, set_summarize_wal, false);
int_var!(I_PostPortNumber, PostPortNumber, set_PostPortNumber, 5432); // DEF_PGPORT
int_var!(
    I_AuthenticationTimeout,
    AuthenticationTimeout,
    set_AuthenticationTimeout,
    60
);
int_var!(I_PreAuthDelay, PreAuthDelay, set_PreAuthDelay, 0);
string_var!(
    CELL_ListenAddresses,
    ListenAddresses,
    set_ListenAddresses,
    Some("localhost")
);
string_var!(
    CELL_Unix_socket_directories,
    Unix_socket_directories,
    set_Unix_socket_directories,
    Some("/tmp")
);

int_var!(I_huge_pages, huge_pages, set_huge_pages, 2); // HUGE_PAGES_TRY
int_var!(
    I_huge_pages_status,
    huge_pages_status,
    set_huge_pages_status,
    3
); // HUGE_PAGES_UNKNOWN

 // COMPUTE_QUERY_ID_AUTO

string_var!(CELL_event_source, event_source, set_event_source, None);
session_string_var!(CELL_client_encoding_string,
    client_encoding_string,
    set_client_encoding_string,
    Some("SQL_ASCII")
);
session_string_var!(CELL_datestyle_string,
    datestyle_string,
    set_datestyle_string,
    Some("ISO, MDY")
);
session_string_var!(CELL_server_encoding_string,
    server_encoding_string,
    set_server_encoding_string,
    Some("SQL_ASCII")
);
string_var!(
    CELL_server_version_string,
    server_version_string,
    set_server_version_string,
    Some("18.3") // PG_VERSION
);
session_string_var!(CELL_role_string,
    role_string,
    set_role_string,
    Some("none")
);
session_string_var!(CELL_session_authorization_string,
    session_authorization_string,
    set_session_authorization_string,
    None
);
string_var!(
    CELL_syslog_ident_str,
    syslog_ident_str,
    set_syslog_ident_str,
    Some("postgres")
);
session_string_var!(CELL_timezone_string,
    timezone_string,
    set_timezone_string,
    Some("GMT")
);
string_var!(
    CELL_log_timezone_string,
    log_timezone_string,
    set_log_timezone_string,
    Some("GMT")
);
session_string_var!(CELL_timezone_abbreviations_string,
    timezone_abbreviations_string,
    set_timezone_abbreviations_string,
    None
);
string_var!(
    CELL_data_directory,
    data_directory,
    set_data_directory,
    None
);
string_var!(CELL_ConfigFileName, ConfigFileName, set_ConfigFileName, None);
string_var!(CELL_HbaFileName, HbaFileName, set_HbaFileName, None);
string_var!(CELL_IdentFileName, IdentFileName, set_IdentFileName, None);
string_var!(
    CELL_external_pid_file,
    external_pid_file,
    set_external_pid_file,
    None
);
session_string_var!(CELL_application_name,
    application_name,
    set_application_name,
    Some("")
);
session_string_var!(CELL_backtrace_functions,
    backtrace_functions,
    set_backtrace_functions,
    Some("")
);
string_var!(
    CELL_debug_io_direct_string,
    debug_io_direct_string,
    set_debug_io_direct_string,
    Some("")
);
string_var!(
    CELL_recovery_target_timeline_string,
    recovery_target_timeline_string,
    set_recovery_target_timeline_string,
    Some("latest")
);
string_var!(
    CELL_recovery_target_string,
    recovery_target_string,
    set_recovery_target_string,
    Some("")
);
string_var!(
    CELL_recovery_target_xid_string,
    recovery_target_xid_string,
    set_recovery_target_xid_string,
    Some("")
);
string_var!(
    CELL_recovery_target_name_string,
    recovery_target_name_string,
    set_recovery_target_name_string,
    Some("")
);
string_var!(
    CELL_recovery_target_lsn_string,
    recovery_target_lsn_string,
    set_recovery_target_lsn_string,
    Some("")
);
string_var!(CELL_cluster_name, cluster_name, set_cluster_name, Some(""));

// be-secure.c file-scope SSL GUC globals, homed here until be_secure owns
// installable storage (EnableSSL precedent above).
bool_var!(
    B_SSLPreferServerCiphers,
    SSLPreferServerCiphers,
    set_SSLPreferServerCiphers,
    true
);
bool_var!(
    B_ssl_passphrase_command_supports_reload,
    ssl_passphrase_command_supports_reload,
    set_ssl_passphrase_command_supports_reload,
    false
);
int_var!(
    CELL_ssl_min_protocol_version,
    ssl_min_protocol_version,
    set_ssl_min_protocol_version,
    crate::consts::PG_TLS1_2_VERSION
);
int_var!(
    CELL_ssl_max_protocol_version,
    ssl_max_protocol_version,
    set_ssl_max_protocol_version,
    crate::consts::PG_TLS_ANY
);
string_var!(CELL_ssl_library, ssl_library, set_ssl_library, Some("OpenSSL"));
string_var!(
    CELL_ssl_cert_file,
    ssl_cert_file,
    set_ssl_cert_file,
    Some("server.crt")
);
string_var!(
    CELL_ssl_key_file,
    ssl_key_file,
    set_ssl_key_file,
    Some("server.key")
);
string_var!(CELL_ssl_ca_file, ssl_ca_file, set_ssl_ca_file, Some(""));
string_var!(CELL_ssl_crl_file, ssl_crl_file, set_ssl_crl_file, Some(""));
string_var!(CELL_ssl_crl_dir, ssl_crl_dir, set_ssl_crl_dir, Some(""));
string_var!(
    CELL_ssl_dh_params_file,
    ssl_dh_params_file,
    set_ssl_dh_params_file,
    Some("")
);
string_var!(
    CELL_ssl_passphrase_command,
    ssl_passphrase_command,
    set_ssl_passphrase_command,
    Some("")
);
string_var!(
    CELL_SSLCipherSuites,
    SSLCipherSuites,
    set_SSLCipherSuites,
    Some("")
);
string_var!(
    CELL_SSLCipherList,
    SSLCipherList,
    set_SSLCipherList,
    Some("HIGH:MEDIUM:+3DES:!aNULL")
);
string_var!(
    CELL_SSLECDHCurve,
    SSLECDHCurve,
    set_SSLECDHCurve,
    Some("X25519:prime256v1")
);
