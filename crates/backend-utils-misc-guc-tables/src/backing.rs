//! Process-global backing storage for the GUC variables whose C
//! `conf->variable` (`guc_tables.c`) is a global *defined in guc_tables.c
//! itself* (lines 515-648 of the C file) rather than owned by another
//! subsystem.
//!
//! In C these are plain process-global `bool`/`int`/`double`/`char *`
//! globals seeded with the entry's `boot_val` and thereafter written by the
//! GUC machinery (or, for the "dummy" SHOW variables, kept in sync by the
//! paired `assign_*` hook). This module mirrors that: one process-global
//! cell per C global, seeded with the C `boot_val`, with a free `get`/`set`
//! function pair (the [`GucVarAccessors`](crate::GucVarAccessors) shape).
//! The owning unit ([`crate::init_seams`]) installs them into the matching
//! [`crate::vars`] slot.
//!
//! Scalars use atomics; strings use a `RwLock<Option<String>>` (NULL stays
//! distinguishable from empty, matching C's `char *`).

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::RwLock;

// ---- helpers -------------------------------------------------------------

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

macro_rules! real_var {
    ($cell:ident, $name:ident, $set:ident, $boot:expr) => {
        // f64 stored as its bit pattern in an AtomicU64.
        static $cell: AtomicU64 = AtomicU64::new(($boot as f64).to_bits());
        pub fn $name() -> f64 {
            f64::from_bits($cell.load(Ordering::Relaxed))
        }
        pub fn $set(v: f64) {
            $cell.store(v.to_bits(), Ordering::Relaxed);
        }
    };
}

macro_rules! string_var {
    ($cell:ident, $get:ident, $set:ident, $boot:expr) => {
        static $cell: RwLock<Option<String>> = RwLock::new(None);
        pub fn $get() -> Option<String> {
            // Seed with the C boot_val on first read while still unset.
            // `$boot` is `Option<&'static str>`.
            let guard = $cell.read().unwrap();
            match &*guard {
                Some(s) => Some(s.clone()),
                None => {
                    let boot: Option<&'static str> = $boot;
                    boot.map(|s| s.to_string())
                }
            }
        }
        pub fn $set(v: Option<String>) {
            *$cell.write().unwrap() = v;
        }
    };
}

// ---- bool GUCs (guc_tables.c) -------------------------------------------

// (debug_copy_parse_plan_trees / debug_write_read_parse_plan_trees /
// debug_raw_expression_coverage_test / log_btree_build_stats are compiled out
// of the proven build — DEBUG_NODE_TESTS_ENABLED / BTREE_BUILD_STATS — so they
// have no GUC-table slot and are not installed here, matching `vars`.)
bool_var!(B_AllowAlterSystem, AllowAlterSystem, set_AllowAlterSystem, true);
bool_var!(B_log_duration, log_duration, set_log_duration, false);
bool_var!(B_Debug_print_plan, Debug_print_plan, set_Debug_print_plan, false);
bool_var!(
    B_Debug_print_parse,
    Debug_print_parse,
    set_Debug_print_parse,
    false
);
bool_var!(
    B_Debug_print_rewritten,
    Debug_print_rewritten,
    set_Debug_print_rewritten,
    false
);
bool_var!(
    B_Debug_pretty_print,
    Debug_pretty_print,
    set_Debug_pretty_print,
    true
);
bool_var!(B_log_parser_stats, log_parser_stats, set_log_parser_stats, false);
bool_var!(
    B_log_planner_stats,
    log_planner_stats,
    set_log_planner_stats,
    false
);
bool_var!(
    B_log_executor_stats,
    log_executor_stats,
    set_log_executor_stats,
    false
);
bool_var!(
    B_log_statement_stats,
    log_statement_stats,
    set_log_statement_stats,
    false
);
bool_var!(B_row_security, row_security, set_row_security, true);
bool_var!(
    B_check_function_bodies,
    check_function_bodies,
    set_check_function_bodies,
    true
);
bool_var!(
    B_default_with_oids,
    default_with_oids,
    set_default_with_oids,
    false
);
bool_var!(
    B_current_role_is_superuser,
    current_role_is_superuser,
    set_current_role_is_superuser,
    false
);
// `assert_enabled` boot is `DEFAULT_ASSERT_ENABLED`; this build is
// !USE_ASSERT_CHECKING (matching the c2rust ground-truth build) → false.
bool_var!(B_assert_enabled, assert_enabled, set_assert_enabled, false);
bool_var!(
    B_in_hot_standby_guc,
    in_hot_standby_guc,
    set_in_hot_standby_guc,
    false
);
bool_var!(B_data_checksums, data_checksums, set_data_checksums, false);
bool_var!(
    B_integer_datetimes,
    integer_datetimes,
    set_integer_datetimes,
    true
);

// ---- int GUCs (guc_tables.c) --------------------------------------------

int_var!(
    I_log_parameter_max_length,
    log_parameter_max_length,
    set_log_parameter_max_length,
    -1
);
int_var!(
    I_log_parameter_max_length_on_error,
    log_parameter_max_length_on_error,
    set_log_parameter_max_length_on_error,
    0
);
int_var!(I_log_temp_files, log_temp_files, set_log_temp_files, -1);
int_var!(I_temp_file_limit, temp_file_limit, set_temp_file_limit, -1);
int_var!(I_num_temp_buffers, num_temp_buffers, set_num_temp_buffers, 1024);
// PRESET / runtime-computed preset ints. Compile-time presets are seeded to
// their fixed C boot_val (= min = max); runtime-computed ones start at the C
// boot_val and are overwritten by the computing subsystem.
int_var!(
    I_ssl_renegotiation_limit,
    ssl_renegotiation_limit,
    set_ssl_renegotiation_limit,
    0
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

// ---- int GUCs whose C `conf->variable` lives in postmaster.c -------------
// (`SuperuserReservedConnections`, `ReservedConnections`). The postmaster unit
// is not yet ported as a GUC owner, so the process-global backing lives here
// (the same shape as the guc_tables.c-owned ints above); init-small's globals.c
// reader shims read them through the GUC slot. Boot vals from guc_tables.c.
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
// ---- GUCs whose C `conf->variable` lives in postmaster.c ----------------
// The postmaster's own GUC globals (`bool EnableSSL`, `char *ListenAddresses`,
// `int PostPortNumber`, `int MaxConnections`, `char *Unix_socket_directories`,
// `bool Logging_collector`, plus the crash-restart/abort policy bools) are
// declared in postmaster.c. postmaster.c is not yet a GUC owner, so the
// process-global backing lives here (the same shape as the guc_tables.c-owned
// globals above); the GUC machinery reads/writes through the accessors and the
// postmaster unit reads the live value through the `vars::*` slots. Boot vals
// from guc_tables.c (matching the `boot_val` in `tables.rs`).
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

// `int PostAuthDelay = 0;` — the `post_auth_delay` GUC's C `conf->variable`
// lives in postgres.c (globals.c declares it; postgres.c registers the GUC).
// That unit is not yet a GUC owner, so the process-global backing lives here
// (boot val 0, from guc_tables.c); init-small's globals.c reader shim reads it
// through the GUC slot.
int_var!(I_PostAuthDelay, PostAuthDelay, set_PostAuthDelay, 0);

// `int log_min_duration_sample = -1;` and `int log_min_duration_statement = -1;`
// (postgres.c). Both postgres.c-owned statement-logging GUC globals, backed here
// (boot -1, from guc_tables.c) until that unit lands as a GUC owner. Read by the
// command loop's statement-duration logging path.
int_var!(
    I_log_min_duration_sample,
    log_min_duration_sample,
    set_log_min_duration_sample,
    -1
);
int_var!(
    I_log_min_duration_statement,
    log_min_duration_statement,
    set_log_min_duration_statement,
    -1
);

// ---- enum GUCs (guc_tables.c) — stored as the int the enum maps to -------

// `int log_statement = LOGSTMT_NONE;` (postgres.c) — the `log_statement` GUC's
// backing, centralized here alongside the other postgres.c-owned GUC globals
// (cf. `log_statement_stats`). LOGSTMT_NONE == 0.
int_var!(I_log_statement, log_statement, set_log_statement, 0);

// HUGE_PAGES_OFF=0, ON=1, TRY=2 (boot for huge_pages), UNKNOWN=3 (boot for
// huge_pages_status) — see storage/pg_shmem.h.
int_var!(I_huge_pages, huge_pages, set_huge_pages, 2); // HUGE_PAGES_TRY
int_var!(
    I_huge_pages_status,
    huge_pages_status,
    set_huge_pages_status,
    3
); // HUGE_PAGES_UNKNOWN

// compute_query_id (queryjumble.h ComputeQueryIdType): OFF=0, ON=1, AUTO=2
// (boot), REGRESS=3. The enum GUC's backing `int` — read by IsQueryIdEnabled().
int_var!(
    I_compute_query_id,
    compute_query_id,
    set_compute_query_id,
    2
); // COMPUTE_QUERY_ID_AUTO

// ---- real GUCs (guc_tables.c) -------------------------------------------

real_var!(R_phony_random_seed, phony_random_seed, set_phony_random_seed, 0.0);
real_var!(
    R_log_statement_sample_rate,
    log_statement_sample_rate,
    set_log_statement_sample_rate,
    1.0
);
real_var!(
    R_log_xact_sample_rate,
    log_xact_sample_rate,
    set_log_xact_sample_rate,
    0.0
);

// ---- string GUCs (guc_tables.c) -----------------------------------------
// The "dummy" SHOW strings whose real state lives elsewhere (kept in sync by
// assign hooks) still have their `conf->variable` char* defined in
// guc_tables.c, so this crate owns the backing. Boot values are the C
// `boot_val` (NULL → None).

string_var!(CELL_event_source, event_source, set_event_source, None);
string_var!(
    CELL_client_encoding_string,
    client_encoding_string,
    set_client_encoding_string,
    Some("SQL_ASCII")
);
string_var!(
    CELL_datestyle_string,
    datestyle_string,
    set_datestyle_string,
    Some("ISO, MDY")
);
string_var!(
    CELL_server_encoding_string,
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
string_var!(
    CELL_role_string,
    role_string,
    set_role_string,
    Some("none")
);
string_var!(
    CELL_session_authorization_string,
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
string_var!(
    CELL_timezone_string,
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
string_var!(
    CELL_timezone_abbreviations_string,
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
string_var!(
    CELL_application_name,
    application_name,
    set_application_name,
    Some("")
);
string_var!(
    CELL_backtrace_functions,
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
