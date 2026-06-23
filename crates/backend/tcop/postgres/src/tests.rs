//! Unit tests for the pure, dependency-free parts of the `tcop/postgres.c`
//! port: the file-local globals, the `getopt` parsing helpers, and the
//! constant-mapping switch helpers.

use crate::globals;
use crate::guc::{get_stats_option_name, restrict_nonsystem_relation_kind};
use ::types_dest::dest::CommandDest;
use ::types_storage::ProcSignalReason;

#[test]
fn where_to_send_output_defaults_to_debug() {
    // `CommandDest whereToSendOutput = DestDebug;` (postgres.c:91).
    assert_eq!(globals::where_to_send_output(), CommandDest::Debug);
    globals::set_where_to_send_output(CommandDest::None);
    assert_eq!(globals::where_to_send_output(), CommandDest::None);
    // restore for any later test sharing the thread
    globals::set_where_to_send_output(CommandDest::Debug);
}

#[test]
fn recovery_conflict_pending_reasons_are_per_reason() {
    assert!(!globals::recovery_conflict_pending());
    globals::set_recovery_conflict_pending_reason(
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOCK,
        true,
    );
    assert!(globals::recovery_conflict_pending_reason(
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOCK
    ));
    assert!(!globals::recovery_conflict_pending_reason(
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_DATABASE
    ));
    globals::set_recovery_conflict_pending_reason(
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOCK,
        false,
    );
    assert!(!globals::recovery_conflict_pending_reason(
        ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_LOCK
    ));
}

#[test]
fn get_stats_option_name_maps_like_c() {
    // case 'p': optarg[1] == 'a' -> parser; 'l' -> planner; 'e' -> executor.
    assert_eq!(get_stats_option_name("parser"), Some("log_parser_stats"));
    assert_eq!(get_stats_option_name("planner"), Some("log_planner_stats"));
    assert_eq!(get_stats_option_name("executor"), Some("log_executor_stats"));
    assert_eq!(get_stats_option_name("xyz"), None);
    assert_eq!(get_stats_option_name(""), None);
}

#[test]
fn restrict_nonsystem_relation_kind_boots_to_zero() {
    // Boot state mirrors `boot_val ""` (empty RESTRICT_RELKIND_* bitmask).
    assert_eq!(restrict_nonsystem_relation_kind(), 0);
}
