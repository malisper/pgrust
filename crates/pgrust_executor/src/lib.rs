pub mod advisory_locks;
pub mod aggregate;
pub mod async_notify;
pub mod bindings;
pub mod catalog_builtins;
pub mod constraints;
pub mod domain;
pub mod driver;
pub mod exec_tuples;
pub mod fmgr;
pub mod foreign_keys;
pub mod function_guc;
pub mod generate_series;
pub mod hashjoin;
pub mod mergejoin;
pub mod misc_builtins;
pub mod parallel;
pub mod partition;
pub mod permissions;
pub mod predicate;
pub mod services;
pub mod sqlfunc;
pub mod srf;
pub mod startup;
pub mod stats;
pub mod transaction;
pub mod txid;
pub mod window;

pub use advisory_locks::{
    AdvisoryLockCall, AdvisoryLockEvalError, AdvisoryLockOperation, AdvisoryLockRuntime,
    AdvisoryLockScope, advisory_lock_call, execute_advisory_lock_call, is_advisory_builtin,
};
pub use aggregate::{
    AggregateRuntimeSelection, AggregateSupportError, CustomAggregateRuntime, NumericAccum,
    NumericSumAccum, accumulate_sum_value, accumulate_value, aggregate_float_value,
    aggregate_int8_pair, aggregate_numeric_value, aggregate_runtime_selection, clamp_corr,
    clamp_regr_r2, concrete_custom_aggregate_transtype, eval_float8_accum_function,
    eval_float8_combine_function, eval_float8_regr_accum_function,
    eval_float8_regr_combine_function, expect_float8_arg, finalize_array_agg, finalize_regr_stats,
    float8_regr_accum_state, floor_div_i32, format_numeric_result,
    normalize_array_value as normalize_aggregate_array_value, numeric_accum_to_value,
    numeric_div_display_scale, numeric_quotient_decimal_weight, numeric_sqrt,
    numeric_visible_scale, regr_value_or_null, stable_regr_semidefinite_sum,
    string_agg_input_bytes, validate_array_agg_array_input,
};
pub use async_notify::{
    ASYNC_NOTIFY_CHANNEL_MAX_LEN, ASYNC_NOTIFY_PAYLOAD_MAX_LEN, ASYNC_NOTIFY_QUEUE_CAPACITY_BYTES,
    AsyncListenAction, AsyncListenOp, AsyncNotifyArgError, AsyncNotifyArgOrQueueError,
    AsyncNotifyEvalContext, AsyncNotifyQueueError, AsyncNotifyRuntime, DeliveredNotification,
    PendingNotification, eval_pg_notification_queue_usage_function, eval_pg_notify_function,
    merge_pending_notifications, notification_queue_usage_value, pending_notification_bytes,
    pg_notify_args, queue_pending_notification, validate_pending_notification,
};
pub use bindings::{
    ExprEvalBindings, merge_system_bindings, set_inner_expr_bindings, set_outer_expr_bindings,
};
pub use catalog_builtins::{
    CatalogBuiltinError, eval_enum_function as eval_enum_catalog_function,
    eval_pg_collation_is_visible, eval_pg_conversion_is_visible, eval_pg_opclass_is_visible,
    eval_pg_operator_is_visible, eval_pg_opfamily_is_visible, eval_pg_ts_config_is_visible,
    eval_pg_ts_dict_is_visible, eval_pg_ts_parser_is_visible, eval_pg_ts_template_is_visible,
    eval_pg_type_is_visible,
};
pub use constraints::{
    BooleanConstraintResult, CheckConstraintFailure, DeferredConstraintSnapshot,
    DeferredConstraintTracker, DeferredForeignKeyTracker, NotNullConstraintDescriptor,
    NotNullViolation, PendingForeignKeyCheck, PendingParentForeignKeyCheck, PendingUniqueCheck,
    PendingUserConstraintTrigger, RlsDetailSource, RlsWriteCheckFailure, RlsWriteCheckSource,
    check_constraint_failure, find_not_null_violation, rls_write_check_failure,
    row_security_new_row_tid,
};
pub use domain::{
    DomainConstraintError, DomainConstraintLookup, DomainConstraintLookupKind,
    DomainConstraintRuntime, DomainLookup, domain_check_violation_message,
    domain_non_bool_check_detail, domain_not_null_violation_message,
    enforce_domain_constraints_for_value as enforce_domain_constraints_for_value_with_runtime,
    enforce_domain_constraints_for_value_ref as enforce_domain_constraints_for_value_ref_with_runtime,
};
pub use driver::{
    ReadonlyCreateStatisticsError, RestrictedRelationInfo, RestrictedViewCatalog,
    RestrictedViewError, UnsupportedStatementExecError, reject_restricted_views_in_plan,
    reject_restricted_views_in_planned_stmt, reject_restricted_views_in_select,
    restrict_nonsystem_view_enabled, unsupported_statement_error,
    validate_readonly_create_statistics,
};
pub use exec_tuples::{CompiledTupleDecoder, TupleDecodeError};
pub use fmgr::{
    NormalizedVariadicArgs, ScalarFunctionCallInfo, UnsupportedInternalFunctionDetail,
    normalize_variadic_scalar_function_args, scalar_function_call_info_for_proc_row,
    unsupported_internal_function_detail,
};
pub use foreign_keys::{
    ForeignKeyHelperError, ForeignKeyValueRenderContext, ForeignKeyViolationMessage,
    InboundForeignKeyViolationInfo, InsertForeignKeyCheckPhase, build_equality_scan_keys,
    extract_key_values, foreign_key_delete_proc_oid, foreign_key_update_proc_oid,
    inbound_foreign_key_violation_message, inbound_restrict_foreign_key_violation_message,
    key_columns_changed, map_column_indexes_by_name, periods_overlap, render_key_value,
    render_key_values, row_matches_key, temporal_periods_cover, values_match_cross_indexes,
};
pub use function_guc::{
    FunctionGucContext, SavedFunctionIdentity, execute_with_sql_function_context, parsed_proconfig,
    restore_function_gucs, save_function_identity as save_function_identity_state,
};
pub use generate_series::{
    GenerateSeriesError, GenerateSeriesState, MAX_UNBOUNDED_TIMESTAMP_SERIES_ROWS,
    TimestampGenerateSeriesState,
};
pub use hashjoin::{
    HashInstrumentation, HashJoinPhase, HashJoinTable, HashJoinTupleEntry, HashKey,
    HashMemoryConfig, canonical_hash_key_value, hash_batch_count,
    hash_instrumentation_from_row_bytes, hash_value_memory, parse_guc_bool, parse_guc_usize,
    parse_hash_mem_multiplier_millis, parse_memory_kb,
};
pub use mergejoin::{
    MergeJoinBufferedRow, MergeKey, combined_join_values, compare_merge_keys,
    group_end_by_merge_key, null_extended_left_values, null_extended_right_values,
};
pub use misc_builtins::{
    BackendSignalPermission, MiscBuiltinError, backend_signal_op, backend_signal_permission,
    canonicalize_path_text, client_id_arg, configured_current_schema_search_path,
    current_schema_from_search_path, current_schema_is_temp, current_schemas_value, eval_convert,
    eval_get_database_encoding, eval_gist_translate_cmptype_common, eval_greatest, eval_least,
    eval_num_nulls, eval_pg_char_to_encoding, eval_pg_column_toast_chunk_id_values,
    eval_pg_current_logfile, eval_pg_encoding_to_char, eval_pg_log_backend_memory_contexts,
    eval_pg_sleep_function, eval_test_canonicalize_path, eval_uuid_function,
    int4_array_from_client_ids, int32_arg, int64_arg, isolation_session_is_blocked,
    time_precision_overflow_warning, validate_backend_signal_args,
};
pub use parallel::{ParallelRuntime, WorkerTuple};
pub use partition::{
    HashPartitionArgError, PartitionErrorMessage, PartitionTreeViewRow,
    hash_partition_key_count_error, hash_partition_key_type_error,
    hash_partition_relation_open_error, hash_partition_support_proc_return_error,
    int32_arg as hash_partition_int32_arg, not_hash_partitioned_error,
    oid_arg_to_u32 as hash_partition_oid_arg_to_u32, pg_partition_ancestor_rows,
    pg_partition_tree_rows, unsupported_hash_partition_key_error,
    validate_hash_partition_modulus_remainder,
};
pub use permissions::{PermissionCatalog, relation_values_visible_for_error_detail};
pub use predicate::{
    CompiledPredicate as FastCompiledPredicate, PredicateContext, PredicateEvalError,
    PredicateSlot, compile_fast_predicate, compile_fast_predicate_with_decoder,
};
pub use services::{
    ExecutorMutationSink, ExecutorPredicateLockServices, ExecutorRowLockServices,
    ExecutorTransactionServices, LockStatusProvider,
};
pub use sqlfunc::{
    SqlFunctionBodyError, SqlFunctionMetadataError, SqlFunctionRecordFieldTypeMismatch,
    SqlFunctionSubstitutionError, can_coerce_to_compatible_runtime_anchor,
    effective_sql_function_arg_type_oids, merge_polymorphic_runtime_subtype,
    normalize_sql_function_statement_for_execution, normalized_sql_function_body,
    parse_proc_argtype_oids, proc_input_arg_type_oids, quote_sql_identifier, quote_sql_string,
    split_sql_function_body, sql_function_body_is_inline_select_candidate,
    sql_function_is_array_append_transition, sql_function_outputs_single_composite_column,
    sql_function_result_row_for_output, sql_function_return_types_match,
    sql_function_sets_row_security_off, sql_function_single_value_is_whole_result,
    sql_function_statement_needs_database_executor, sql_standard_function_body_inner,
    sql_types_match_for_polymorphic_runtime, starts_with_sql_command, substitute_named_arg,
    substitute_positional_args_with_renderer, substitute_sql_fragment_outside_quotes,
    validate_sql_function_record_field_types,
};
pub use sqlfunc::{
    is_plain_sql_identifier, is_polymorphic_sql_type, is_sql_function_polymorphic_type_oid,
};
pub use sqlfunc::{pack_sql_function_record_row, should_pack_sql_set_returning_record_row};
pub use srf::{
    CursorViewRow, GenerateSubscriptsError, PgOptionsToTableError, PgOptionsToTableRows,
    PreparedStatementViewRow, PreparedXactViewRow, SequenceViewRow, SrfValueError, UnnestError,
    UnnestRows, directory_entry_rows, event_trigger_ddl_command_rows,
    event_trigger_dropped_object_rows, generate_subscripts_values,
    information_schema_sequence_rows, int4_array, local_pg_config_rows, parse_ts_stat_select,
    parse_ts_stat_weights, partition_lookup_oid, pg_backend_memory_context_rows,
    pg_config_fallback_rows, pg_cursor_rows, pg_get_catalog_foreign_key_rows,
    pg_hba_file_rule_rows, pg_ls_dir_rows, pg_ls_named_dir_rows, pg_mcv_list_item_rows,
    pg_prepared_statement_rows, pg_prepared_xact_rows, pg_sequences_rows,
    pg_show_all_settings_rows, pg_tablespace_databases_rows, pg_timezone_abbrev_rows,
    pg_timezone_name_rows, pg_wait_event_rows, publication_names_from_values, regtype_array,
    sequence_type_display, set_returning_call_label, srf_file_timestamp_value,
    srf_io_error_message, text_array, text_search_table_function_for_proc_src, unnest_array_values,
    unnest_expands_single_composite_arg,
};
pub use srf::{
    combine_rows_from_item_values, expr_uses_outer_columns as srf_expr_uses_outer_columns,
    function_output_columns, rows_from_cache_key, rows_from_item_uses_outer_columns,
    single_row_function_scan_values,
};
pub use startup::{
    append_sort_key_qualifier_from_plan, plan_depends_on_worktable,
    plan_needs_network_strict_less_tiebreak, plan_uses_outer_columns, qual_list_is_never_true,
    recursive_union_distinct_hashable, worktable_dependent_cte_ids,
};
pub use stats::NodeExecStats;
pub use stats::{
    FunctionStatsSnapshot, RelationStatsSnapshot, StatsArgError, function_stats_value,
    function_xact_stats_value, pg_stat_get_backend_pid_value, pg_stat_get_backend_wal_value,
    relation_stats_value, relation_xact_stats_value, stats_oid_arg,
};
pub use transaction::{ExecutorTransactionState, SharedExecutorTransactionState};
pub use txid::{
    CurrentTxidSnapshotValue, TxidBuiltinError, TxidRuntime, TxidStatusError,
    current_txid_snapshot_text, eval_txid_builtin_function, txid_status_value,
};
pub use window::{
    WindowFrameError, WindowRangeOrder, WindowRangeRow, first_included_frame_row_index,
    in_range_value, last_included_frame_row_index, nth_included_frame_row_index,
    range_frame_end_from_offset, range_frame_start_from_offset, row_is_included_by_frame_exclusion,
    rows_frame_end, rows_frame_start,
};
