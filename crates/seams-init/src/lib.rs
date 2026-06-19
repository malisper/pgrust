//! Startup aggregator: calls every ported crate's `init_seams()`.
//!
//! This crate contains NO logic and NO `set()` calls of its own — one line
//! per ported crate, nothing else. Each crate wires its own seams in its own
//! `init_seams()`; this is just the place that invokes them all.

/// The checked-in inventory of canonical built-ins the runtime registry is
/// currently missing or has registered with diverging metadata. The
/// completeness guard (`builtin_registry_matches_canonical_or_baseline`) holds
/// the live gap to exactly this set.
pub mod builtin_gap_baseline;

pub fn init_all() {
    // One line per ported crate, kept sorted:
    contrib_amcheck_verify_nbtree::init_seams();
    contrib_amcheck_verify_common::init_seams();
    backend_archive_shell_archive::init_seams();
    backend_commands_async::init_seams();
    backend_test_regress::init_seams();
    common_extra_encnames::init_seams();
    backend_access_common_detoast::init_seams();
    backend_access_common_toast_compression::init_seams();
    backend_access_common_heaptuple::init_seams();
    backend_access_common_indextuple::init_seams();
    backend_access_common_next::init_seams();
    backend_access_common_bufmask::init_seams();
    backend_access_common_toast_internals::init_seams();
    backend_access_common_printtup::init_seams();
    backend_access_common_relation::init_seams();
    backend_access_common_reloptions::init_seams();
    backend_access_common_session::init_seams();
    backend_access_common_tidstore::init_seams();
    backend_access_common_tupdesc::init_seams();
    backend_access_gin_core_probe::init_seams();
    backend_access_gin_ginfast::init_seams();
    backend_access_gin_ginget::init_seams();
    backend_access_gin_gininsert::init_seams();
    backend_access_gin_ginscan::init_seams();
    backend_access_gin_ginutil::init_seams();
    backend_access_gin_ginvacuum::init_seams();
    backend_access_gin_ginxlog::init_seams();
    backend_access_hashvalidate::init_seams();
    backend_access_heap_heapam::init_seams();
    backend_access_heap_heapam_handler_core::init_seams();
    backend_access_heap_heapam_handler_dml::init_seams();
    backend_access_heap_heapam_visibility::init_seams();
    backend_access_heap_heapam_xlog::init_seams();
    backend_access_heap_heaptoast::init_seams();
    backend_access_table_toast_helper::init_seams();
    backend_access_heap_hio::init_seams();
    backend_access_heap_pruneheap::init_seams();
    backend_access_heap_rewriteheap::init_seams();
    backend_access_heap_vacuumlazy::init_seams();
    backend_access_heap_visibilitymap::init_seams();
    backend_access_index_amapi::init_seams();
    backend_access_index_amvalidate::init_seams();
    backend_access_index_genam::init_seams();
    backend_access_index_indexam::init_seams();
    backend_access_spg_proc::init_seams();
    backend_access_spg_quadtree::init_seams();
    backend_access_gist_proc::init_seams();
    backend_access_gist_core::init_seams();
    // gist-build installs the `gistbuild`/`gistbuildempty` AM build-dispatch
    // seams that gist-core's `ambuild`/`ambuildempty` adapters call (#341).
    backend_access_gist_build::init_seams();
    backend_access_nbt_dedup::init_seams();
    backend_access_nbt_xlog::init_seams();
    backend_access_nbtree_nbtree::init_seams();
    backend_access_rmgrdesc_replorigindesc::init_seams();
    backend_access_rmgrdesc_small::init_seams();
    backend_access_rmgrdesc_smgrdesc::init_seams();
    backend_access_rmgrdesc_xactdesc::init_seams();
    backend_access_rmgrdesc_xlogdesc::init_seams();
    backend_access_sequence::init_seams();
    backend_access_table_table::init_seams();
    backend_access_table_tableam::init_seams();
    backend_access_brin_xlog::init_seams();
    backend_replication_logical_message::init_seams();
    backend_access_spgist_core::init_seams();
    backend_access_brin_insert_vacuum::init_seams();
    backend_access_brin_minmax::init_seams();
    backend_access_hash_xlog::init_seams();
    backend_access_transam_clog::init_seams();
    backend_access_transam_commit_ts::init_seams();
    backend_access_transam_generic_xlog::init_seams();
    backend_access_transam_multixact::init_seams();
    backend_access_transam_parallel::init_seams();
    backend_access_transam_subtrans::init_seams();
    backend_access_transam_timeline::init_seams();
    backend_access_transam_transam::init_seams();
    backend_access_transam_twophase::init_seams();
    backend_access_transam_varsup::init_seams();
    backend_access_transam_xact::init_seams();
    backend_access_transam_xlog::init_seams();
    backend_access_transam_xlogfuncs::init_seams();
    backend_access_transam_xlogarchive::init_seams();
    backend_access_transam_xloginsert::init_seams();
    backend_access_transam_xlogprefetcher::init_seams();
    backend_access_transam_xlogreader::init_seams();
    backend_access_transam_xlogrecovery::init_seams();
    backend_access_transam_xlogstats::init_seams();
    backend_access_transam_xlogutils::init_seams();
    backend_backup_copy::init_seams();
    backend_backup_gzip::init_seams();
    backend_backup_lz4::init_seams();
    backend_backup_zstd::init_seams();
    backend_backup_incremental::init_seams();
    backend_backup_basebackup::init_seams();
    backend_backup_basebackup_target::init_seams();
    backend_backup_server::init_seams();
    backend_backup_sink::init_seams();
    backend_backup_sink_support::init_seams();
    backend_backup_throttle::init_seams();
    backend_bootstrap_bootstrap::init_seams();
    backend_bootstrap_bootparse::init_seams();
    backend_bootstrap_catalog_data::init_seams();
    backend_catalog_catalog::init_seams();
    backend_catalog_storage::init_seams();
    backend_catalog_namespace::init_seams();
    backend_catalog_objectaccess::init_seams();
    backend_catalog_objectaddress::init_seams();
    backend_catalog_partition::init_seams();
    backend_catalog_indexing::init_seams();
    backend_catalog_heap::init_seams();
    backend_catalog_index::init_seams();
    backend_catalog_pg_cast::init_seams();
    backend_catalog_pg_class::init_seams();
    backend_catalog_pg_conversion::init_seams();
    backend_catalog_pg_collation::init_seams();
    backend_catalog_pg_parameter_acl::init_seams();
    backend_catalog_pg_database::init_seams();
    backend_catalog_pg_db_role_setting::init_seams();
    backend_catalog_pg_constraint::init_seams();
    backend_catalog_pg_attrdef::init_seams();
    backend_catalog_pg_depend::init_seams();
    backend_catalog_dependency::init_seams();
    backend_catalog_pg_enum::init_seams();
    backend_catalog_pg_operator::init_seams();
    backend_catalog_pg_aggregate::init_seams();
    backend_catalog_pg_type::init_seams();
    backend_catalog_pg_inherits::init_seams();
    backend_catalog_pg_range::init_seams();
    backend_catalog_pg_largeobject::init_seams();
    backend_catalog_pg_namespace::init_seams();
    backend_catalog_pg_authid::init_seams();
    backend_catalog_pg_proc::init_seams();
    backend_catalog_pg_publication::init_seams();
    backend_catalog_pg_subscription::init_seams();
    backend_commands_publicationcmds::init_seams();
    backend_commands_subscriptioncmds::init_seams();
    backend_catalog_pg_shdepend::init_seams();
    backend_catalog_toasting::init_seams();
    backend_commands_amcmds::init_seams();
    backend_commands_analyze::init_seams();
    backend_statistics_extended_stats::init_seams();
    backend_commands_cluster::init_seams();
    backend_commands_prepare::init_seams();
    backend_commands_tablecmds::init_seams();
    backend_commands_user::init_seams();
    backend_commands_vacuum::init_seams();
    backend_commands_vacuumparallel::init_seams();
    backend_commands_variable::init_seams();
    backend_commands_comment::init_seams();
    backend_commands_indexcmds::init_seams();
    backend_commands_proclang::init_seams();
    backend_commands_dbcommands::init_seams();
    backend_commands_collationcmds::init_seams();
    backend_commands_conversioncmds::init_seams();
    backend_commands_statscmds::init_seams();
    backend_commands_copyto::init_seams();
    backend_commands_copyfrom::init_seams();
    backend_commands_copy::init_seams();
    backend_commands_createas::init_seams();
    backend_commands_view::init_seams();
    backend_commands_define::init_seams();
    backend_commands_alter::init_seams();
    backend_commands_policy::init_seams();
    backend_commands_dropcmds::init_seams();
    backend_commands_event_trigger::init_seams();
    backend_commands_extension::init_seams();
    backend_commands_explain::init_seams();
    backend_commands_foreigncmds::init_seams();
    backend_commands_lockcmds::init_seams();
    backend_commands_functioncmds::init_seams();
    backend_commands_opclasscmds::init_seams();
    backend_commands_operatorcmds::init_seams();
    backend_commands_matview::init_seams();
    backend_commands_schemacmds::init_seams();
    backend_commands_portalcmds::init_seams();
    backend_commands_seclabel::init_seams();
    backend_commands_sequence::init_seams();
    backend_commands_tablespace::init_seams();
    backend_commands_trigger::init_seams();
    backend_commands_tsearchcmds::init_seams();
    backend_commands_typecmds::init_seams();
    backend_conv_utf8_and_big5::init_seams();
    backend_conv_utf8_and_cyrillic::init_seams();
    backend_conv_utf8_and_euc2004::init_seams();
    backend_conv_utf8_and_euc_cn::init_seams();
    backend_conv_utf8_and_euc_jp::init_seams();
    backend_conv_utf8_and_euc_kr::init_seams();
    backend_conv_utf8_and_euc_tw::init_seams();
    backend_executor_execAmi::init_seams();
    backend_executor_execCurrent::init_seams();
    backend_executor_execExpr::init_seams();
    backend_executor_execExprInterp::init_seams();
    backend_executor_execIndexing::init_seams();
    backend_executor_execJunk::init_seams();
    backend_executor_execMain::init_seams();
    backend_executor_execParallel::init_seams();
    backend_executor_execPartition::init_seams();
    backend_executor_execProcnode::init_seams();
    backend_executor_execReplication::init_seams();
    backend_executor_execScan::init_seams();
    backend_executor_execSRF::init_seams();
    backend_executor_execTuples::init_seams();
    backend_executor_execUtils::init_seams();
    backend_executor_execGrouping::init_seams();
    backend_executor_nodeFunctionscan::init_seams();
    backend_executor_nodeTableFuncscan::init_seams();
    backend_executor_spi::init_seams();
    backend_executor_instrument::init_seams();
    backend_executor_nodeAgg::init_seams();
    backend_executor_nodeAppend::init_seams();
    backend_executor_nodeBitmapAnd::init_seams();
    backend_executor_nodeBitmapHeapscan::init_seams();
    backend_executor_nodeCtescan::init_seams();
    backend_executor_nodeBitmapOr::init_seams();
    backend_executor_nodeCustom::init_seams();
    backend_executor_nodeForeignscan::init_seams();
    backend_foreign_foreign::init_seams();
    backend_jit_jit::init_seams();
    backend_executor_nodeGather::init_seams();
    backend_executor_nodeGatherMerge::init_seams();
    backend_executor_nodeGroup::init_seams();
    backend_executor_nodeHash::init_seams();
    backend_executor_nodeHashjoin::init_seams();
    backend_executor_nodeBitmapIndexscan::init_seams();
    backend_executor_nodeIndexonlyscan::init_seams();
    backend_executor_nodeIndexscan::init_seams();
    backend_executor_nodeLimit::init_seams();
    backend_executor_nodeLockRows::init_seams();
    backend_executor_nodeMaterial::init_seams();
    backend_executor_nodeMemoize::init_seams();
    backend_executor_nodeMergejoin::init_seams();
    backend_executor_nodeModifyTable::init_seams();
    backend_executor_nodeRecursiveunion::init_seams();
    backend_executor_nodeProjectSet::init_seams();
    backend_executor_nodeNamedtuplestorescan::init_seams();
    backend_executor_nodeResult::init_seams();
    backend_executor_nodeSamplescan::init_seams();
    backend_access_tablesample_core::init_seams();
    backend_executor_nodeSeqscan::init_seams();
    backend_executor_nodeSetOp::init_seams();
    backend_executor_nodeSubqueryscan::init_seams();
    backend_executor_nodeSort::init_seams();
    backend_executor_nodeIncrementalSort::init_seams();
    backend_executor_nodeSubplan::init_seams();
    backend_executor_tqueue::init_seams();
    backend_executor_nodeUnique::init_seams();
    backend_executor_nodeValuesscan::init_seams();
    backend_geqo_all::init_seams();
    backend_lib_bloomfilter::init_seams();
    backend_lib_dshash::init_seams();
    backend_lib_radixtree::init_seams();
    backend_main_main::init_seams();
    backend_libpq_auth::init_seams();
    backend_libpq_be_fsstubs::init_seams();
    backend_libpq_hba::init_seams();
    backend_libpq_be_gssapi_common::init_seams();
    backend_libpq_be_secure_common::init_seams();
    backend_libpq_be_secure::init_seams();
    backend_libpq_be_secure_openssl::init_seams();
    // OpenSSL provider (`--with-ssl=openssl`): binds libssl + libcrypto and
    // installs the outward OpenSSL FFI seams. With its `ssl-openssl` feature off
    // this is a no-op and the seams stay loud-panicking (faithful USE_SSL off).
    backend_libpq_be_secure_openssl_ffi::init_seams();
    backend_libpq_auth_scram::init_seams();
    backend_libpq_crypt::init_seams();
    backend_libpq_pqcomm::init_seams();
    backend_libpq_pqmq::init_seams();
    backend_libpq_pqformat::init_seams();
    backend_libpq_pqsignal::init_seams();
    interfaces_libpq_fe::init_seams();
    backend_nodes_copyfuncs::init_seams();
    backend_nodes_core::init_seams();
    backend_nodes_equalfuncs::init_seams();
    backend_nodes_outfuncs::init_seams();
    backend_nodes_readfuncs::init_seams();
    backend_access_hash_core::init_seams();
    backend_access_hashfunc::init_seams();
    backend_access_hash_entry::init_seams();
    backend_nodes_extensible::init_seams();
    backend_optimizer_rte_seams::init_seams();
    backend_optimizer_path_allpaths::init_seams();
    backend_optimizer_path_indxpath::init_seams();
    backend_optimizer_path_joinrels::init_seams();
    backend_optimizer_util_relnode::init_seams();
    backend_optimizer_util_appendinfo::init_seams();
    backend_optimizer_util_inherit::init_seams();
    backend_optimizer_util_plancat::init_seams();
    backend_optimizer_path_pathkeys::init_seams();
    backend_access_nbt_compare::init_seams();
    backend_access_nbt_validate::init_seams();
    backend_access_nbtree_core::init_seams();
    backend_access_nbtree_nbtsort::init_seams();
    backend_common_relpath::init_seams();
    backend_optimizer_path_costsize::init_seams();
    backend_optimizer_path_joinpath::init_seams();
    backend_optimizer_path_equivclass::init_seams();
    backend_optimizer_plan_init_subselect::init_seams();
    backend_optimizer_path_small::init_seams();
    backend_optimizer_util_joininfo::init_seams();
    backend_optimizer_util_paramassign::init_seams();
    backend_optimizer_util_vars::init_seams();
    backend_parser_parse_expr::init_seams();
    backend_parser_agg::init_seams();
    backend_parser_func::init_seams();
    backend_parser_clause::init_seams();
    backend_parser_parse_target::init_seams();
    backend_optimizer_util_clauses::init_seams();
    backend_optimizer_prep_prepqual::init_seams();
    backend_optimizer_prep_prepjointree::init_seams();
    backend_optimizer_prep_preptlist::init_seams();
    backend_optimizer_prep_prepunion::init_seams();
    backend_optimizer_prep_prepagg::init_seams();
    backend_optimizer_plan_subselect_pullup::init_seams();
    backend_optimizer_plan_analyzejoins::init_seams();
    backend_optimizer_plan_createplan::init_seams();
    backend_optimizer_plan_planner::init_seams();
    backend_optimizer_plan_setrefs::init_seams();
    backend_optimizer_util_inherit_predtest::init_seams();
    backend_optimizer_util_pathnode::init_seams();
    backend_parser_coerce::init_seams();
    backend_parser_parse_oper::init_seams();
    backend_parser_parse_type::init_seams();
    backend_parser_relation::init_seams();
    backend_parser_analyze::init_seams();
    backend_parser_parse_utilcmd::init_seams();
    backend_parser_small1::init_seams();
    backend_parser_driver::init_seams();
    backend_parser_gram_core::init_seams();
    backend_port_atomics::init_seams();
    backend_port_sysv_sema::init_seams();
    backend_port_sysv_shmem::init_seams();
    backend_postmaster_autovacuum::init_seams();
    backend_postmaster_bgworker::init_seams();
    backend_postmaster_bgwriter::init_seams();
    backend_postmaster_interrupt::init_seams();
    backend_postmaster_launch_backend::init_seams();
    backend_postmaster_fork_process::init_seams();
    backend_postmaster_auxprocess::init_seams();
    backend_postmaster_pgarch::init_seams();
    backend_postmaster_pmchild::init_seams();
    backend_postmaster_postmaster::init_seams();
    backend_postmaster_checkpointer::init_seams();
    backend_postmaster_startup::init_seams();
    backend_postmaster_syslogger::init_seams();
    backend_backup_walsummary::init_seams();
    backend_postmaster_walsummarizer::init_seams();
    backend_postmaster_walwriter::init_seams();
    backend_regex_core::init_seams();
    backend_replication_libpqwalreceiver::init_seams();
    backend_replication_logical_applyparallelworker::init_seams();
    backend_replication_logical_conflict::init_seams();
    backend_replication_logical_decode::init_seams();
    backend_replication_logical_launcher::init_seams();
    backend_replication_logical_worker::init_seams();
    backend_replication_logical_logical::init_seams();
    backend_replication_logical_origin::init_seams();
    backend_replication_logical_proto::init_seams();
    backend_replication_logical_reorderbuffer::init_seams();
    backend_replication_logical_slotsync::init_seams();
    backend_replication_logical_snapbuild::init_seams();
    backend_replication_syncrep_scanner::init_seams();
    backend_replication_repl_scanner::init_seams();
    backend_replication_syncrep::init_seams();
    backend_replication_slot::init_seams();
    backend_replication_slotfuncs::init_seams();
    backend_replication_walreceiver::init_seams();
    backend_replication_walreceiverfuncs::init_seams();
    backend_replication_walsender::init_seams();
    backend_rmgrdesc_next::init_seams();
    backend_rewrite_core::init_seams();
    backend_rewrite_rewriteRemove::init_seams();
    backend_rewrite_rewritehandler::init_seams();
    backend_storage_file_buffile::init_seams();
    backend_storage_file_fd::init_seams();
    backend_storage_file_fileset::init_seams();
    backend_storage_freespace::init_seams();
    backend_storage_ipc::init_seams();
    backend_storage_ipc_dsm_core::init_seams();
    backend_storage_ipc_dsm_registry::init_seams();
    backend_storage_ipc_latch::init_seams();
    backend_storage_ipc_pmsignal::init_seams();
    backend_storage_ipc_procarray::init_seams();
    backend_storage_buffer_support::init_seams();
    backend_storage_buffer_bufmgr::init_seams();
    backend_storage_aio_read_stream::init_seams();
    backend_storage_aio_methods::init_seams();
    backend_storage_ipc_procsignal::init_seams();
    backend_storage_ipc_shm_mq::init_seams();
    backend_storage_ipc_shm_toc::init_seams();
    backend_storage_ipc_shmem::init_seams();
    backend_storage_ipc_sinval::init_seams();
    backend_storage_ipc_waiteventset::init_seams();
    backend_storage_ipc_standby::init_seams();
    backend_storage_large_object::init_seams();
    backend_storage_lmgr_condition_variable::init_seams();
    backend_storage_lmgr_deadlock::init_seams();
    backend_storage_lmgr_lock::init_seams();
    backend_storage_lmgr_lmgr::init_seams();
    backend_storage_lmgr_predicate::init_seams();
    backend_storage_lmgr_proc::init_seams();
    backend_storage_lmgr_lwlock::init_seams();
    backend_storage_lmgr_s_lock::init_seams();
    backend_storage_page::init_seams();
    backend_storage_page_checksum::init_seams();
    backend_storage_smgr_bulkwrite::init_seams();
    backend_storage_smgr_md::init_seams();
    backend_storage_smgr_smgr::init_seams();
    backend_storage_sync::init_seams();
    backend_tcop_backend_startup::init_seams();
    backend_tcop_dest::init_seams();
    backend_executor_tstorereceiver::init_seams();
    backend_tcop_fastpath::init_seams();
    backend_tcop_postgres::init_seams();
    backend_tcop_pquery::init_seams();
    backend_executor_functions::init_seams();
    backend_tcop_utility::init_seams();
    backend_timezone_localtime::init_seams();
    backend_timezone_pgtz::init_seams();
    backend_snowball_dict_snowball::init_seams();
    backend_timezone_strftime::init_seams();
    backend_tsearch_dict::init_seams();
    backend_tsearch_ispell_regis::init_seams();
    backend_tsearch_parse::init_seams();
    backend_tsearch_spell::init_seams();
    backend_utils_activity_small::init_seams();
    backend_utils_activity_status::init_seams();
    // Per-kind pgstat owner crates must register their builtin kinds BEFORE
    // backend_utils_activity_pgstat::init_seams() seals the kind table.
    backend_utils_activity_pgstat_io::init_seams();
    backend_utils_activity_pgstat_wal::init_seams();
    backend_utils_activity_pgstat_replslot::init_seams();
    backend_utils_activity_pgstat_subscription::init_seams();
    backend_utils_activity_pgstat_slru::init_seams();
    backend_utils_activity_pgstat_backend::init_seams();
    backend_utils_activity_pgstat_database::init_seams();
    backend_utils_activity_pgstat_function::init_seams();
    backend_utils_activity_pgstat_relation::init_seams();
    backend_utils_activity_pgstat::init_seams();
    backend_utils_activity_waitevent::init_seams();
    backend_utils_activity_xact::init_seams();
    backend_utils_adt_misc2::init_seams();
    backend_utils_adt_misc::init_seams();
    backend_catalog_aclchk::init_seams();
    backend_utils_adt_acl::init_seams();
    backend_utils_adt_datetime::init_seams();
    backend_utils_adt_array_selfuncs::init_seams();
    backend_utils_adt_geo_selfuncs::init_seams();
    backend_utils_adt_array_typanalyze::init_seams();
    backend_utils_adt_arrayfuncs::init_seams();
    backend_utils_adt_arrayutils::init_seams();
    backend_utils_adt_dbsize::init_seams();
    backend_utils_adt_char::init_seams();
    backend_utils_adt_oid::init_seams();
    backend_utils_adt_xid::init_seams();
    backend_utils_adt_int::init_seams();
    backend_utils_adt_int8::init_seams();
    backend_utils_adt_ascii::init_seams();
    backend_utils_adt_amutils::init_seams();
    backend_utils_adt_mcxtfuncs::init_seams();
    backend_utils_adt_oracle_compat::init_seams();
    backend_utils_adt_tsginidx::init_seams();
    backend_utils_adt_varbit::init_seams();
    backend_utils_adt_xid8funcs::init_seams();
    backend_utils_adt_name::init_seams();
    backend_utils_adt_float::init_seams();
    // money (cash.c): register the `money` type's fmgr builtins (I/O, arithmetic,
    // comparison, casts) into fmgr-core's by-OID dispatch table.
    backend_utils_adt_cash::init_seams();
    // pg_lsn.c: register the `pg_lsn` type's fmgr builtins (I/O, comparison,
    // hash, larger/smaller, and the numeric-bridging mi/pli/mii arithmetic).
    backend_utils_adt_lsn_trigfuncs::init_seams();
    // libm provider: binds the float8 erf/erfc/tgamma/lgamma seams to the
    // system math library (same `<math.h>` PostgreSQL links).
    backend_utils_adt_float_libm_ffi::init_seams();
    backend_utils_adt_format_type::init_seams();
    backend_utils_adt_ruleutils::init_seams();
    backend_utils_adt_xml::init_seams();
    backend_utils_adt_xml_libxml_ffi::init_seams();
    backend_utils_adt_geo_ops::init_seams();
    backend_utils_adt_formatting::init_seams();
    backend_utils_adt_json::init_seams();
    backend_utils_adt_jsonb::init_seams();
    backend_utils_adt_jsonb_gin::init_seams();
    backend_utils_adt_jsonfuncs::init_seams();
    common_jsonapi::init_seams();
    backend_utils_adt_jsonbsubs::init_seams();
    backend_utils_adt_jsonpath::init_seams();
    backend_utils_adt_jsonpath_gram::init_seams();
    backend_utils_adt_like::init_seams();
    common_md5::init_seams();
    common_cryptohash::init_seams();
    backend_utils_adt_cryptohashfuncs::init_seams();
    backend_utils_adt_encode::init_seams();
    backend_utils_adt_multirangetypes::init_seams();
    backend_utils_adt_network::init_seams();
    backend_utils_adt_network_gist::init_seams();
    backend_utils_adt_network_selfuncs::init_seams();
    backend_utils_adt_mac::init_seams();
    backend_utils_adt_mac8::init_seams();
    backend_utils_adt_numeric::init_seams();
    backend_utils_adt_uuid::init_seams();
    backend_utils_adt_pseudorandomfuncs::init_seams();
    backend_utils_adt_numutils::init_seams();
    backend_utils_adt_pg_locale::init_seams();
    backend_utils_adt_pg_locale_icu::init_seams();
    backend_utils_adt_quote::init_seams();
    backend_utils_adt_range_selfuncs::init_seams();
    backend_utils_adt_selfuncs::init_seams();
    backend_utils_adt_rangetypes::init_seams();
    backend_utils_adt_rangetypes_typanalyze::init_seams();
    backend_utils_adt_regexp::init_seams();
    backend_utils_adt_pseudotypes::init_seams();
    backend_utils_adt_enum::init_seams();
    backend_utils_adt_scalar_datum_core::init_seams();
    backend_utils_adt_skipsupport::init_seams();
    backend_utils_adt_tsvector_core::init_seams();
    backend_utils_adt_tsquery_core::init_seams();
    backend_utils_adt_tsrank::init_seams();
    backend_utils_adt_varchar::init_seams();
    backend_utils_adt_varlena::init_seams();
    backend_utils_adt_version::init_seams();
    backend_utils_adt_ri_triggers::init_seams();
    backend_utils_cache_attoptcache::init_seams();
    backend_utils_cache_catcache::init_seams();
    backend_utils_cache_evtcache::init_seams();
    backend_utils_cache_funccache::init_seams();
    backend_utils_cache_inval::init_seams();
    backend_utils_cache_lsyscache::init_seams();
    backend_utils_cache_partcache::init_seams();
    backend_utils_cache_plancache::init_seams();
    backend_utils_cache_relcache::init_seams();
    backend_utils_cache_relcache_nodexform::init_seams();
    backend_utils_cache_relfilenumbermap::init_seams();
    backend_utils_cache_relmapper::init_seams();
    backend_utils_cache_spccache::init_seams();
    backend_utils_cache_syscache::init_seams();
    backend_utils_cache_ts_cache::init_seams();
    backend_utils_cache_typcache::init_seams();
    backend_utils_error::init_seams();
    backend_utils_error_small::init_seams();
    backend_utils_fmgr_core::init_seams();
    backend_utils_fmgr_dfmgr::init_seams();
    port_dynloader::init_seams();
    backend_utils_fmgr_funcapi::init_seams();
    backend_utils_hash_dynahash::init_seams();
    backend_utils_init_miscinit::init_seams();
    backend_utils_init_postinit::init_seams();
    backend_utils_init_small::init_seams();
    backend_conv_cyrillic_and_mic::init_seams();
    backend_conv_latin_and_mic::init_seams();
    backend_conv_latin2_and_win1250::init_seams();
    backend_conv_euc_cn_and_mic::init_seams();
    backend_conv_euc_jp_and_sjis::init_seams();
    backend_conv_euc_kr_and_mic::init_seams();
    backend_conv_euc_tw_and_big5::init_seams();
    backend_conv_euc2004_sjis2004::init_seams();
    backend_utils_mb_conv_string_helpers::init_seams();
    backend_conv_utf8_and_gb18030::init_seams();
    backend_conv_utf8_and_gbk::init_seams();
    backend_conv_utf8_and_iso8859_1::init_seams();
    backend_conv_utf8_and_iso8859::init_seams();
    backend_conv_utf8_and_johab::init_seams();
    backend_conv_utf8_and_sjis::init_seams();
    backend_conv_utf8_and_sjis2004::init_seams();
    backend_conv_utf8_and_uhc::init_seams();
    backend_conv_utf8_and_win::init_seams();
    backend_utils_mb_mbutils::init_seams();
    backend_utils_mb_wstrcmp::init_seams();
    backend_utils_mb_wstrncmp::init_seams();
    backend_utils_misc_guc::init_seams();
    backend_utils_misc_guc_file::init_seams();
    backend_utils_misc_guc_funcs::init_seams();
    backend_utils_misc_guc_tables::init_seams();
    backend_utils_misc_more::init_seams();
    backend_utils_misc_pg_rusage::init_seams();
    backend_utils_misc_queryenvironment::init_seams();
    pg_prng::init_seams();
    backend_utils_misc_sampling::init_seams();
    backend_utils_misc_stack_depth::init_seams();
    backend_utils_misc_timeout::init_seams();
    backend_utils_mmgr_dsa::init_seams();
    backend_utils_mmgr_freepage::init_seams();
    backend_utils_mmgr_portalmem::init_seams();
    backend_utils_resowner_resowner::init_seams();
    backend_utils_sort_small::init_seams();
    backend_utils_sort_sortsupport::init_seams();
    backend_utils_sort_storage::init_seams();
    backend_utils_sort_tuplesort::init_seams();
    backend_utils_time_combocid::init_seams();
    backend_utils_time_snapmgr::init_seams();
    common_blkreftable::init_seams();
    common_checksum_helper::init_seams();
    common_hashfn::init_seams();
    common_unicode_category::init_seams();
    common_ip::init_seams();
    common_pglz::init_seams();
    common_prng_base64::init_seams();
    common_scram_common::init_seams();
    common_string::init_seams();
    interfaces_libpq_legacy_pqsignal::init_seams();
    port_crc32c::init_seams();
    port_pg_strong_random::init_seams();
    port_pgsleep::init_seams();
    port_noblock::init_seams();
    port_pqsignal::init_seams();
    probe_adt_scalar_bool::init_seams();
    backend_utils_adt_jsonpath_exec::init_seams();
    backend_pl_plpgsql_comp::init_seams();
    backend_pl_plpgsql_exec::init_seams();
    backend_pl_plpgsql_handler::init_seams();
}

#[cfg(test)]
mod recurrence_guard {
    //! Guard against the "merge=union silent-drop" defect: every crate whose
    //! `init_seams()` actually installs a seam (its body contains at least one
    //! `::set(`) MUST be invoked by `init_all()` above. If a future merge adds
    //! a seam-installing crate without wiring it here, this test fails in CI
    //! instead of panicking at runtime on the first cross-cycle call.

    use std::fs;
    use std::path::{Path, PathBuf};

    fn crates_dir() -> PathBuf {
        // CARGO_MANIFEST_DIR = .../crates/seams-init
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crates dir")
            .to_path_buf()
    }

    /// Extract the balanced body of `pub fn init_seams() { ... }` from `src`.
    fn init_seams_body(src: &str) -> Option<String> {
        let marker_pos = {
            let mut found = None;
            let bytes = src.as_bytes();
            let needle = b"pub fn init_seams";
            let mut i = 0;
            while i + needle.len() <= bytes.len() {
                if &bytes[i..i + needle.len()] == needle {
                    found = Some(i);
                    break;
                }
                i += 1;
            }
            found?
        };
        // find first '{' after the marker
        let rest = &src[marker_pos..];
        let brace_rel = rest.find('{')?;
        let start = marker_pos + brace_rel;
        let bytes = src.as_bytes();
        let mut depth = 0i32;
        let mut body_start = start + 1;
        let mut i = start;
        while i < bytes.len() {
            match bytes[i] as char {
                '{' => {
                    if depth == 0 {
                        body_start = i + 1;
                    }
                    depth += 1;
                }
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(src[body_start..i].to_string());
                    }
                }
                _ => {}
            }
            i += 1;
        }
        None
    }

    /// Collect all `.rs` files under a directory.
    fn rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
        if let Ok(entries) = fs::read_dir(dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    rs_files(&p, out);
                } else if p.extension().map(|x| x == "rs").unwrap_or(false) {
                    out.push(p);
                }
            }
        }
    }

    /// True for a file that holds only test code (a `tests.rs` module or a file
    /// under a `tests/` dir). Such files commonly `::set(` seams to stub
    /// dependencies for unit tests — those are NOT real installs and must not be
    /// counted as an owner installing its seam.
    fn is_test_file(p: &Path) -> bool {
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name == "tests.rs" || name.ends_with("_tests.rs") {
            return true;
        }
        p.components().any(|c| c.as_os_str() == "tests")
    }

    /// Map a `*-seams` crate dir name to the dir name of its OWNER crate.
    ///
    /// BLIND-SPOT FIX (infix-tag seam dirs): most seam crates are
    /// `<owner>-seams`, but some carry an infix split-tag before `-seams`
    /// (`<owner>-pc-seams`, `<owner>-pq-seams`, `<owner>-elog-seams`,
    /// `<owner>-pre-seams`) used when a unit's seams were split out in a
    /// post-/pre-/elog/pquery pass. A naive `strip_suffix("-seams")` yields a
    /// nonexistent dir (`<owner>-pc`) so the whole crate was silently skipped.
    /// Returns the first candidate that exists as a real crate dir.
    fn seam_owner_dir(crates: &Path, seam_dir_name: &str) -> Option<String> {
        let base = seam_dir_name.strip_suffix("-seams")?;
        // 1) plain `<owner>-seams`.
        if crates.join(base).join("src").is_dir() {
            return Some(base.to_string());
        }
        // 2) `<owner>-<tag>-seams` for a known infix split-tag.
        for tag in ["-pc", "-pq", "-elog", "-pre", "-post"] {
            if let Some(owner) = base.strip_suffix(tag) {
                if crates.join(owner).join("src").is_dir() {
                    return Some(owner.to_string());
                }
            }
        }
        None
    }

    /// Parse `use <path> as <alias>;` lines and return the alias->final-segment
    /// map, where the final segment is the crate ident (e.g.
    /// `use backend_x_seams as x;` -> `x` => `backend_x_seams`). This lets the
    /// call-site collector resolve aliased seam calls back to the real crate.
    fn alias_map(src: &str) -> std::collections::HashMap<String, String> {
        let mut map = std::collections::HashMap::new();
        for line in src.lines() {
            let t = line.trim();
            let rest = match t.strip_prefix("use ") {
                Some(r) => r,
                None => continue,
            };
            let rest = rest.trim_end_matches(';').trim();
            // split on " as "
            let pos = match rest.find(" as ") {
                Some(p) => p,
                None => continue,
            };
            let path = rest[..pos].trim();
            let alias = rest[pos + 4..].trim();
            if alias.is_empty() || alias.contains(|c: char| !(c.is_alphanumeric() || c == '_')) {
                continue;
            }
            // final path segment is the crate/module ident.
            let last = path.rsplit("::").next().unwrap_or(path).trim();
            if last.is_empty() {
                continue;
            }
            map.insert(alias.to_string(), last.to_string());
        }
        map
    }

    /// The lib crate identifier (`[lib] name`, else package name with `-`->`_`).
    fn lib_name(cargo_toml: &str) -> Option<String> {
        // Look for [lib] section name first.
        if let Some(lib_pos) = cargo_toml.find("[lib]") {
            let after = &cargo_toml[lib_pos..];
            // stop at next section header
            let section_end = after[1..].find("
[").map(|x| x + 1).unwrap_or(after.len());
            let section = &after[..section_end];
            if let Some(n) = extract_name(section) {
                return Some(n);
            }
        }
        // Fall back to [package] name.
        if let Some(pkg_pos) = cargo_toml.find("[package]") {
            let after = &cargo_toml[pkg_pos..];
            let section_end = after[1..].find("
[").map(|x| x + 1).unwrap_or(after.len());
            let section = &after[..section_end];
            if let Some(n) = extract_name(section) {
                return Some(n.replace('-', "_"));
            }
        }
        None
    }

    fn extract_name(section: &str) -> Option<String> {
        for line in section.lines() {
            let t = line.trim();
            if let Some(rest) = t.strip_prefix("name") {
                let rest = rest.trim_start();
                if let Some(rest) = rest.strip_prefix('=') {
                    let v = rest.trim().trim_matches('"');
                    if !v.is_empty() {
                        return Some(v.to_string());
                    }
                }
            }
        }
        None
    }

    #[test]
    fn every_seam_installing_crate_is_wired_into_init_all() {
        let crates = crates_dir();
        let this_lib = fs::read_to_string(crates.join("seams-init/src/lib.rs"))
            .expect("read seams-init lib.rs");

        let mut unwired: Vec<(String, usize)> = Vec::new();

        for entry in fs::read_dir(&crates).expect("read crates dir").flatten() {
            let cpath = entry.path();
            if !cpath.is_dir() {
                continue;
            }
            let src = cpath.join("src");
            if !src.is_dir() {
                continue;
            }
            let mut files = Vec::new();
            rs_files(&src, &mut files);

            // BLIND-SPOT FIX (delegated install): an owner's `init_seams()` may
            // not contain the `::set(` calls directly — it can delegate to a
            // helper (`wire::install_*()`, `inward_seams::install()`,
            // `seam_layer::install()`). Counting only the init_seams body misses
            // those crates. Count `::set(` anywhere in the crate's NON-TEST src,
            // and separately require that an `init_seams()` entry point exists
            // (that is the symbol init_all() calls).
            let mut set_count = 0usize;
            let mut has_init_seams = false;
            for f in &files {
                if is_test_file(f) {
                    continue;
                }
                let txt = strip_cfg_test(&fs::read_to_string(f).unwrap_or_default());
                if init_seams_body(&txt).is_some() {
                    has_init_seams = true;
                }
                set_count += txt.matches("::set(").count() + txt.matches("::set (").count();
            }
            if set_count == 0 || !has_init_seams {
                continue; // installs nothing, or has no init_seams entry point
            }

            let cargo = match fs::read_to_string(cpath.join("Cargo.toml")) {
                Ok(c) => c,
                Err(_) => continue,
            };
            let lib = match lib_name(&cargo) {
                Some(l) => l,
                None => continue,
            };

            let call = format!("{}::init_seams();", lib);
            if !this_lib.contains(&call) {
                unwired.push((lib, set_count));
            }
        }

        if !unwired.is_empty() {
            unwired.sort_by(|a, b| b.1.cmp(&a.1));
            let total: usize = unwired.iter().map(|(_, n)| n).sum();
            let detail: String = unwired
                .iter()
                .map(|(l, n)| format!("\n  {} ({} installs)", l, n))
                .collect();
            panic!(
                "seam-wiring defect: {} crate(s) install {} seam(s) via init_seams() \
                 but are NOT called by init_all() in seams-init/src/lib.rs \
                 (merge=union silent-drop). Add the missing `<lib>::init_seams();` \
                 line(s) and the path dep in Cargo.toml:{}",
                unwired.len(),
                total,
                detail
            );
        }
    }

    /// Extract the names of every seam declared by a `*-seams` crate: each
    /// `seam_core::seam!( ... pub fn NAME ... )` (or bare `seam!( ... )`)
    /// invocation contributes exactly the `NAME` of the `pub fn` inside its
    /// balanced parens. `pub fn`s that are NOT inside a `seam!(...)` (e.g.
    /// inherent methods like `new`/`release` on a guard struct the seam crate
    /// also defines) are deliberately ignored — only the macro-declared seam
    /// surface is the contract the owner must install.
    fn declared_seam_fns(src: &str) -> Vec<String> {
        let bytes = src.as_bytes();
        let mut out = Vec::new();
        let mut i = 0;
        let needle = b"seam!";
        while i + needle.len() <= bytes.len() {
            if &bytes[i..i + needle.len()] != needle {
                i += 1;
                continue;
            }
            // Find the opening '(' after `seam!`.
            let mut j = i + needle.len();
            while j < bytes.len() && (bytes[j] as char).is_whitespace() {
                j += 1;
            }
            if j >= bytes.len() || bytes[j] as char != '(' {
                i += needle.len();
                continue;
            }
            // Find the matching close paren.
            let mut depth = 0i32;
            let start = j;
            let mut end = j;
            while j < bytes.len() {
                match bytes[j] as char {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            end = j;
                            break;
                        }
                    }
                    _ => {}
                }
                j += 1;
            }
            let inner = &src[start + 1..end];
            if let Some(name) = first_pub_fn_name(inner) {
                out.push(name);
            }
            i = end + 1;
        }
        out
    }

    /// Given the body of a `seam!( ... )` invocation, return the identifier
    /// following the first `pub fn`.
    fn first_pub_fn_name(inner: &str) -> Option<String> {
        let marker = "pub fn";
        let pos = inner.find(marker)?;
        let rest = &inner[pos + marker.len()..];
        let rest = rest.trim_start();
        // Identifier runs until '(' or '<' (generics) or whitespace.
        let name: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if name.is_empty() {
            None
        } else {
            Some(name)
        }
    }

    /// KNOWN contract-divergence / mis-homed seams on COMPLETE owners whose
    /// reconcile is queued (`DESIGN_DEBT.md` → "seam contract reconcile
    /// pending"). These are NOT pure-wiring misses: each is a seam whose
    /// owner unit is `merged`/`audited` and which IS `::call`ed in non-test
    /// code, yet cannot be installed by a bare `::set()` of an existing body
    /// because the owner's signature diverges from the seam decl (extra `Mcx`,
    /// `Result`-wrapper, out-param reshaping, baked-in constants) OR the real
    /// body is mis-homed (lives in a different / not-yet-ported crate, or the
    /// seam's nominal owner is a split sibling that installs it elsewhere).
    ///
    /// Force-wiring any of these would mean altering a ported, audited body —
    /// forbidden. They are tracked, named debt, not a blanket skip: each entry
    /// has a matching DESIGN_DEBT line. Pay one down by reconciling the
    /// contract, installing it, and DELETING its entry here — the guard then
    /// re-asserts the seam stays installed.
    ///
    /// Entry = (owner-crate-lib-name, seam-fn). Keep sorted.
    const CONTRACT_RECONCILE_PENDING: &[(&str, &str)] = &[
        // DESIGN_DEBT (TD-FDWROUTINE-UPDATABLE): rewriteHandler.c's
        // `relation_is_updatable` foreign-table leg reads the FDW routine's modify
        // callbacks (`IsForeignRelUpdatable`/`ExecForeignInsert`/`...Update`/
        // `...Delete`) to compute the supported-events mask. The repo's
        // `types_nodes::FdwRoutine` carrier is trimmed to the scan/parallel/async
        // callback-presence flags and does NOT model the modify callbacks, so the
        // computation is homed behind `foreign_rel_updatable_events` on the foreign
        // owner (`backend-foreign-foreign`, which holds `GetFdwRoutineForRelation`).
        // The owner is COMPLETE but cannot install this until the FdwRoutine carrier
        // grows the modify-callback fields. Loud-panics (mirror-PG) until then;
        // DELETE this entry when the FdwRoutine modify-callback carrier lands.
        ("backend_foreign_foreign", "foreign_rel_updatable_events"),
        // (TD-DEPENDENCY-REMOVEFUNC RETIRED: `remove_function_tuple`
        // (functioncmds.c:1311 `RemoveFunctionById`) is now INSTALLED cross-crate
        // from backend-catalog-indexing's family2 — it opens pg_proc, reads the
        // row's `prokind`, `CatalogTupleDelete`s it, `pgstat_drop_function`s, and
        // for `PROKIND_AGGREGATE` also deletes the `pg_aggregate` row keyed on
        // `aggfnoid`. The catalog-delete leg lives where `CatalogTupleDelete` +
        // the heap-scan substrate do, not in functioncmds.c — so the entry was
        // DELETED.)
        // NOTE (TD-SYSCACHE-DYNAMIC-TID): dependency.c's generic `DropObjectById`
        // calls `backend_utils_cache_syscache::search_syscache1_tid` — a new
        // generic `SearchSysCache1(cacheId, ...)` primitive for a DYNAMIC cacheId,
        // returning the matched tuple's t_self for CatalogTupleDelete. Its owner
        // dir `backend-utils-cache-syscache` is NOT a complete CATALOG unit
        // (syscache.c is owned by backend-utils-cache-small), so the
        // `every_declared_seam_is_installed_by_its_owner` guard already exempts it
        // (the owner isn't `complete`) — no allowlist entry is needed here (adding
        // one is flagged stale). The debt is recorded in DESIGN_DEBT.md; install
        // when the dynamic-cacheId SearchSysCache1 primitive lands.
        // NOTE: the `inline_set_returning_function` GATE LADDER (clauses.c:5067)
        // is now ported and INSTALLED by `backend-optimizer-util-clauses`; it
        // declines (`Ok(None)`) every non-inlinable SRF, including every
        // C-language SRF (e.g. generate_series, which fails the LANGUAGE-SQL
        // gate). The remaining unported leg — the SQL body parse/rewrite/
        // single-SELECT validation that returns the inlined `Query` — rides the
        // `inline_set_returning_function_sql_body` OUTWARD seam, which clauses
        // calls itself; the guard's outward-seam exclusion covers it, so no
        // allowlist entry is needed (its real owner is the unported SQL-function
        // parse/rewrite path). DESIGN_DEBT TD-SRF-INLINE-QUERY.
        // DESIGN_DEBT (TD-INDEX-OPCLASS-OPTIONS): `index_build_local_reloptions`
        // is the `local_relopts` tail of indexam.c's `index_opclass_options`:
        // `init_local_reloptions(&relopts, 0)` +
        // `FunctionCall1(procinfo, PointerGetDatum(&relopts))` +
        // `build_local_reloptions(&relopts, attoptions, validate)`. The reloptions
        // owner HAS the two helper bodies (`init_local_reloptions`,
        // `build_local_reloptions`), but the middle leg invokes the opclass's
        // options-parsing SUPPORT PROCEDURE through fmgr (`FunctionCall1`), passing
        // a *pointer* to the stack `LocalRelOpts` as a Datum that the proc mutates
        // in place. The reloptions crate has no fmgr dependency, and the bare-word
        // `Datum(usize)` model has no pointer lane to carry `&mut LocalRelOpts`
        // through the call — the same fmgr-Datum-pointer bridge that is unported
        // workspace-wide. Installing would just relocate the runtime panic into the
        // fmgr leg. Pay down once the pointer-Datum/fmgr dispatch bridge lands.
        ("backend_access_common_reloptions", "index_build_local_reloptions"),
        // DESIGN_DEBT (TD-GIN-OPCLASS-DISPATCH): the GIN opclass support-proc
        // dispatch family — `gin_extract_query` (extractQueryFn,
        // `FunctionCall7Coll`, ginscan.c `ginNewScanKey`), `gin_compare_partial`
        // (comparePartialFn, `FunctionCall4Coll`, ginget.c `collectMatchBitmap` /
        // `matchPartialInPendingList`), plus the sibling `gin_extract_value`
        // (extractValueFn), `gin_compare_entries` (compareFn), and
        // `gin_consistent_call_{bool,tri}` (consistent/triConsistent) seams.
        //
        // CORRECTED MODEL (verified C5/#161): the faithful re-model is NOT a
        // "generic fmgr GIN-call dispatcher" — it is TYPED per-opclass dispatch
        // keyed on the support-proc OID (`flinfo.fn_oid`, the resolved
        // `index_getprocinfo` row), exactly the proven BRIN
        // (`backend-access-brin-minmax::dispatch_*` matching `F_BRIN_MINMAX_*`)
        // and SP-GiST opclass-dispatch idiom. The natural single owner is
        // `backend-access-gin-core-probe` (it already holds the array_ops bodies
        // and an `init_seams()`), depending on `tsginidx` for the tsvector_ops
        // bodies and installing all six ginutil-seams keyed on fn_oid:
        //   array_ops    extractValue=ginarrayextract(2743/3076),
        //                extractQuery=ginqueryarrayextract(2774),
        //                consistent=ginarrayconsistent, tri=ginarraytriconsistent
        //                (no comparePartial); bodies PORTED in
        //                `backend-access-gin-core-probe::ginarrayproc`.
        //   tsvector_ops extractValue=gin_extract_tsvector(3656/3077),
        //                extractQuery=gin_extract_tsquery(3657/3087/3791),
        //                comparePartial=gin_cmp_prefix(2700),
        //                compare=gin_cmp_tslexeme(3724); bodies PORTED in
        //                `backend-utils-adt-tsginidx`.
        //   jsonb_ops / jsonb_path_ops: opclass bodies UNPORTED (no jsonb_gin
        //                crate) — those OIDs loud-panic (`unported_opclass`),
        //                the genuine residual GIN opclass port.
        //
        // REAL BLOCKERS (why the family is still uninstalled, not generic-fmgr):
        //   (a) TWO-DATUM-MODEL SPLIT for array_ops extract: `ginarrayextract` /
        //       `ginqueryarrayextract` are on the bare-word `types_datum::Datum`
        //       (`struct Datum(usize)`) lane — they call `deconstruct_array`,
        //       which returns `(types_datum::Datum, bool)`. The GIN scan/index
        //       seams carry the canonical `types_tuple` `Datum` enum
        //       (`ByVal(usize)|ByRef(PgVec<u8>)`). For by-VALUE element types the
        //       bare word maps to `ByVal`, but for by-REFERENCE element keys
        //       (`text[]`, etc.) the bare word is a raw pointer into array
        //       storage with no recoverable bytes — there is NO canonical-`Datum`
        //       `deconstruct_array` variant, so array extract cannot faithfully
        //       produce `ByRef` keys. Keystone: a canonical-`Datum`-returning
        //       `deconstruct_array` (part of the Datum-unification plan). The
        //       tsvector_ops extract path is NOT so blocked — `tsginidx` works in
        //       `PgVec<u8>` varlena bytes that map cleanly to `ByRef`.
        //   (b) NO SINGLE FAMILY OWNER YET: all six seams are uninstalled
        //       together (0 non-test `::set`); installing only the tsvector half
        //       while array extract loud-panics on a complete in-tree opclass —
        //       AND while `gin_consistent_call_{bool,tri}` (the consistent
        //       dispatch a real scan needs) remain uninstalled — would be a
        //       misleading "installed" shell, which the discipline rejects.
        // Pay this down by landing the canonical-`Datum` `deconstruct_array`
        // keystone, then the gin-core-probe opclass-dispatch owner installing all
        // six seams keyed on fn_oid (array+tsvector arms, jsonb loud-panic).
        // DELETE each entry as its arm lands.
        ("backend_access_gin_ginutil", "gin_extract_query"),
        ("backend_access_gin_ginutil", "gin_compare_partial"),
        // DESIGN_DEBT (TD-GIN-RELOPTIONS-KEYSTONE): `gin_get_use_fast_update`
        // (GinGetUseFastUpdate, gin_private.h:34) and `gin_get_pending_list_cleanup_size`
        // (GinGetPendingListCleanupSize, gin_private.h:38) read the index's
        // GIN-specific `GinOptions` bytea out of `rd_options`, which the trimmed
        // relcache does not yet carry. The ginutil owner (audited, the GinOptions
        // owner) therefore has no body to install on these ginutil-seams decls —
        // they panic loudly until the relcache GinOptions keystone lands. (ginfast
        // installs a separate gininsert-seams copy of `gin_get_use_fast_update`
        // over an Oid lookup, a duplicate decl; these ginutil-seams copies remain
        // genuinely uninstalled for the relcache-keystone reason.) DELETE when the
        // relcache GinOptions keystone lands.
        ("backend_access_gin_ginutil", "gin_get_pending_list_cleanup_size"),
        ("backend_access_gin_ginutil", "gin_get_use_fast_update"),
        // (TD-HEAPAM-UNPORTED-DRIVERS RETIRED: the four heapam-seams driver
        //  bodies are now ported + installed by the heap owner
        //  (backend-access-heap-heapam, catalog_drivers.rs):
        //    * insert_one_tuple — bootstrap.c `InsertOneTuple`: CreateTupleDesc
        //      + heap_form_tuple + simple_heap_insert (the seam was re-signed to
        //      carry the full open `Relation`, which simple_heap_insert needs).
        //    * read_pg_type — bootstrap.c `populate_typ_list`: table_open(NoLock)
        //      + table_beginscan_catalog + heap_getnext loop + GETSTRUCT deform.
        //    * scan_indisclustered — cluster.c `get_tables_to_cluster`: pg_index
        //      indisclustered table_beginscan_catalog scan; the per-row aclcheck
        //      stays in the cluster.c caller.
        //    * index_compute_xid_horizon_for_tuples — genam.c's AM-generic
        //      table_index_delete_tuples() shim over the installed
        //      heap_index_delete_tuples.
        //  heap_multi_insert was already ported+installed; its entry was removed
        //  earlier.)
        // (TD-DEST-COMMAND-LIFECYCLE RETIRED: printtup.c's
        // `SetRemoteDestReceiverParams` + the `DestRemote`/`DestRemoteExecute`/
        // `DestDebug` receiver routing now land — `backend-access-common-printtup`
        // installs `set_remote_dest_receiver_params` and registers `printtup`'s
        // real vtable into the tcop-dest router via the `printtup_create_dr` seam,
        // so a `DestRemote` SELECT emits RowDescription + DataRow over the wire.)
        // (backend_status.c's pgstat_report_activity_running / _query_id /
        // _plan_id are also `::call`ed by the F1 pipeline and uninstalled, but
        // their owner `backend-utils-activity-status` is not a complete crate —
        // the guard does not flag those, so no allowlist entry is needed; the
        // debt is the unported backend_status.c owner.)
        // DESIGN_DEBT (TD-PATHNODE-CAN-CREATE-UNIQUE): pathnode.c's
        // `can_create_unique_path` is the `create_unique_path(...) != NULL` test.
        // Its body (`create_unique_path`, pathnode.c:1730) is itself genuinely
        // uninstalled in the otherwise-complete `backend-optimizer-util-pathnode`
        // crate — it crosses lsyscache (`get_ordering_op_for_equality_op` /
        // `get_equality_op_for_ordering_op`), plancat
        // (`relation_has_unique_index_for`), analyzejoins
        // (`query_is_distinct_for`), and pathkeys.c
        // (`make_pathkeys_for_sortclauses`), all unported, so `create_unique_path`
        // delegates to a `unique_seam` that nobody installs (loud panic).
        // Installing `can_create_unique_path` would just relocate that same panic.
        // DELETE this entry once `create_unique_path`'s cross-subsystem owners
        // land. (`install_dummy_append_path` — the pathnode-side of joinrels.c's
        // `mark_dummy_rel` — is now INSTALLED by the pathnode owner: its body only
        // needs `create_append_path`/`add_path`/`set_cheapest`, all ported
        // in-owner.)
        ("backend_optimizer_util_pathnode", "can_create_unique_path"),
        // (#159 STEP C plancache de-handle RETIRED: the handle-based tupdesc pc-seam
        // `free_tuple_desc` is no longer called — plancache now owns TupleDescData
        // values in a private MemoryContext (clone_in via the value `create_tuple_desc_copy`
        // seam), freed by dropping the context rather than a handle free seam.)
        // RETIRED (task #161): `heap_tuple_header_get_datum`
        // (HeapTupleHeaderGetDatum) is now installed by heaptoast's init_seams().
        // The composite/record-Datum carrier bridge landed.
        // DESIGN_DEBT (TOWER-B): the index-AM owner (backend-access-index-amapi,
        // amapi.c) installs the 11 GetIndexAmRoutine-derived seams. It now also
        // installs `am_reloptions` (reloptions.c's `index_reloptions` ->
        // `amoptions(reloptions, validate)` dispatch): although `amoptions` is a
        // by-name AM callback the unified `IndexAmRoutine` vtable DROPPED in
        // TOWER-A, amapi sits above all the AM *core* crates in the dep graph and
        // reaches their `*options` bodies directly (`nbtree-core::utils::btoptions`,
        // `hash-core::hashutil::hashoptions`, `gist-core::gistutil::gistoptions`,
        // `gin-ginutil::ginoptions`, `spgist-core::spgoptions`), dispatched on the
        // index's `rd_amhandler` exactly like `GetIndexAmRoutine` (#341). BRIN's
        // `brinoptions` is itself unported, so a BRIN index with reloptions
        // seam-and-panics (mirror PG and panic), same as the dynamic-AM leg.
        // `am_adjust_members` is NOT amapi.c logic and remains uninstalled:
        // it is opclasscmds.c's `amroutine->amadjustmembers(...)` dispatch, and
        // additionally needs a conversion between the seam's
        // `types_opclass::OpFamilyMember` and the trimmed per-AM `OpFamilyMember`
        // the bt/hash adjustmembers callbacks mutate (a by-amoid AM-callback
        // dispatch table + carrier reconcile in opclasscmds). (am_adjust_members
        // is consumed only via a brace-grouped `use ...::{...}` import that the
        // recurrence guard's call-site scanner does not resolve to its seam crate,
        // so it is not seen as "called" and needs no allowlist entry; it remains
        // uninstalled for the same contract reason.)
        // RESOLVED (TOWER-C): the 16 index_* scan-lifecycle / retrieval seams
        // (index_beginscan{,_bitmap,_parallel}, index_rescan{,_is,_bis},
        // index_endscan, index_markpos, index_restrpos, index_getnext_tid,
        // index_fetch_heap, index_getnext_slot, index_getbitmap,
        // index_parallelscan_estimate, index_parallelscan_initialize,
        // index_parallelrescan) are now INSTALLED by the indexam owner. After
        // TOWER-A unified the scan descriptor (types_nodes::nodeindexonlyscan
        // re-exports the canonical types_tableam::relscan types), the owner's
        // init_seams() installs thin `seam_*` wrappers that adapt the
        // node-/SlotId-shaped seam decls to the C-faithful `index_*` bodies:
        // snapshot Option<Rc<SnapshotData>> -> SnapshotData unwrap; instrument ->
        // Option; node scan-key arrays -> &[ScanKeyData] (split-borrow); SlotId ->
        // estate.slot_mut(); the payload-erased TIDBitmap round-trip; and the
        // parallel-descriptor PgBox<->value bridge.
        //
        // `index_scan_resolve_shared_info` is now INSTALLED by the indexam owner:
        // the owned `ParallelIndexScanDescData` carries the
        // `SharedIndexScanInstrumentation` region (the C `ps_offset_ins` blob) as
        // the value field `shared_instrument`, so the worker-side resolution is a
        // clone of that owned region rather than DSM `OffsetToPointer` arithmetic.
        //
        // STILL PENDING (parallel-scan DSM infrastructure unported):
        // `bt_resolve_parallel_scan` resolves a `*mut BTParallelScanDescData` into
        // the DSM-resident parallel scan blob at `ps_offset_am`, which the nbtree
        // state machine dereferences under the descriptor's embedded `btps_lock`
        // LWLock. The owned model stores the AM-specific region as a serialized
        // `Vec<u8>` (`am_specific`), with no live shared `BTParallelScanDescData`
        // value to hand a `*mut` to across leader/worker. Wiring it faithfully
        // needs the cross-process DSM shared-memory substrate (a real
        // `BTParallelScanDescData` in DSM with a working `btps_lock`) — a
        // tree-wide parallel-scan-infrastructure campaign, not index-AM wiring. A
        // serial scan never reaches it; it seam-and-panics (mirror-pg-and-panic).
        // See DESIGN_DEBT.md.
        ("backend_access_index_indexam", "bt_resolve_parallel_scan"),
        // (get_table_am_routine / table_relation_toast_am /
        // table_relation_needs_toast_table / table_parallelscan_reinitialize
        // retired: heapam_handler.c (core stage) + tableamapi.c::GetTableAmRoutine
        // are ported in backend-access-heap-heapam-handler-core, which installs all
        // four provider-facing seams from its init_seams().)
        // (Both serial index-build heap-scan seams are now INSTALLED by
        // backend-access-heap-heapam-handler-core (build_scan) on the canonical,
        // fully-typed contract (mcx + execnodes::IndexInfo<'mcx> + canonical
        // Datum): `table_index_build_range_scan` -> heapam_index_build_range_scan
        // (brinsummarize reaches it via build_index_info), and the whole-relation
        // `table_index_build_scan` -> the same provider over the entire relation.
        // The IndexInfo-through-ambuild keystone re-signed all the AM serial
        // build drivers to carry the real `execnodes::IndexInfo` + mcx, so the
        // `table_index_build_scan` allowlist entry is retired. The brin consumer
        // imports the range-scan seam fn directly so the recurrence guard's
        // call-site scanner never attributed that call to the tableam-seams
        // crate; it was never a tuple entry and needs none.)
        // (#159 STEP C plancache de-handle RETIRED: the handle/CtxId search-path
        // matcher seams — copy_search_path_matcher / get_search_path_matcher /
        // search_path_matches_current_environment — are no longer called. plancache
        // now stores an owned SearchPathMatcher<'static> value and calls the
        // value-shaped namespace seams get_search_path_matcher_value /
        // search_path_matches_current_environment_value instead.)
        // (restrict_search_path retired: RestrictSearchPath is guc.c's function
        // (guc.c:2246), now ported + installed by the merged guc owner
        // (backend-utils-misc-guc) and its seam re-homed to
        // backend-utils-misc-guc-seams. Consumers (matview, cluster) call it
        // there.)
        // xlog reconciled out: CATALOG status corrected merged->needs-decomp
        // (chore/xlog-catalog-honest, task #111). An incomplete owner legitimately
        // seam-and-panics its unported surface (mirror-pg-and-panic), so the guard
        // no longer flags it (condition (b) false) — these entries went stale.
        // (extract_set_variable_args + GUCArrayAdd/Delete/Reset retired: all four
        // guc.c/guc_funcs.c functions were re-homed off
        // backend-commands-functioncmds-seams onto their real owners' -seams
        // crates (backend-utils-misc-guc-funcs-seams /
        // backend-utils-misc-guc-seams) and are installed by those merged owners'
        // init_seams(). GUCArrayAdd/Delete/Reset are now genuinely ported in
        // backend-utils-misc-guc's guc_array.rs over the Vec<String> value model.)
        // RESOLVED (aclchk F1): `aclcheck_error_type` (aclchk.c) was re-homed off
        // backend-commands-functioncmds-seams onto its real owner's seam crate
        // (backend-catalog-aclchk-seams) and is now installed by the ported
        // `backend-catalog-aclchk` owner (its body calls `get_element_type` +
        // `format_type_be` then the generic `aclcheck_error(.., OBJECT_TYPE, ..)`).
        // The two consumers (functioncmds, objectaddress) call the re-homed seam.
        // Allowlist entry removed.
        // (get_language_oid is now installed by its real owner,
        // backend-commands-proclang::init_seams; the proclang port wraps the
        // syscache LANGNAME OID lookup with the missing-language error.)
        // NOTE: the PARAM_EXEC `execPlan`-link seams formerly listed here under
        // backend_executor_execProcnode were RELOCATED to execMain-seams (their
        // real owner: they operate on the executor-owned `es_param_exec_vals` /
        // `es_subplanstates`, not on any execProcnode.c function). The
        // `ParamExecData.execPlan` field is now modeled (an `ExecPlanLink`
        // identity into `es_subplanstates`), so the three field-level ops
        // (mark/clear/pending-test) are INSTALLED by
        // backend-executor-execMain::init_seams. The two still genuinely blocked
        // (`exec_set_param_plan_for_pending` = the `ExecSetParamPlan` re-entry,
        // `link_subplan_planstate` = `sstate->planstate = list_nth(...)`) need
        // nodeSubplan's SubPlanState-reachability wiring (the InitPlan
        // `SubPlanState`s are owned by the parent plan-state's `initPlan` list, not
        // addressable from the param array yet) — but execMain is CATALOG
        // `needs-decomp`, so the seam-install guard already exempts its unfinished
        // surface; no allowlist entry is required.
        // (`cur_tuple_getattr` + `replace_cur_tuple_from_slot` RESOLVED: the
        // nodeSubplan `SubPlanState.curTuple` carrier was widened from the
        // data-less `HeapTuple` (`HeapTupleData`, which cannot reach the
        // user-data area `heap_deform_tuple` needs) to the owned `FormedTuple`
        // (header + user-data area). `replace_cur_tuple_from_slot` now stores
        // `ExecCopySlotHeapTuple(slot)` into it and `cur_tuple_getattr`
        // `heap_getattr`s it against the producing slot's descriptor — both
        // installed by backend-executor-execTuples::init_seams.)
        //
        // DESIGN_DEBT (TD-FORCESTORE-HEAPTUPLE-DATALESS): `exec_force_store_heap_tuple`
        // (`ExecForceStoreHeapTuple`) stays uninstalled. The seam carries
        // `tuple: &HeapTupleData`, but the C body's non-heap-slot branch deforms
        // the tuple (`heap_deform_tuple(tuple, slot->tts_tupleDescriptor,
        // slot->tts_values, slot->tts_isnull)`) which needs the user-data area —
        // and the owned `HeapTupleData` / its callers' `xs_hitup`
        // (`tableam::IndexScanDesc.xs_hitup: HeapTuple`) carrier carry NO data
        // area in this model (it lives only in `FormedTuple.data`). Installing a
        // body that synthesizes an empty data area would silently mis-deform
        // virtual/minimal target slots. Faithful install needs the tree-wide
        // carrier-widen of `xs_hitup` (and the seam arg) from data-less
        // `HeapTuple` to data-bearing `FormedTuple` — same class as the curTuple
        // widen above, but it crosses the tableam relscan carrier (out of lane).
        ("backend_executor_execTuples", "exec_force_store_heap_tuple"),
        // backend-foreign-foreign owns foreign/foreign.c's READ accessors + the
        // FDW-routine resolution AND now the pg_foreign_* catalog-write/DDL seams
        // commands/foreigncmds.c issues (insert/update/set_owner/lookup/options
        // for FDW/SERVER/USER MAPPING/FOREIGN TABLE + validate_options +
        // import_classify_raw_stmt/import_set_schemaname) — all installed in its
        // init_seams(). The heap_form_tuple + CatalogTupleInsert/Update value
        // layer crosses the catalog/indexing.c-owned
        // catalog_tuple_{insert,update}_pg_foreign_* seams (listed below; they
        // panic until indexing.c lands — sanctioned mirror-pg-and-panic). Two of
        // the installed seams are themselves seam-and-panic bodies (still
        // INSTALLED, so removed from this list): `validate_options` reaches the
        // unported text[]-Datum array build + OidFunctionCall2(fdwvalidator)
        // fmgr-dispatch bridge; `import_classify_raw_stmt`/`import_set_schemaname`
        // reach the unported RawStmt parser-node field accessor (and are only
        // reachable after fdw_import_foreign_schema, a runtime FDW vtable with no
        // provider). See DESIGN_DEBT.md.
        //
        // What REMAINS here for backend-foreign-foreign are the FDW-provider
        // runtime-vtable callbacks (node->fdwroutine->X) dispatched through a
        // runtime FDW vtable: no FDW provider (postgres_fdw/contrib) is ported,
        // so there is nothing to ::set(). `fdw_import_foreign_schema` likewise
        // dispatches the provider's ImportForeignSchema vtable callback.
        ("backend_foreign_foreign", "begin_direct_modify"),
        ("backend_foreign_foreign", "begin_foreign_scan"),
        ("backend_foreign_foreign", "end_direct_modify"),
        ("backend_foreign_foreign", "end_foreign_scan"),
        ("backend_foreign_foreign", "estimate_dsm_foreign_scan"),
        ("backend_foreign_foreign", "fdw_import_foreign_schema"),
        ("backend_foreign_foreign", "foreign_async_configure_wait"),
        ("backend_foreign_foreign", "foreign_async_notify"),
        ("backend_foreign_foreign", "foreign_async_request"),
        ("backend_foreign_foreign", "initialize_dsm_foreign_scan"),
        ("backend_foreign_foreign", "initialize_worker_foreign_scan"),
        ("backend_foreign_foreign", "iterate_direct_modify"),
        ("backend_foreign_foreign", "iterate_foreign_scan"),
        ("backend_foreign_foreign", "recheck_foreign_scan"),
        ("backend_foreign_foreign", "reinitialize_dsm_foreign_scan"),
        ("backend_foreign_foreign", "rescan_foreign_scan"),
        ("backend_foreign_foreign", "shutdown_foreign_scan"),
        ("backend_foreign_foreign", "stamp_scan_slot_tableoid"),
        // NOTE: the catalog/indexing.c-owned pg_foreign_* catalog-tuple
        // insert/update seams the foreigncmds catalog-write seams above delegate
        // to (catalog_tuple_{insert,update}_pg_foreign_{data_wrapper,server,
        // user_mapping,table}) are NOT listed here: their owner
        // backend-catalog-indexing is `todo` (not complete) in CATALOG.tsv, so
        // the recurrence guard already exempts every indexing-seams seam (same
        // as the existing catalog_tuple_insert_pg_namespace/pg_am). They panic
        // until indexing.c lands — sanctioned mirror-pg-and-panic.
        // DESIGN_DEBT: `publish_wtparam_slot` is the *deposit* end of the
        // RecursiveUnion<->WorkTableScan cross-node aliasing channel. In C
        // `ExecInitRecursiveUnion` does `prmdata->value = PointerGetDatum(rustate)`,
        // storing a *live pointer* to the ancestor's `RecursiveUnionState` into the
        // reserved `Param` slot (`es_param_exec_vals[wtParam]`), which a descendant
        // `WorkTableScan` recovers via `resolve_rustate`
        // (`castNode(RecursiveUnionState, DatumGetPointer(param->value))`). Our
        // `ParamExecData.value` is the bare-word `Datum(usize)` with no pointer
        // lane, and `WorkTableScanStateData.rustate` is an owned
        // `Option<Box<RecursiveUnionStateData>>`, not an alias of the ancestor's
        // `PgBox`. Installing the deposit faithfully requires the same unported
        // datum-pointer/handle-arena machinery the recovery side
        // (`resolve_rustate`, also still seam-and-panic) needs — a contract
        // redesign of the cross-node aliasing channel, not a `::set()`. Pay down
        // alongside the `resolve_rustate` recovery channel.
        ("backend_executor_nodeWorktablescan", "publish_wtparam_slot"),
        // nodes-core re-homes these two cross-unit DESIGN_DEBT seams onto its own
        // -seams crate so the guard can track them (see DESIGN_DEBT.md). Both
        // read the unported call-expression node tree (FuncExpr/OpExpr/RowExpr/
        // Const) and fold into funcapi's `internal_get_result_type` /
        // `build_function_result_tupdesc_t` tupdesc spine — neither the node
        // model nor a funcapi callback seam exists yet, so the body stays
        // seam-and-panic (mirror-pg-and-panic) until those owners land.
        ("backend_nodes_core", "call_stmt_result_desc"),
        ("backend_nodes_core", "get_expr_result_type_node"),
        // DESIGN_DEBT (provider-unported): the CustomScan/CustomScanState
        // provider callbacks (extensible.h `CustomScanMethods` /
        // `CustomExecMethods`, dispatched by nodeCustom.c through
        // `node->methods->X`) are installed by a custom-scan-provider extension.
        // There is no in-tree custom-scan provider — exactly the FDW-provider
        // case above (`backend_foreign_foreign` begin/end/iterate_foreign_scan
        // et al.) — so there is nothing to `::set()`: the seams stay
        // seam-and-panic (mirror-pg-and-panic) until a provider lands.
        // backend-nodes-extensible owns the registry side (Register*/Get* method
        // tables) which it installs in init_seams(). See DESIGN_DEBT.md.
        ("backend_nodes_extensible", "begin_custom_scan"),
        ("backend_nodes_extensible", "create_custom_scan_state"),
        ("backend_nodes_extensible", "end_custom_scan"),
        ("backend_nodes_extensible", "estimate_dsm_custom_scan"),
        ("backend_nodes_extensible", "exec_custom_scan"),
        ("backend_nodes_extensible", "initialize_dsm_custom_scan"),
        ("backend_nodes_extensible", "initialize_worker_custom_scan"),
        ("backend_nodes_extensible", "mark_pos_custom_scan"),
        ("backend_nodes_extensible", "reinitialize_dsm_custom_scan"),
        ("backend_nodes_extensible", "rescan_custom_scan"),
        ("backend_nodes_extensible", "restr_pos_custom_scan"),
        ("backend_nodes_extensible", "shutdown_custom_scan"),
        // DESIGN_DEBT (TD-BUFMGR-AIO-GUC): one bufmgr-seam `::call`ed in a live
        // consumer (backend-storage-aio-read-stream) whose value comes from the
        // unported aio.c machinery, not this owner:
        //   * io_method_sync — the `io_method == IOMETHOD_SYNC` test; the `io_method`
        //     GUC/enum lives in the unported aio.c, not this owner.
        // (maintenance_io_concurrency / bgwriter_flush_after RETIRED: the GUC owner
        // crate now `::set`s both accessors from their backing GUC variables, so
        // the seams are installed and these allowlist entries no longer fire.)
        // (buffer_manager_shmem_size / buffer_manager_shmem_init RETIRED: the owner
        // now installs both — BufferManagerShmemSize is the faithful add_size/
        // mul_size accumulator and BufferManagerShmemInit allocate-or-attaches the
        // four named buffer-pool regions via ShmemInitStruct, then publishes the
        // process-local pool view, mirroring procarray's ProcArrayShmemInit.)
        // (io_method_sync RETIRED: the bufmgr owner now `::set`s it to read the
        // live `io_method` enum GUC slot — `vars::io_method.read() ==
        // IOMETHOD_SYNC` — backed by aio-methods at boot, the same `vars::*.read()`
        // idiom as the other bufmgr GUC getter seams.)
        // (The three SetLatch-by-proc latch seams — set_latch_by_proc_number /
        // set_latch_for_proc_pid / set_latch_for_procno — are now INSTALLED.
        // The latch<->proc handle spaces were unified: a `LatchHandle` is a
        // tagged union (`LatchKind::Local` registry slot vs `LatchKind::Proc`
        // proc number), and the latch unit resolves a proc-tagged handle to the
        // real `&ProcGlobal->allProcs[procno].procLatch` through the proc unit's
        // `with_proc_latch` seam — no separate side-table for proc latches.
        // `set_latch_for_proc_pid` maps PID->ProcNumber via procarray's
        // `BackendPidGetProc`.)
        // (set_postmaster_death_watch_cloexec RETIRED: the postmaster unit now
        // installs it — the death-watch fd (`postmaster_alive_fds`) is
        // postmaster.c's own, so the `fcntl(.., F_SETFD, FD_CLOEXEC)` runs in the
        // postmaster crate and is consumed by miscinit's InitPostmasterChild.)
        // (`initialize_fast_path_locks` RESOLVED: the C `InitializeFastPathLocks()`
        // lives in postinit.c — declared in miscadmin.h, not proc.c — and is fully
        // ported + installed by the postinit unit. The mis-homed
        // backend-storage-lmgr-proc-seams duplicate decl was removed and ipci.c now
        // calls the real postinit seam. The clog.c group XID-status update set —
        // clog_group_first_* / *_clog_group_* — was retired once clog.c
        // TransactionGroupUpdateXidStatus + procarray's InitProcGlobal arena landed;
        // those 13 seams are now installed by inward_seams over ProcGlobal->
        // clogGroupFirst + the per-PGPROC clogGroup* fields.)
        // (has_bypassrls_privilege RESOLVED: the acl owner now installs it — a
        // superuser_arg short-circuit + AUTHOID-syscache `rolbypassrls` read, after
        // widening the AuthIdRow projection to carry rolbypassrls.)
        // RESOLVED (aclchk F1): `object_ownercheck` (catalog/aclchk.c) is now
        // installed by the ported `backend-catalog-aclchk` owner crate, over the F0
        // generic `object_owner_acl` syscache projection (the `get_object_catcache_oid`
        // dispatch) plus a `table_open` + `systable_beginscan` + `heap_deform_tuple`
        // fallback for cache-less catalogs (`cacheid == -1`). Allowlist entry removed.
        // (#159 STEP C plancache de-handle RETIRED: the 16 inward plancache seams —
        // create/complete/save/drop/get/release_cached_plan, cached_plan_stmt_list,
        // cached_plan_get_target_list, and the plansource_* field accessors — are now
        // INSTALLED by backend-utils-cache-plancache::init_seams. The owner no longer
        // stores opaque RawStmt/Query/PlannedStmt/TupleDesc/SearchPathMatcher handles:
        // CachedPlanSourceData/CachedPlanData/CachedExpressionData now own the value
        // trees in private MemoryContexts (clone_in + 'static drop-order, the portalmem
        // pattern), so the seams cross owned `'mcx` values instead of registry handles.
        // The handle REGISTRY is kept ONLY for plan/source u64 IDENTITY + refcount +
        // gplan sharing (faithful to C's shared CachedPlan*).)
        // DESIGN_DEBT (TD-DFMGR-DYNLOADER): the dynamic-library / extension-hook
        // surface of dfmgr.c + miscinit.c. `load_archive_module_init` is
        // `load_external_function(filename, "_PG_archive_module_init", ...)` — it
        // `dlopen`s an archive-module `.so` and resolves its init symbol; the
        // dynamic loader (`load_external_function` / `load_file`) is inherently
        // unported in an idiomatic-Rust build (no `.so` ABI surface). The
        // `shmem_request_hook` / `shmem_request_hook_present` seams are now
        // installed by backend-utils-init-miscinit (which owns the
        // `shmem_request_hook` pointer, NULL in core PG); only the dynamic-loader
        // leg below remains owner-unported.
        ("backend_utils_fmgr_dfmgr", "load_archive_module_init"),
        // RESOLVED: `setup_signal_handlers` (the slot-sync worker's
        // `pqsignal(...)` block, slotsync.c:1413-1421) is now installed by the
        // slotsync owner's init_seams() (cross-crate install — the C function's
        // true home is slotsync.c, not miscinit.c). interrupt.c
        // (SignalHandlerForConfigReload) and procsignal.c
        // (procsignal_sigusr1_handler) are merged, and the postgres.c handler
        // bodies it wires — `die` / `StatementCancelHandler` /
        // `FloatExceptionHandler` — are now all installed by the now-merged
        // backend-tcop-postgres unit (a `float_exception_handler` seam returning
        // the SIGFPE handler fn-pointer was added alongside `die_signal_handler`).
        // Allowlist entry removed.
        // RETIRED (task #161): `record_from_values` is now installed by funcapi's
        // init_seams(). The composite/record-Datum carrier bridge landed.
        // NOTE: `value_srf_unported` is now INSTALLED by funcapi's init_seams() as
        // an EXPLICIT honest seam-and-panic (mirror-pg-and-panic) — its body lives
        // in `srf_support::value_srf_unported` and panics loudly naming the missing
        // value-per-call SRF machinery. It is therefore no longer an uninstalled
        // contract divergence and must NOT be allowlisted here (the guard would
        // flag a stale entry).
        // RETIRED: `init_process_globals` is now installed by init-small's
        // init_seams() (the InitProcessGlobals body landed there, homed next to
        // the MyStartTime[stamp] setters until postmaster.c lands).
        // RETIRED (TD-PORTAL-HANDLE de-handle): PREPARE/EXECUTE's portal-run tail
        // was migrated off the parsestmt opaque handle newtypes
        // (`PortalHandle`/`QueryCompletionHandle`/`SnapshotHandle`) onto the
        // value-typed `types_portal::Portal` (`Rc<RefCell<PortalData>>`),
        // `Rc<SnapshotData>`, and `QueryCompletion`. prepare now calls the base
        // `portalmem`/`pquery`/`snapmgr` seams (create_new_portal /
        // portal_set_visible / portal_define_query_list / portal_drop /
        // portal_start / portal_run / get_active_snapshot), all installed by
        // their owners. The `-pre-seams` slice crates and their allowlist
        // entries are gone.
        // DESIGN_DEBT (TD-PORTAL-CURSOR): `with_running_cursor` lends borrows of
        // the running `EStateData`/`PlanStateNode` tree (RunningCursorState) for
        // execCurrentOf. Those carrier types are the executor de-handle keystone
        // (#167 EState/Plan ownership, #169 consolidated de-handle); the portal's
        // `queryDesc->estate` borrow cannot be lent until that lands.
        // Keystone-blocked.
        ("backend_utils_mmgr_portalmem", "with_running_cursor"),
        // RETIRED (drain re-sweep): the WAL page-read driver
        // (xlogrecovery.c XLogPageRead / WaitForWALToBecomeAvailable + the prefetcher
        // recovery read-record leg) has LANDED in backend-access-transam-xlogrecovery
        // (walrecovery.rs). xlogrecovery's init_seams() now cross-installs all five
        // formerly-blocked seams — prefetcher_begin_read / prefetcher_read_record
        // (the read-record entry points) and xlog_rec_info / xlog_rec_rmid /
        // xlog_rec_total_len (the decoded-record accessors, now keyed off the held
        // reader). readrecord.rs's ReadRecord retry loop calls them against the
        // installed bodies. Allowlist entries removed.
        // RETIRED (drain re-sweep): `prefetcher_compute_stats`
        // (XLogPrefetcherComputeStats) is now installed by xlogrecovery's
        // init_seams() alongside the rest of the page-read driver leg
        // (walrecovery.rs) — the held prefetcher/reader is now allocated by the
        // landed recovery driver, so the stats call has a real body.
        //
        // RETIRED (walreceiverfuncs streaming-control): the 6 streaming-control
        // seams the recovery page-read driver reaches on its standby legs are now
        // installed with real bodies:
        //   * wal_rcv_streaming / request_xlog_streaming /
        //     get_wal_rcv_flush_rec_ptr_full — the genuine walreceiverfuncs.c
        //     routines, installed by backend-replication-walreceiverfuncs'
        //     init_seams (WalRcvStreaming / RequestXLogStreaming /
        //     GetWalRcvFlushRecPtr). The `&str` conninfo/slotname divergence is
        //     resolved by an in-owner adapter mapping both to `Some(bytes)`.
        //   * xlog_shutdown_wal_rcv / set+reset_install_xlog_file_segment_active —
        //     these are xlog.c functions touching the xlog-owned
        //     `XLogCtl->InstallXLogFileSegmentActive` flag under ControlFileLock;
        //     ported in backend-access-transam-xlog (write.rs) and installed from
        //     xlog's init_seams (the real owner). XLogShutdownWalRcv reaches the
        //     inner ShutdownWalRcv via the new `shutdown_wal_rcv` seam.
        // Allowlist entries removed.
        // (analyze_and_rewrite_varparams — now ported: analyze.c's
        //  parse_analyze_varparams [setup_parse_variable_parameters +
        //  transformTopLevelStmt + check_variable_parameters] in
        //  backend-parser-analyze, wrapped by postgres.c's
        //  pg_analyze_and_rewrite_varparams [param-completeness check +
        //  pg_rewrite_query] installed from backend-tcop-postgres.)
        // (run_post_parse_analyze_hook — now installed (NULL-hook no-op) from
        //  backend-parser-analyze; the C `if (post_parse_analyze_hook)` guard
        //  falls through with no extension loaded.)
        // (#159 STEP C plancache de-handle RETIRED: the 16 -pc-seams handle forms
        // [QueryHandle/RawStmtHandle/AnalyzedQueryHandle/QueryListHandle/...] plus
        // analyze_and_rewrite_fixedparams / analyze_and_rewrite_withcb /
        // analyze_requires_snapshot / stmt_requires_parse_analysis /
        // query_requires_rewrite_plan / the query_* field accessors /
        // walk_query_sublinks_for_locks are no longer called. plancache now owns
        // RawStmt<'static>/Query<'static> values and calls the value seams
        // stmt_requires_parse_analysis_value / analyze_requires_snapshot_value /
        // query_requires_rewrite_plan_value / pg_analyze_and_rewrite_fixedparams_params,
        // reads Query fields directly, and walks sublinks via node_walker.)
        // (TD-PARSETYPE-TYPENAME-CARRIER RETIRED: backend-parser-driver now
        // installs `raw_parse_type_name` from its init_seams(). It drives
        // `raw_parser(str, RAW_PARSE_TYPE_NAME)` in a private MemoryContext,
        // pulls the single `TypeName` node out of the RawStmt wrapper, and
        // bridges the arena `types_nodes::rawnodes::TypeName<'mcx>` into the owned
        // `types_parsenodes::TypeName` (the arena->owned reconcile mirrors
        // parse_type.c's `raw_typename_to_parse`) before the context drops.)
        // (TD-TUPLESORT-INDEX-VARIANTS retired by F3b: the tuplesort unit now
        // installs tuplesort_begin_index_btree/hash/gist + putindextuplevalues +
        // getindextuple from its init_seams(), with real comparetup_index_* /
        // writetup_index / readtup_index / removeabbrev_index bodies.)
        // DESIGN_DEBT (TD-INDEXING-PERCATALOG-OWNERS): backend-catalog-indexing's
        // per-catalog forming/mutation bodies have now been PORTED + installed in
        // family2.rs (pg_type insert/update/rename, pg_constraint, pg_depend/
        // pg_shdepend, pg_sequence, pg_class/pg_index, pg_largeobject,
        // pg_db_role_setting, namespace, the foreign-data catalogs, the cluster
        // open/close/delete engine pass-throughs, get_catalog_object_by_oid,
        // set_relation_rule_status, set_pg_class_*). Those allowlist entries were
        // therefore DELETED — the seams are real installs now.
        //
        // The typecmds.c F3/F4 narrow single-column pg_type mutators
        // (catalog_tuple_update_{typowner_typacl,typnamespace,typnotnull,
        // typdefault,attrs}_pg_type) are now INSTALLED by backend-catalog-
        // indexing's family2 — each re-fetches the row by `type_oid`, deforms it,
        // replaces only the targeted column(s) (heap_modify_tuple over the
        // selectively-set `replaces[]`), and CatalogTupleUpdate — so their
        // entries were DELETED.
        //
        // The generic update_object_owner_tuple (alter.c AlterObjectOwner_internal)
        // is now INSTALLED by backend-catalog-indexing's family2 — it deforms the
        // re-fetched row, sets the owner column, re-serializes aclnewowner(acl,
        // old, new) into the aclitem[] varlena via the shared acl_new_owner_datum
        // codec, CatalogTupleUpdate + UnlockTuple — so its entry was DELETED.
        // ===================================================================
        // AUDIT-FIX #345 — blind-spot revealed by the col-4-fallback guard fix.
        // The 24 merged/audited rows with an EMPTY `crate` column (and the
        // 3-column rows with no `crate` column at all) were silently exempted
        // from the install guard. With the fallback in `complete_crate_dirs`
        // they are now scoped, surfacing these declared-but-uninstalled,
        // actually-`::call`ed seams on complete owners. Each below is EITHER a
        // genuinely-unported owner leg OR a handle/contract divergence — NOT a
        // pure-wiring miss. The owners' wireable legs were installed instead.
        // ===================================================================
        //
        // -- backend-nodes-copyfuncs --
        // (#159 STEP C plancache de-handle RETIRED: the 9 -pc-seams handle forms —
        // copy_query_list / copy_plan_list / copy_raw_stmt / copy_analyzed_query /
        // copy_expr / query_list_elements / plan_list_elements /
        // extract_query_dependencies / expression_planner_with_deps — are no longer
        // called. plancache de-handled onto owned node values: it clones via
        // Query/PlannedStmt/RawStmt/Expr::clone_in into private MemoryContexts and
        // calls the value seams extract_query_dependencies_value /
        // expression_planner_with_deps_value instead of the opaque-token handle forms.)
        // (list_member_oid — now INSTALLED by backend-nodes-copyfuncs from its
        //  init_seams(): the bare list.c:722 primitive is a linear OID-membership
        //  scan over the caller-supplied `&[Oid]` slice, with no cross-crate
        //  dependency, so the owner installs it directly. Entry DELETED.)
        //
        // -- backend-commands-trigger (F1 firing/DDL leg still todo) --
        // DESIGN_DEBT (TD-TRIGGER-F1): trigger.c is CATALOG `merged` only for
        // its F0 value-type keystone (the trigger value structs landed). The
        // scalar `TriggerData` / `Trigger` field accessors ri_triggers.c reads
        // (tg_event, tg_relation_{oid,name,namespace,owner,is_partitioned},
        // RIAtt{Name,Type,Collation}, tg_trigger, tg_trig/newslot, trigger_*,
        // called_as_trigger) are now installed: they resolve their handle to the
        // live TriggerData on the current-trigger side-channel and read the field
        // (crates/backend-commands-trigger/src/ri_accessors.rs) — the owned
        // analogue of dereferencing fcinfo->context.
        //
        // What remains uninstalled below needs the per-row AFTER-trigger firing
        // substrate (EState-owned slot materialization + heap-scan family) or the
        // trigger-DDL family, both separate campaigns:
        //   * tg_trigtuple / tg_newtuple — the OLD/NEW HeapTuple copies the
        //     firing path leaves NULL (per-row fetch substrate).
        //   * tg_relation_tuple_satisfies_snapshot_self, tg_relation (the live
        //     heap Relation), slot_tid / slot_attisnull / slot_is_current_xact_tuple
        //     / slot_getattr / pk_datum_image_eq — drive the table-AM / slot value
        //     deform against the OLD/NEW TupleTableSlots, which AfterTriggerExecute
        //     does not yet materialize (tg_trigslot/tg_newslot left NULL, per-row
        //     fetch loud-panics).
        //   * RemoveTriggerById / renametrig — the unported catalog-write DDL leg
        //     (CreateTrigger family: systable scans over pg_trigger, renametrig
        //     partition recursion, RangeVarGetRelidExtended callbacks).
        // Install + DELETE each as that substrate / DDL family lands.
        ("backend_commands_trigger", "tg_relation_tuple_satisfies_snapshot_self"),
        ("backend_commands_trigger", "tg_trigtuple"),
        ("backend_commands_trigger", "tg_newtuple"),
        ("backend_commands_trigger", "tg_relation"),
        ("backend_commands_trigger", "slot_tid"),
        ("backend_commands_trigger", "slot_attisnull"),
        ("backend_commands_trigger", "slot_is_current_xact_tuple"),
        ("backend_commands_trigger", "slot_getattr"),
        ("backend_commands_trigger", "pk_datum_image_eq"),
        ("backend_commands_trigger", "RemoveTriggerById"),
        ("backend_commands_trigger", "renametrig"),
        //
        // -- backend-access-index-genam (build_index_value_description unported) --
        // DESIGN_DEBT (TD-GENAM-RELCACHE-SCANS): the genam unit ported genam.c's
        // systable_* primitive engine (begin/getnext/endscan, installed) AND the
        // relcache catalog scan-and-decode helpers (ScanPgRelation /
        // RelationBuildTupleDesc's scan_pg_class/scan_pg_attribute +
        // RelationGetIndexList / GetStatExtList / GetFKeyList / GetExclusionInfo /
        // AttrDefaultFetch / CheckNNConstraintFetch — all bodied + installed in
        // src/decode.rs, so their allowlist entries were removed). Only
        // `build_index_value_description` (the per-key out-function +
        // ACL-visibility render) remains a genam.c function not yet bodied;
        // install + DELETE when the genam unit ports its render body.
        ("backend_access_index_genam", "build_index_value_description"),
        //
        // (TD-RELCACHE-INDEX-NODETREE RETIRED for the relcache owner's four
        // `BuildIndexInfo`/`BuildDummyIndexInfo` accessors: all of
        // `relation_get_index_expressions`, `relation_get_index_predicate`,
        // `relation_get_dummy_index_expressions`, and `relation_get_exclusion_info`
        // are now installed from `backend-utils-cache-relcache::init_seams`.
        // The three expression/predicate/dummy accessors compute their NIL
        // quick-exit faithfully off the owned entry — `rd_index == None`, no zero
        // `indkey` entry (the on-disk `InvalidAttrNumber` marker = an expression
        // column), or `heap_attisnull(indpred)` via the `pg_index_has_predicate`
        // syscache owner — which is the path every system-catalog index (all
        // simple-column, none partial) takes; only a real expression / predicate
        // / dummy-Const index reaches the still-unported `stringToNode` node-tree
        // decode, which mirror-PG-and-panics. `relation_get_exclusion_info` is NOT
        // a node-tree decode at all: `conexclop` is a plain 1-D Oid array, decoded
        // by the real ported `genam::relcache_exclusion_info` body (pg_constraint
        // scan + `get_opcode`/`get_op_opfamily_strategy`), so the relcache owner
        // runs it + caches the three arrays + returns them — fully installed.)
        //
        // -- backend-utils-cache-relcache-nodexform (#159 planner-arena keystone) --
        // The nodexform owner (sanctioned relcache.c sibling-split) installs its
        // three live seams (open_index_attrs, relation_build_publication_desc,
        // publication_desc). The three index node-tree CACHING seams below stay
        // uninstalled: they cache a built `stringToNode` + eval/canonicalize node
        // tree into the relcache entry's `rd_indexprs`/`rd_indpred`/dummy-Const
        // fields, which the TRIMMED owned entry does not carry, AND their only
        // consumers (`get_index_expressions`/`get_index_predicate` in the
        // planner-catalog read path) panic on the unmodeled planner-arena node
        // projection regardless (#159 planner-values keystone); the dummy seam has
        // no live consumer. Faithful mirror-pg-and-panic until the relcache
        // node-tree cache fields + the planner-arena projection land. Install +
        // DELETE these three then.
        ("backend_utils_cache_relcache_nodexform", "index_expressions"),
        ("backend_utils_cache_relcache_nodexform", "index_predicate"),
        ("backend_utils_cache_relcache_nodexform", "dummy_index_expressions"),
        //
        // (TD-INDEXING-APPEND-ATTRIBUTE-TUPLES RETIRED: `AppendAttributeTuples`
        // (catalog/index.c) is now INSTALLED by backend-catalog-indexing's
        // family3 — it opens pg_attribute RowExclusiveLock, builds one
        // `PgAttributeInsertRow` per index column from
        // `RelationGetDescr(indexRelation)` (the per-attno `Form_pg_attribute`,
        // whose `attrelid` `InitializeAttributeOids` already stamped), applies the
        // optional `attopts`/`stattargets` overrides, and delegates to the
        // already-ported `catalog_insert_pg_attribute_tuples`
        // (`InsertPgAttributeTuples`) — so its entry was DELETED.)
        //
        // (TD-ENCNAMES-ICU RETIRED: `is_encoding_supported_by_icu` —
        // `common/encnames.c`'s `pg_enc2icu_tbl` reader (encnames.c:461),
        // declared in `backend-utils-mb-mbutils-seams` but mbutils.c never calls
        // it — is now installed from its true C owner, the encnames unit
        // `common-extra-encnames-fgram::init_seams()` (cross-crate install). The
        // `recomputeNamespacePath` ICU branch (namespace.c:2323) reaches a real
        // body.)
        // DESIGN_DEBT (TD-PGDATABASE-ACLNEWOWNER): `aclnewowner_datacl`
        // (declared in `backend-catalog-pg-database-seams`) is dbcommands.c's
        // AlterDatabaseOwner ACL rewrite — `aclnewowner(DatumGetAclP(datacl),
        // olddba, newowner)` returning the re-encoded `aclitem[]` varlena bytes.
        // `pg_database.datacl` crosses the FormPgDatabase carrier as opaque
        // varlena bytes by design; decoding the `aclitem[]` array out of those
        // bytes and re-encoding `aclnewowner`'s result needs the array/varlena
        // deconstruct+construct layer (`DatumGetAclP` / `construct_array`), which
        // the pg_database catalog owner does not yet reach. `aclnewowner` itself
        // (backend-utils-adt-acl) is ported but takes `&[AclItem]`, not raw bytes.
        // Loud-panics until the pg_database owner wires the aclitem[] varlena
        // decode/encode around `aclnewowner`. DELETE this entry then.
        ("backend_catalog_pg_database", "aclnewowner_datacl"),
        // DESIGN_DEBT (TD-STORAGE-DBCOPY-ENGINE): `create_and_copy_relation_data`
        // and `scan_source_database_pg_class` (declared in
        // `backend-catalog-storage-seams`) are the createdb WAL_LOG copy engine.
        // `create_and_copy_relation_data` is storage.c's
        // `RelationCreateStorage` + per-fork `RelationCopyStorageUsingBuffer` +
        // `smgrimmedsync`; `RelationCopyStorageUsingBuffer` is NOT ported anywhere
        // in this model. `scan_source_database_pg_class` is dbcommands.c's
        // cross-database raw buffered `pg_class` scan (smgr/`GetAccessStrategy`/
        // `RegisterSnapshot`/`ReadBufferWithoutRelcache`/page-item walk gated by
        // `HeapTupleSatisfiesVisibility`), whose buffered-bulk-read engine the
        // storage owner does not yet reach. Both loud-panic until the buffered
        // storage-copy + cross-DB raw-scan engine lands. DELETE these entries then.
        ("backend_catalog_storage", "create_and_copy_relation_data"),
        ("backend_catalog_storage", "scan_source_database_pg_class"),
        // DESIGN_DEBT (TD-JSONFUNCS-FMGR-ARG-DETOAST): jsonfuncs.c's SQL entry
        // points (`json[b]_object_keys`/`_each`/`_array_elements`/`populate_*`/
        // `to_record(set)`) read a `json`/`jsonb` varlena argument and (for
        // populate_recordset) a composite `record` argument from the fmgr call
        // frame. The repo's trimmed `FunctionCallInfoBaseData` carries args as
        // bare-word `types_datum::Datum`, and the bare-word -> detoasted bytes /
        // -> `FormedTuple` conversion is the project-wide fmgr argument-detoast
        // boundary that funcapi (the call-frame owner) has not yet grown. These
        // two seams (`srf_arg_varlena_bytes` for the json/jsonb arg bytes,
        // `srf_arg_record` for the composite record arg) are declared on funcapi
        // and called by jsonfuncs; funcapi cannot install them until that
        // detoast boundary lands, so they loud-panic on a real SRF call path.
        // DELETE these entries when funcapi grows the fmgr arg-detoast accessors.
        ("backend_utils_fmgr_funcapi", "srf_arg_varlena_bytes"),
        ("backend_utils_fmgr_funcapi", "srf_arg_record"),
        // DESIGN_DEBT (TD-TABLECMDS-F1F6-UNPORTED): tablecmds.c is a 22k-LOC giant
        // ported in families. Only FAMILY F0 (relation create/drop/truncate +
        // on_commit + small helpers) is landed in `backend-commands-tablecmds`
        // (audited). The seams below are tablecmds.c functions belonging to
        // the not-yet-ported families F1-F6 (the ALTER phase machine, column /
        // constraint / ALTER TYPE / inheritance-partition / RENAME / SET SCHEMA /
        // change-owner machinery, and the sequence-create driver). They are
        // declared in `backend-commands-tablecmds-seams` and `::call`ed by already-
        // merged consumers (commands/alter.c, parse-utilcmd, sequence.c, REASSIGN
        // OWNED, etc.). The F0 owner crate has no body for them yet, so it cannot
        // install them — they loud-panic (mirror-pg-and-panic) on a real call path
        // until the owning family lands. DELETE each entry as its family ports the
        // function and installs the seam in `init_seams()`.
        ("backend_commands_tablecmds", "RenameRelation"),
        ("backend_commands_tablecmds", "renameatt"),
        ("backend_commands_tablecmds", "RenameConstraint"),
        ("backend_commands_tablecmds", "AlterTableNamespace"),
        ("backend_commands_tablecmds", "AlterTableNamespaceInternal"),
        ("backend_commands_tablecmds", "alter_relation_namespace_internal"),
        // `at_exec_change_owner` retired from this list: the ALTER-phase spine
        // (FAMILY F1, now landed) `::call`s it from `ATExecCmd`'s AT_ChangeOwner
        // arm, so the guard classifies it as an OUTWARD dependency seam of
        // tablecmds (real owner = the still-unported ATExecChangeOwner body),
        // not an uninstalled inward contract. It loud-panics until that body
        // lands and installs it.
        ("backend_commands_tablecmds", "rename_relation_internal"),
        ("backend_commands_tablecmds", "reset_rel_rewrite"),
        // DESIGN_DEBT (TD-INDEXCREATE-BOOTSTRAP-LEGS): catalog/index.c's
        // `index_create` reaches three legs ONLY in bootstrap mode (or via the
        // deferrable-constraint path of `index_constraint_create`) whose owners
        // exist but cannot install the seam without a prerequisite keystone:
        //
        //  * `index_register` (bootstrap.c): the bootstrap owner stores the
        //    registered index's `IndexInfo` on its no-gc list as `IndexInfo
        //    <'static>`, but the seam crosses a per-query `IndexInfo<'mcx>`. The
        //    owner cannot soundly promote `'mcx` -> `'static`; installing needs
        //    the bootstrap-context lifetime keystone (a real 'static deep-copy of
        //    IndexInfo into the bootstrap IL context). Loud-panics until then;
        //    only reached during initdb's bootstrap, which the CREATE INDEX gate
        //    does not exercise.
        //  ( `relation_init_index_access_info` RETIRED: the relcache owner now
        //    installs it from `seams.rs` — the registry-mutable-entry accessor
        //    `core_entry_store::with_relation_mut(oid, ..)` exists, so the by-OID
        //    seam borrows the cell mutably and runs `RelationInitIndexAccessInfo`
        //    on it. The body only re-reads OTHER catalogs; the one self-resolve
        //    (rd_opcoptions priming) was already deferred to
        //    `force_index_att_options`, so no re-entrant borrow. )
        //  * `create_unique_key_recheck_trigger` (the `CreateTrigger` call in
        //    index_constraint_create's deferrable leg): the trigger manager owner
        //    has not ported `CreateTrigger` yet. Loud-panics until trigger.c's
        //    CreateTrigger lands; only reached for a DEFERRABLE PK/UNIQUE.
        //
        // Delete each entry when its owner installs the seam.
        ("backend_bootstrap_bootstrap", "index_register"),
        ("backend_commands_trigger", "create_unique_key_recheck_trigger"),

        // ============================================================
        // `ported`-status owner deferrals (surfaced when is_complete_status
        // widened to accept `ported`, 2026-06-17). Each is a declared+called
        // seam on a code-complete owner whose FAITHFUL provider genuinely does
        // not exist yet (keystone-blocked / unported subsystem / SSL-vtable
        // floor). The owner legitimately mirror-pg-and-panics this surface.
        // Delete each entry when its named blocker lands and installs the seam.
        // ============================================================

        // (#159 STEP C plancache de-handle RETIRED: the planner plancache-consumer
        // slice — plan_queries + the 9 pstmt_* PlannedStmt accessors — and the
        // tcop/utility utility_contains_query handle seam are no longer called.
        // plancache now owns PlannedStmt<'static> values: it plans via the value
        // seam pg_plan_queries_value, reads transientPlan/dependsOnRole/invalItems/
        // commandType/utilityStmt/rtable fields and planTree.total_cost
        // [PlannedStmt::plan_total_cost] directly off the owned stmts, and mirrors
        // UtilityContainsQuery as a local value recursion.)

        // DESIGN_DEBT (TD-VACUUMPARALLEL-OUTWARD): the `*_pv`/`*_basvac`/
        // `vacuum_*_nworkers` seams live in `backend-commands-vacuum-seams` (the
        // shared declaration home) but are `::call`ed only by vacuumparallel.c.
        // The whole tranche is now INSTALLED — by
        // backend-commands-vacuumparallel::init_seams (the outward subsystems)
        // and backend-commands-vacuum::init_seams (the GUC + index path):
        //
        // RESOLVED (vacuumparallel-worker-mcx lane): the #4 by-OID heap/index
        // reopen keystone. parallel_vacuum_main now holds its own transaction
        // Mcx and reopens the heap + indexes as owned `Relation<'mcx>`s (the
        // leader transiently reopens each index for its AM-options read); the 6
        // seams (table_open_lock, table_close_lock, am_parallel_vacuum_options,
        // am_use_maintenance_work_mem, relation_get_number_of_blocks_pv,
        // relation_get_namespace_name_pv) carry the real value types
        // (Relation<'mcx> / &Relation<'mcx>) and are installed by
        // vacuumparallel::init_seams, delegating to the canonical table-AM
        // (table_open), index-AM (GetIndexAmRoutineByAmId), relcache/smgr
        // (RelationGetNumberOfBlocks) and lsyscache (get_namespace_name)
        // providers. Allowlist entries removed below.
        //
        // RESOLVED (vp-dsm lane): the VacuumSharedCostBalance/VacuumActiveNWorkers
        // DSM atomics and the per-worker DSM instrument usage slots are now
        // installed. The shared cost-state is modeled as a genuinely-shared
        // `Arc<VacuumSharedCostState>` (two `AtomicU32`s): the leader/worker
        // enable seams point vacuum.c's globals at it, both atomic-mutate the same
        // cell. The instrument slots live in vacuumparallel's DSM side-table and
        // delegate the buffer/WAL math to backend-executor-instrument.
        //
        // RESOLVED (vp-small lane): the worker buffer-access strategy
        // (get_access_strategy_with_size_basvac / free_access_strategy_pv,
        // re-signed to the real BufferAccessStrategy), the MyProc->statusFlags
        // PROC_IN_VACUUM accessor (set_proc_in_vacuum_flags / my_proc_in_vacuum_only,
        // over backend-storage-lmgr-proc), and the parallel error-context callback
        // (push/pop_parallel_vacuum_error_context, faithful no-ops since the
        // ambient error_context_stack chain is retired) are now installed.
        //
        // RESOLVED (vacuum-strategy-resign lane): the leader/serial buffer-access
        // strategy is now the real `BufferAccessStrategy` threaded through the
        // whole vacuum()/ExecVacuum/vacuum_rel/heap_vacuum_rel + analyze.c spine
        // (the `StrategyHandle(u64)` opaque carrier is retired). The leader
        // `get_access_strategy_with_size` + `get_access_strategy_buffer_count`
        // seams are INSTALLED by vacuumparallel::init_seams over
        // backend-storage-buffer-support; entry removed below.
        // DESIGN_DEBT (TD-SUBSCRIPTION-CHECKRELKIND): CheckSubscriptionRelkind
        // is declared here but actually lives in executor/execReplication.c
        // (and is called from worker.c / relation.c / subscriptioncmds.c), not
        // pg_subscription.c. Its faithful owner is the (unported) execReplication
        // unit, so the backend-catalog-pg-subscription owner installs only the
        // pg_subscription.c surface. get_subscription_list (launcher.c) IS now
        // ported + installed by backend-catalog-pg-subscription. DELETE this
        // entry when execReplication lands.
        ("backend_catalog_pg_subscription", "check_subscription_relkind"),

        // DESIGN_DEBT (TD-RIR-RULE-ENGINE): rewriteHandler.c's
        // relation_has_security_invoker is installed by the DML/RIR rule-engine
        // slice (alongside get_view_query / relation_is_updatable), which is the
        // NEXT rewriteHandler slice and is keystone-blocked on the query_rewrite
        // contract collapse (portalcmds::Query opaque token). No body yet; the
        // lockcmds caller seam-and-panics. DELETE when the RIR engine lands.
        ("backend_rewrite_rewritehandler", "relation_has_security_invoker"),

    ];

    /// CATALOG.tsv unit statuses that mean the owner crate is COMPLETE — its
    /// declared seams are an installed contract, not a mid-port frontier where
    /// `mirror-pg-and-panic` legitimately keeps them panicking.
    ///
    /// `ported` is COMPLETE: it means the owner crate's CODE EXISTS and is
    /// fully written (the port is done; only the final audit pass is pending).
    /// A `ported` owner's declared+called seams therefore have a real provider
    /// and MUST be wired (`<fn>::set(...)`) or explicitly tracked in
    /// `CONTRACT_RECONCILE_PENDING` — leaving one declared+called+uninstalled is
    /// a latent runtime panic the moment a consumer reaches it. Treating
    /// `ported` as exempt was a BLIND SPOT (86 crates) that let unwired seams
    /// slip past the static guard until a runtime path hit them.
    ///
    /// The genuinely-unfinished frontier statuses (`todo`, `in-progress`,
    /// `needs-decomp`, `partial`, `scaffold`) STAY EXEMPT: those owners
    /// legitimately mirror-pg-and-panic across their still-incomplete surface,
    /// so flagging them would perma-red the live port frontier.
    fn is_complete_status(status: &str) -> bool {
        status == "merged" || status == "audited" || status == "ported"
    }

    /// Map every crate-dir name listed in `CATALOG.tsv`'s `crate` column to the
    /// set of crate dirs whose owning unit is COMPLETE (merged/audited). The
    /// `crate` column may list several crates (`A + B`, `A, B`) for one unit.
    fn complete_crate_dirs(crates: &Path) -> std::collections::HashSet<String> {
        let mut complete = std::collections::HashSet::new();
        let catalog_path = crates
            .parent()
            .expect("repo root")
            .join("CATALOG.tsv");
        let text = fs::read_to_string(&catalog_path).expect("read CATALOG.tsv");
        for (i, line) in text.lines().enumerate() {
            if i == 0 {
                continue; // header
            }
            let cols: Vec<&str> = line.split('\t').collect();
            // A row may have only 3 columns (no trailing `crate`/`notes` tabs at
            // all). Earlier this `< 4` skip silently EXEMPTED such complete
            // owners (e.g. backend-utils-cache-inval) — same blind spot as the
            // empty col-4 below. Only the unit (col-1) + status (col-3) are
            // required; treat a missing `crate` column as empty.
            if cols.len() < 3 {
                continue;
            }
            let status = cols[2].trim();
            let crate_col = cols.get(3).copied().unwrap_or("");
            if !is_complete_status(status) {
                continue;
            }
            // BLIND-SPOT FIX (empty `crate` column): many merged/audited rows
            // leave col-4 (`crate`) blank because the crate dir name equals the
            // unit name (col-1). Skipping those rows silently EXEMPTED their
            // owners from the install guard — a complete owner's declared-but-
            // unset, actually-called seams would panic at runtime undetected.
            // Fall back to the unit name (col-1) so the owner enters `complete`.
            let mut any = false;
            for c in crate_col.split(|ch| ch == '+' || ch == ',') {
                let c = c.trim();
                if !c.is_empty() {
                    complete.insert(c.to_string());
                    any = true;
                }
            }
            if !any {
                let unit = cols[0].trim();
                if !unit.is_empty() {
                    complete.insert(unit.to_string());
                }
            }
        }
        complete
    }

    /// Remove `#[cfg(test)]`-gated item bodies from a source string so seam
    /// `::call()` sites inside tests don't count as "used in non-test code".
    fn strip_cfg_test(src: &str) -> String {
        let mut out = String::new();
        let bytes = src.as_bytes();
        let needle = b"#[cfg(test)]";
        let mut i = 0;
        while i < src.len() {
            if i + needle.len() <= bytes.len() && &bytes[i..i + needle.len()] == needle {
                // Scan forward to whichever comes first: a ';' (a block-less
                // item DECL such as `#[cfg(test)] mod tests;`, which has no body
                // to strip) or a '{' (an inline item with a balanced body to
                // drop). Stopping at ';' first is essential: a bare module decl
                // is terminated by ';' BEFORE the next item's '{', so naively
                // scanning to the first '{' would erase the body of the NEXT
                // item (e.g. `pub fn init_seams() {...}`) instead.
                let mut j = i + needle.len();
                while j < bytes.len()
                    && bytes[j] as char != '{'
                    && bytes[j] as char != ';'
                {
                    j += 1;
                }
                if j >= bytes.len() {
                    break;
                }
                if bytes[j] as char == ';' {
                    // Block-less decl: drop only the attribute + decl up to and
                    // including the ';'. Nothing else to strip.
                    i = j + 1;
                    continue;
                }
                let mut depth = 0i32;
                while j < bytes.len() {
                    match bytes[j] as char {
                        '{' => depth += 1,
                        '}' => {
                            depth -= 1;
                            if depth == 0 {
                                j += 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                    j += 1;
                }
                i = j;
            } else {
                out.push(bytes[i] as char);
                i += 1;
            }
        }
        out
    }

    /// Collect every `<seams_crate_lib>::<seam_fn>::call(` site that appears in
    /// NON-test code anywhere under `crates/`. An installed-but-never-called
    /// seam on a complete owner is at worst a lint, not a runtime panic — so
    /// the regression guard only fires for seams that are actually invoked.
    fn called_seams(crates: &Path) -> std::collections::HashSet<(String, String)> {
        let mut called = std::collections::HashSet::new();
        for entry in fs::read_dir(crates).expect("read crates dir").flatten() {
            let src = entry.path().join("src");
            if !src.is_dir() {
                continue;
            }
            let mut files = Vec::new();
            rs_files(&src, &mut files);
            for f in &files {
                if is_test_file(f) {
                    continue; // whole-file test modules: stub `::set`/`::call` only
                }
                let raw = fs::read_to_string(f).unwrap_or_default();
                // BLIND-SPOT FIX (aliased call sites): resolve `x::foo::call()`
                // where `use backend_x_seams as x;` was declared, so the call is
                // attributed to the real seam crate, not the alias.
                let aliases = alias_map(&raw);
                let txt = strip_cfg_test(&raw);
                collect_call_sites(&txt, &aliases, &mut called);
            }
        }
        called
    }

    /// Workspace-wide `(seams_lib, fn)` set of every seam installed via
    /// `<seams_lib>::<fn>::set(` in non-test code, anywhere under `crates/`.
    /// A seam's INSTALLER need not be the crate whose NAME matches the seam
    /// crate: the C function's real owner may be a different crate (e.g.
    /// `add_function_cost` is declared in `costsize-seams` but its real owner
    /// is plancat.c, so `plancat` installs it). A cross-crate `::set` is a
    /// legitimate install, so the by-name owner check must not flag it.
    fn installed_seams(crates: &Path) -> std::collections::HashSet<(String, String)> {
        let mut installed = std::collections::HashSet::new();
        for entry in fs::read_dir(crates).expect("read crates dir").flatten() {
            let src = entry.path().join("src");
            if !src.is_dir() {
                continue;
            }
            let mut files = Vec::new();
            rs_files(&src, &mut files);
            for f in &files {
                if is_test_file(f) {
                    continue; // test stubs `::set` deps; not a real install
                }
                let raw = fs::read_to_string(f).unwrap_or_default();
                let aliases = alias_map(&raw);
                let txt = strip_cfg_test(&raw);
                collect_sites(&txt, b"::set", &aliases, &mut installed);
            }
        }
        installed
    }

    /// Parse `ident::ident::call(` triples out of one source string, resolving
    /// the leading crate ident through `aliases` (`use ... as <alias>;`).
    fn collect_call_sites(
        src: &str,
        aliases: &std::collections::HashMap<String, String>,
        out: &mut std::collections::HashSet<(String, String)>,
    ) {
        collect_sites(src, b"::call", aliases, out);
    }

    /// Parse `ident::ident<needle>(` triples (e.g. `::call` / `::set`) out of
    /// one source string, resolving the leading crate ident through `aliases`.
    fn collect_sites(
        src: &str,
        needle: &[u8],
        aliases: &std::collections::HashMap<String, String>,
        out: &mut std::collections::HashSet<(String, String)>,
    ) {
        let bytes = src.as_bytes();
        let mut i = 0;
        while i + needle.len() <= bytes.len() {
            if &bytes[i..i + needle.len()] != needle {
                i += 1;
                continue;
            }
            // require `(` (allowing whitespace) right after `::call`
            let mut k = i + needle.len();
            while k < bytes.len() && (bytes[k] as char).is_whitespace() {
                k += 1;
            }
            if k >= bytes.len() || bytes[k] as char != '(' {
                i += needle.len();
                continue;
            }
            // walk backwards over `seam_fn` ident, then `::`, then `seams_lib` ident
            let is_ident = |c: u8| (c as char).is_alphanumeric() || c == b'_';
            let mut s = i;
            while s > 0 && is_ident(bytes[s - 1]) {
                s -= 1;
            }
            let fn_name = &src[s..i];
            if fn_name.is_empty() || s < 2 || &src[s - 2..s] != "::" {
                i += needle.len();
                continue;
            }
            let mut t = s - 2;
            while t > 0 && is_ident(bytes[t - 1]) {
                t -= 1;
            }
            let lib = &src[t..s - 2];
            if !lib.is_empty() && !fn_name.is_empty() {
                // Resolve an alias (`use backend_x_seams as lib;`) to the real
                // seam-crate ident; non-aliased idents pass through unchanged.
                let resolved = aliases.get(lib).map(|s| s.as_str()).unwrap_or(lib);
                out.insert((resolved.to_string(), fn_name.to_string()));
            }
            i += needle.len();
        }
    }

    /// True-regression guard: a declared seam on a COMPLETE owner that is
    /// actually `::call`ed but not installed would panic at runtime on a real
    /// call path. Scoped (NOT weakened) so it fires only for those — the
    /// mid-port frontier (`todo`/scaffold owners) legitimately seam-and-panic
    /// (`mirror-pg-and-panic`), and known contract divergences are tracked in
    /// `CONTRACT_RECONCILE_PENDING` + DESIGN_DEBT rather than force-wired.
    ///
    /// Flags a `(owner X, seam fn)` ONLY when ALL hold:
    ///   (a) owner crate `X` exists for `crates/X-seams`,
    ///   (b) `X`'s CATALOG.tsv unit status is `merged` or `audited`,
    ///   (c) the seam is `::call`ed somewhere in non-test code,
    ///   (d) it is NOT in `CONTRACT_RECONCILE_PENDING`.
    ///
    /// This is the dual of `every_seam_installing_crate_is_wired_into_init_all`.
    #[test]
    fn every_declared_seam_is_installed_by_its_owner() {
        let crates = crates_dir();
        let complete = complete_crate_dirs(&crates);
        let called = called_seams(&crates);
        // Every seam installed via `::set(` ANYWHERE in non-test code, keyed by
        // (seams_lib, fn). A seam's real installer may be a crate whose name
        // does not match the seam crate (the C function's true owner) — that is
        // still a valid install, so it must clear the by-name owner check.
        let installed = installed_seams(&crates);
        let allowed: std::collections::HashSet<(&str, &str)> =
            CONTRACT_RECONCILE_PENDING.iter().copied().collect();

        // Offenders: (owner_lib, missing_seam_fn).
        let mut missing: Vec<(String, String)> = Vec::new();
        // Allowlist entries we expected to fire but didn't (now installed or
        // gone) — stale debt we want flagged so the list stays honest.
        let mut stale_allow: Vec<(String, String)> = Vec::new();
        let mut live_allow: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();

        for entry in fs::read_dir(&crates).expect("read crates dir").flatten() {
            let cpath = entry.path();
            if !cpath.is_dir() {
                continue;
            }
            let dir_name = match cpath.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            if !dir_name.ends_with("-seams") {
                continue; // not a seams crate
            }

            // (a) OWNER must exist under crates/ — else genuinely unported.
            // BLIND-SPOT FIX (infix-tag seam dirs): resolve `<owner>-pc-seams`
            // etc. to the real owner dir, not the nonexistent `<owner>-pc`.
            let owner_dir_name = match seam_owner_dir(&crates, &dir_name) {
                Some(o) => o,
                None => continue,
            };
            let owner_path = crates.join(&owner_dir_name);

            // (b) OWNER unit must be COMPLETE (merged/audited) in CATALOG.tsv.
            // A `todo`/scaffold/in-progress owner legitimately seam-and-panics
            // on its still-unfinished surface (mirror-pg-and-panic), so it is
            // EXEMPT — flagging it would perma-red the live port frontier.
            if !complete.contains(&owner_dir_name) {
                continue;
            }

            // Collect every seam fn the seams crate declares.
            let mut seam_files = Vec::new();
            rs_files(&cpath.join("src"), &mut seam_files);
            let mut declared: Vec<String> = Vec::new();
            for f in &seam_files {
                let txt = fs::read_to_string(f).unwrap_or_default();
                declared.extend(declared_seam_fns(&txt));
            }
            if declared.is_empty() {
                continue;
            }

            // The owner must `<fn>::set(...)` every declared seam. The install
            // may live directly in `init_seams()` or in a helper it delegates
            // to (e.g. a `wire::install_*_seams()` fn in another module), so we
            // scan the owner's ENTIRE src.
            let mut owner_files = Vec::new();
            rs_files(&owner_path.join("src"), &mut owner_files);
            let mut owner_src = String::new();
            let mut has_init_seams = false;
            // Seams the OWNER itself `::call`s (alias-resolved). A seam crate
            // `<X>-seams` bundles BOTH X's INWARD seams (X installs, others call)
            // AND X's OUTWARD seams (X calls, the dependency owner installs).
            // An outward seam is legitimately uninstalled until its *real* owner
            // (often still unported) lands — that's mirror-pg-and-panic, NOT a
            // regression — so it must NOT be attributed to X. The discriminator:
            // X calls an OUTWARD seam; X never calls its own INWARD seams.
            let mut owner_calls: std::collections::HashSet<(String, String)> =
                std::collections::HashSet::new();
            for f in &owner_files {
                if is_test_file(f) {
                    continue; // test stubs `::set()` deps; not a real install
                }
                let raw = fs::read_to_string(f).unwrap_or_default();
                let aliases = alias_map(&raw);
                let txt = strip_cfg_test(&raw);
                if init_seams_body(&txt).is_some() {
                    has_init_seams = true;
                }
                collect_call_sites(&txt, &aliases, &mut owner_calls);
                owner_src.push('\n');
                owner_src.push_str(&txt);
            }

            let owner_lib = owner_dir_name.replace('-', "_");
            let seams_lib = dir_name.replace('-', "_");

            for fname in &declared {
                let pat1 = format!("{}::set(", fname);
                let pat2 = format!("{}::set (", fname);
                let installed_by_name = has_init_seams
                    && (owner_src.contains(&pat1) || owner_src.contains(&pat2));
                if installed_by_name {
                    continue;
                }

                // Cross-crate install: the seam's real owner (the C function's
                // true home) may be a DIFFERENT crate than the name-matched one
                // — e.g. `add_function_cost` is declared in `costsize-seams`
                // but defined in plancat.c, so `plancat` installs it. A `::set`
                // of this seam anywhere in non-test code is a valid install.
                if installed.contains(&(seams_lib.clone(), fname.clone())) {
                    continue;
                }

                // OUTWARD-seam exclusion: if the dir-owner itself calls this
                // seam, it is an outward dependency seam (real owner elsewhere,
                // often unported) — not the dir-owner's inward contract. Skip.
                if owner_calls.contains(&(seams_lib.clone(), fname.clone())) {
                    continue;
                }

                // (c) Only a seam that is actually `::call`ed in non-test code
                // can panic at runtime; an unused declared seam is a lint.
                if !called.contains(&(seams_lib.clone(), fname.clone())) {
                    continue;
                }

                // (d) Known contract-divergence / mis-homed seams are tracked
                // debt (DESIGN_DEBT), not a regression — but we still verify
                // the allowlist is LIVE (a still-uninstalled, still-called
                // divergence) so retired entries get surfaced as stale.
                if allowed.contains(&(owner_lib.as_str(), fname.as_str())) {
                    live_allow.insert((owner_lib.clone(), fname.clone()));
                    continue;
                }

                missing.push((owner_lib.clone(), fname.clone()));
            }
        }

        for (owner, f) in CONTRACT_RECONCILE_PENDING {
            if !live_allow.contains(&((*owner).to_string(), (*f).to_string())) {
                stale_allow.push(((*owner).to_string(), (*f).to_string()));
            }
        }

        if !missing.is_empty() || !stale_allow.is_empty() {
            missing.sort();
            stale_allow.sort();
            let mut msg = String::new();
            if !missing.is_empty() {
                let detail: String = missing
                    .iter()
                    .map(|(owner, f)| format!("\n  {}: {}::set(...) missing", owner, f))
                    .collect();
                msg.push_str(&format!(
                    "seam-install REGRESSION: {} declared seam(s) on a COMPLETE \
                     (merged/audited) owner are `::call`ed in non-test code but \
                     NOT installed via `<fn>::set(...)` — would panic at runtime \
                     on a real call path. Either install the seam in the owner's \
                     init_seams() (pure wiring) or, if the contract diverges, add \
                     a DESIGN_DEBT entry + a CONTRACT_RECONCILE_PENDING allowlist \
                     line:{}",
                    missing.len(),
                    detail
                ));
            }
            if !stale_allow.is_empty() {
                let detail: String = stale_allow
                    .iter()
                    .map(|(owner, f)| format!("\n  {}: {}", owner, f))
                    .collect();
                if !msg.is_empty() {
                    msg.push_str("\n\n");
                }
                msg.push_str(&format!(
                    "stale CONTRACT_RECONCILE_PENDING: {} allowlist entry(ies) no \
                     longer fire (the seam is now installed, no longer called, or \
                     the owner regressed out of complete status). Reconcile and \
                     DELETE the entry (and its DESIGN_DEBT line):{}",
                    stale_allow.len(),
                    detail
                ));
            }
            panic!("{}", msg);
        }
    }
}



#[cfg(test)]
mod builtin_registry_completeness {
    //! Completeness guard for the fmgr built-in REGISTRY — the runtime analog of
    //! C's compile-time-complete `fmgr_builtins[]`.
    //!
    //! C's `Gen_fmgrtab.pl` emits EVERY built-in from `pg_proc.dat` into one
    //! static array, so `fmgr_isbuiltin(oid)` can never silently miss a real
    //! built-in. The port assembles the equivalent set at runtime from per-crate
    //! `register_builtins` calls in each `init_seams()`. If a crate is unported or
    //! its `init_seams()` is unwired, its built-ins are silently absent and
    //! `fmgr_isbuiltin` misses — for a catalog-scan comparator that recurses to a
    //! boot-time stack overflow. This guard makes that gap a loud `cargo test`
    //! failure instead.
    //!
    //! `init_all()` populates the (per-backend `thread_local`) registry; we then
    //! compare the live gap against `crate::builtin_gap_baseline::KNOWN_GAP`. The
    //! canonical set is `backend_utils_fmgr_core::builtin_canonical::CANONICAL`,
    //! derived from the SAME `pg_proc.dat` C uses.
    //!
    //! ## Semantics: the baseline is an UPPER BOUND, not an exact set
    //!
    //! The guard is **subset-tolerant**: the live gap must be a SUBSET of the
    //! baseline gap. It FAILS only on a REGRESSION — a builtin that is missing
    //! (or metadata-mismatched in a way the baseline did not record) now but is
    //! NOT accepted by the baseline (a previously-registered builtin went
    //! missing, or a newly-surfaced gap).
    //!
    //! It deliberately does NOT fail when the live gap is SMALLER than the
    //! baseline. Registering more builtins is always progress, and a workflow
    //! may register builtins across many crates independently; none of those
    //! crates should have to edit the shared baseline just because the gap
    //! shrank. "Stale" baseline entries (now-registered builtins still listed)
    //! are therefore tolerated here — the baseline is the maximum set of gaps we
    //! still accept, and is re-tightened toward empty separately, out of band.

    use backend_utils_fmgr_core::{missing_builtins, BuiltinGap, BuiltinGapKind};
    use std::collections::BTreeMap;

    fn kind_str(k: &BuiltinGapKind) -> String {
        match k {
            BuiltinGapKind::NotRegistered => "not-registered".to_string(),
            BuiltinGapKind::Mismatch { field } => format!("mismatch:{}", field),
        }
    }

    #[test]
    fn builtin_registry_matches_canonical_or_baseline() {
        super::init_all();

        // Live gap: (foid) -> (name, kind), keyed for set comparison.
        let live: BTreeMap<u32, BuiltinGap> = missing_builtins()
            .into_iter()
            .map(|g| (g.foid, g))
            .collect();
        // Accepted baseline gap, same keying.
        let baseline: BTreeMap<u32, (&'static str, &BuiltinGapKind)> =
            crate::builtin_gap_baseline::KNOWN_GAP
                .iter()
                .map(|(oid, name, kind)| (*oid, (*name, kind)))
                .collect();

        // REGRESSIONS: a live gap not accepted by the baseline, OR accepted but
        // with a different failure kind (e.g. was a metadata mismatch, now fully
        // unregistered). Naming the OID + function is the loud-failure contract.
        let mut regressions: Vec<String> = Vec::new();
        for (oid, g) in &live {
            match baseline.get(oid) {
                None => regressions.push(format!(
                    "\n  builtin {} {} {} (NOT in baseline — an unported/unwired \
                     builtin crate, or a previously-registered builtin went missing)",
                    oid,
                    g.name,
                    kind_str(&g.kind)
                )),
                Some((_, bkind)) if kind_str(&g.kind) != kind_str(bkind) => regressions
                    .push(format!(
                        "\n  builtin {} {} now {} (baseline recorded {})",
                        oid,
                        g.name,
                        kind_str(&g.kind),
                        kind_str(bkind)
                    )),
                Some(_) => {}
            }
        }

        // SUBSET-TOLERANT: a live gap SMALLER than the baseline is always OK —
        // it means more builtins got registered, which is progress and must not
        // fail this guard (nor force any crate to edit the shared baseline).
        // Baseline entries that are no longer live gaps are tolerated here and
        // re-tightened separately, out of band. We therefore only fail on the
        // REGRESSIONS collected above (live gaps NOT covered by the baseline).
        if regressions.is_empty() {
            return;
        }

        let total = regressions.len();
        let shown: String = regressions.iter().take(40).cloned().collect();
        let more = if total > 40 {
            format!("\n  ... and {} more", total - 40)
        } else {
            String::new()
        };
        panic!(
            "fmgr built-in registry REGRESSED: {} canonical built-in(s) newly \
             missing/mismatched beyond the accepted baseline (an unported or \
             unwired adt crate — C's fmgr_builtins[] can never miss, the \
             per-crate registry must not either). Wire/register the owner, or \
             if intentionally still-unported add the OID to \
             seams-init/src/builtin_gap_baseline.rs:{}{}",
            total, shown, more
        );
    }

    /// Independent of the live registry: the baseline must only ever cite OIDs
    /// that are actually canonical built-ins (guards against a typo'd or
    /// stale-after-PG-bump OID lingering in the baseline). Names must match the
    /// canonical `prosrc` too.
    #[test]
    fn baseline_only_cites_canonical_builtins() {
        use backend_utils_fmgr_core::builtin_canonical::CANONICAL;
        let canon: BTreeMap<u32, &'static str> =
            CANONICAL.iter().map(|(oid, name, ..)| (*oid, *name)).collect();
        let mut bad: Vec<String> = Vec::new();
        for (oid, name, _) in crate::builtin_gap_baseline::KNOWN_GAP {
            match canon.get(oid) {
                None => bad.push(format!("\n  {} {} (not a canonical builtin OID)", oid, name)),
                Some(cn) if cn != name => {
                    bad.push(format!("\n  {} baseline name {:?} != canonical {:?}", oid, name, cn))
                }
                Some(_) => {}
            }
        }
        assert!(
            bad.is_empty(),
            "builtin_gap_baseline.rs cites non-canonical OID(s)/name(s) \
             (regenerate after a PostgreSQL bump):{}",
            bad.concat()
        );
    }
}
