//! Startup aggregator: calls every ported crate's `init_seams()`.
//!
//! This crate contains NO logic and NO `set()` calls of its own — one line
//! per ported crate, nothing else. Each crate wires its own seams in its own
//! `init_seams()`; this is just the place that invokes them all.

pub fn init_all() {
    // One line per ported crate, kept sorted:
    contrib_amcheck_verify_nbtree::init_seams();
    contrib_amcheck_verify_common::init_seams();
    backend_archive_shell_archive::init_seams();
    backend_commands_async::init_seams();
    backend_access_common_detoast::init_seams();
    backend_access_common_heaptuple::init_seams();
    backend_access_common_indextuple::init_seams();
    backend_access_common_next::init_seams();
    backend_access_common_printtup::init_seams();
    backend_access_common_relation::init_seams();
    backend_access_common_reloptions::init_seams();
    backend_access_common_tidstore::init_seams();
    backend_access_common_tupdesc::init_seams();
    backend_access_gin_core_probe::init_seams();
    backend_access_gin_ginfast::init_seams();
    backend_access_gin_ginget::init_seams();
    backend_access_gin_gininsert::init_seams();
    backend_access_gin_ginscan::init_seams();
    backend_access_gin_ginvacuum::init_seams();
    backend_access_gin_ginxlog::init_seams();
    backend_access_hashvalidate::init_seams();
    backend_access_heap_heapam::init_seams();
    backend_access_heap_heapam_handler_core::init_seams();
    backend_access_heap_heapam_handler_dml::init_seams();
    backend_access_heap_heapam_visibility::init_seams();
    backend_access_heap_heapam_xlog::init_seams();
    backend_access_heap_heaptoast::init_seams();
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
    backend_access_nbt_dedup::init_seams();
    backend_access_nbt_xlog::init_seams();
    backend_access_nbtree_nbtree::init_seams();
    backend_access_rmgrdesc_replorigindesc::init_seams();
    backend_access_rmgrdesc_small::init_seams();
    backend_access_rmgrdesc_smgrdesc::init_seams();
    backend_access_rmgrdesc_xactdesc::init_seams();
    backend_access_rmgrdesc_xlogdesc::init_seams();
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
    backend_catalog_pg_proc::init_seams();
    backend_catalog_pg_publication::init_seams();
    backend_catalog_pg_subscription::init_seams();
    backend_commands_publicationcmds::init_seams();
    backend_catalog_pg_shdepend::init_seams();
    backend_catalog_toasting::init_seams();
    backend_commands_amcmds::init_seams();
    backend_commands_analyze::init_seams();
    backend_commands_cluster::init_seams();
    backend_commands_tablecmds::init_seams();
    backend_commands_vacuum::init_seams();
    backend_commands_vacuumparallel::init_seams();
    backend_commands_variable::init_seams();
    backend_commands_comment::init_seams();
    backend_commands_indexcmds::init_seams();
    backend_commands_proclang::init_seams();
    backend_commands_dbcommands::init_seams();
    backend_commands_conversioncmds::init_seams();
    backend_commands_statscmds::init_seams();
    backend_commands_copyto::init_seams();
    backend_commands_createas::init_seams();
    backend_commands_view::init_seams();
    backend_commands_define::init_seams();
    backend_commands_alter::init_seams();
    backend_commands_policy::init_seams();
    backend_commands_dropcmds::init_seams();
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
    backend_executor_execTuples::init_seams();
    backend_executor_execUtils::init_seams();
    backend_executor_execGrouping::init_seams();
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
    backend_main_main::init_seams();
    backend_libpq_auth::init_seams();
    backend_libpq_be_fsstubs::init_seams();
    backend_libpq_hba::init_seams();
    backend_libpq_be_gssapi_common::init_seams();
    backend_libpq_be_secure_common::init_seams();
    backend_libpq_be_secure::init_seams();
    backend_libpq_be_secure_openssl::init_seams();
    backend_libpq_auth_scram::init_seams();
    backend_libpq_crypt::init_seams();
    backend_libpq_pqcomm::init_seams();
    backend_libpq_pqformat::init_seams();
    backend_libpq_pqsignal::init_seams();
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
    backend_optimizer_util_plancat::init_seams();
    backend_optimizer_path_pathkeys::init_seams();
    backend_access_nbt_compare::init_seams();
    backend_access_nbt_validate::init_seams();
    backend_access_nbtree_core::init_seams();
    backend_access_nbtree_nbtsort::init_seams();
    backend_common_relpath::init_seams();
    backend_optimizer_path_costsize::init_seams();
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
    backend_replication_walreceiver::init_seams();
    backend_replication_walreceiverfuncs::init_seams();
    backend_replication_walsender::init_seams();
    backend_rmgrdesc_next::init_seams();
    backend_rewrite_core::init_seams();
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
    backend_tcop_utility::init_seams();
    backend_timezone_localtime::init_seams();
    backend_timezone_pgtz::init_seams();
    backend_snowball_dict_snowball::init_seams();
    backend_timezone_strftime::init_seams();
    backend_tsearch_ispell_regis::init_seams();
    backend_tsearch_spell::init_seams();
    backend_utils_activity_small::init_seams();
    backend_utils_activity_waitevent::init_seams();
    backend_utils_activity_xact::init_seams();
    backend_utils_adt_misc2::init_seams();
    backend_catalog_aclchk::init_seams();
    backend_utils_adt_acl::init_seams();
    backend_utils_adt_datetime::init_seams();
    backend_utils_adt_array_selfuncs::init_seams();
    backend_utils_adt_array_typanalyze::init_seams();
    backend_utils_adt_arrayfuncs::init_seams();
    backend_utils_adt_arrayutils::init_seams();
    backend_utils_adt_char::init_seams();
    backend_utils_adt_float::init_seams();
    backend_utils_adt_format_type::init_seams();
    backend_utils_adt_ruleutils::init_seams();
    backend_utils_adt_xml::init_seams();
    backend_utils_adt_geo_ops::init_seams();
    backend_utils_adt_formatting::init_seams();
    backend_utils_adt_json::init_seams();
    backend_utils_adt_jsonb_gin::init_seams();
    backend_utils_adt_jsonfuncs::init_seams();
    backend_utils_adt_jsonpath_gram::init_seams();
    backend_utils_adt_like::init_seams();
    backend_utils_adt_encode::init_seams();
    backend_utils_adt_multirangetypes::init_seams();
    backend_utils_adt_network_gist::init_seams();
    backend_utils_adt_network_selfuncs::init_seams();
    backend_utils_adt_numeric::init_seams();
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
    backend_utils_adt_scalar_datum_core::init_seams();
    backend_utils_adt_tsvector_core::init_seams();
    backend_utils_adt_varlena::init_seams();
    backend_utils_adt_version::init_seams();
    backend_utils_adt_ri_triggers::init_seams();
    backend_utils_cache_attoptcache::init_seams();
    backend_utils_cache_catcache::init_seams();
    backend_utils_cache_evtcache::init_seams();
    backend_utils_cache_inval::init_seams();
    backend_utils_cache_lsyscache::init_seams();
    backend_utils_cache_partcache::init_seams();
    backend_utils_cache_plancache::init_seams();
    backend_utils_cache_relcache::init_seams();
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
    backend_utils_misc_sampling::init_seams();
    backend_utils_misc_stack_depth::init_seams();
    backend_utils_misc_timeout::init_seams();
    backend_utils_mmgr_dsa::init_seams();
    backend_utils_mmgr_freepage::init_seams();
    backend_utils_mmgr_portalmem::init_seams();
    backend_utils_sort_small::init_seams();
    backend_utils_sort_sortsupport::init_seams();
    backend_utils_sort_storage::init_seams();
    backend_utils_sort_tuplesort::init_seams();
    backend_utils_time_combocid::init_seams();
    backend_utils_time_snapmgr::init_seams();
    common_blkreftable::init_seams();
    common_checksum_helper::init_seams();
    common_hashfn::init_seams();
    common_ip::init_seams();
    common_pglz::init_seams();
    common_prng_base64::init_seams();
    common_scram_common::init_seams();
    common_string::init_seams();
    interfaces_libpq_legacy_pqsignal::init_seams();
    port_crc32c::init_seams();
    port_pgsleep::init_seams();
    port_pqsignal::init_seams();
    probe_adt_scalar_bool::init_seams();
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
        // DESIGN_DEBT (TD-DEPENDENCY-REMOVEFUNC): dependency.c's `doDeletion` calls
        // `remove_function_tuple` (the pg_proc `RemoveFunctionById` catalog delete)
        // for OCLASS_PROC. functioncmds.c is a CONSUMER of this seam (it also calls
        // it at ddl_core.rs:1163); the real owner is pg_proc.c's RemoveFunctionById,
        // which is unported, so nobody installs it. Loud-panics until pg_proc lands.
        ("backend_commands_functioncmds", "remove_function_tuple"),
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
        // DESIGN_DEBT (TD-SRF-INLINE-QUERY): `inline_set_returning_function` is the
        // full SET-returning-function inliner (clauses.c:5134) that
        // `preprocess_function_rtes` (prepjointree.c:931) calls to turn a FUNCTION
        // RTE into a subquery RTE. The owner is clauses.c, but the inline leg
        // (LANGUAGE SQL prosrc parse + rewrite + single-SELECT querytree
        // validation, returning an owned `Query`) is gated on the SQL-function
        // parse/rewrite path, which is unported — the same gap as the sibling
        // `inline_set_returning_function_core` (the scalar-SQL inline leg, also
        // uninstalled). The FuncExpr node universe + SQL-function querytree are not
        // reachable as a walkable `Query` here, so the seam loud-panics (a wrong-
        // plan-class change, never a silent skip) until the inliner leg lands.
        // DELETE this entry when clauses.c's SRF-inliner is ported.
        ("backend_optimizer_util_clauses", "inline_set_returning_function"),
        // DESIGN_DEBT (TD-INITSPLAN-REBUILD-JOINCLAUSE): analyzejoins.c's
        // `remove_leftjoinrel_from_query` (left-join removal) calls
        // `rebuild_joinclause_attr_needed` (initsplan.c:3559) to re-add the
        // attr_needed bits contributed by join clauses after a join removal. The
        // owner is initsplan.c (`backend-optimizer-plan-init-subselect`), where
        // this function is NOT yet ported (the sibling `rebuild_lateral_attr_needed`
        // IS ported and installed). The seam is declared in
        // `backend-optimizer-plan-small-seams` and loud-panics until initsplan
        // lands the body. DELETE this entry when initsplan ports
        // `rebuild_joinclause_attr_needed`.
        (
            "backend_optimizer_plan_small",
            "rebuild_joinclause_attr_needed",
        ),
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
        // DESIGN_DEBT (TD-HEAPAM-UNPORTED-DRIVERS): six heapam-seams whose real
        // bodies are NOT in the merged heap-AM slice yet — sanctioned
        // mirror-pg-and-panic on a complete owner. Each is `::call`ed in a live
        // consumer but the owner has no contract-matching body:
        //   * insert_one_tuple — bootstrap.c `InsertOneTuple` (form tuple from
        //     attrtypes/values/nulls + simple_heap_insert). The owner's
        //     init_seams() explicitly documents this as out-of-slice-scope
        //     (heap-INSERT family's job); only `simple_heap_insert` exists.
        //   * read_pg_type — bootstrap.c pg_type catalog-scan driver
        //     (populate_typ_list); scan substrate exists, the driver is unwritten.
        //   * scan_indisclustered — cluster.c `get_tables_to_cluster` pg_index
        //     `indisclustered` systable scan; driver unwritten.
        //   * log_heap_visible — XLOG_HEAP2_VISIBLE WAL emission; owner has
        //     log_heap_new_cid/log_heap_update but not this one.
        //   * index_compute_xid_horizon_for_tuples — the full index-buffer
        //     line-pointer + heap-page conflict-horizon driver; only the per-tuple
        //     helper HeapTupleHeaderAdvanceConflictHorizon is ported.
        // (heap_multi_insert — heapam.c's page-at-a-time batch heap insert — is
        //  now ported in backend-access-heap-heapam (insert::heap_multi_insert)
        //  and installed from its init_seams(); its allowlist entry was removed.)
        // DELETE each entry when its driver lands in the heap-AM port.
        ("backend_access_heap_heapam", "index_compute_xid_horizon_for_tuples"),
        ("backend_access_heap_heapam", "insert_one_tuple"),
        ("backend_access_heap_heapam", "log_heap_visible"),
        ("backend_access_heap_heapam", "read_pg_type"),
        ("backend_access_heap_heapam", "scan_indisclustered"),
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
        // DESIGN_DEBT (TD-TUPDESC-HANDLE): the plancache-facing tupdesc seams
        // (`-pc-seams`: create_tuple_desc_copy / free_tuple_desc /
        // equal_row_types) are HANDLE-based (`TupleDescHandle`, an opaque `u64`
        // with no backing registry), while the owner's real bodies
        // (CreateTupleDescCopy / FreeTupleDesc / equalRowTypes) and its installed
        // value-seams (`-seams`) are VALUE-based (`&TupleDescData`). Installing
        // the handle seams needs a TupleDescHandle->TupleDescData registry
        // (substantial unported machinery / a forbidden token-registry hack) or
        // migrating plancache's whole result-desc path off opaque handles onto
        // value descriptors (a contract redesign rippling through
        // pquery/utility/analyze seams). Only `free_tuple_desc` is flagged here;
        // the other two pc-seam names collide with the installed value-seam names
        // so the name-keyed guard sees them as satisfied (they are equally
        // uninstalled at runtime — same blocker).
        ("backend_access_common_tupdesc", "free_tuple_desc"),
        // RETIRED (task #161): `heap_tuple_header_get_datum`
        // (HeapTupleHeaderGetDatum) is now installed by heaptoast's init_seams().
        // The composite/record-Datum carrier bridge landed.
        // DESIGN_DEBT (TOWER-B): the index-AM owner (backend-access-index-amapi,
        // amapi.c) installs the 11 GetIndexAmRoutine-derived seams, but
        // `am_adjust_members` and `am_reloptions` are NOT amapi.c functions and
        // cannot be installed by it: am_adjust_members is opclasscmds.c's
        // `amroutine->amadjustmembers(...)` dispatch and am_reloptions is
        // reloptions.c's `index_reloptions` -> `amoptions(reloptions, validate)`
        // dispatch. Both reach by-name AM callbacks (`amadjustmembers` /
        // `amoptions`) that the unified `IndexAmRoutine` vtable deliberately
        // DROPPED in TOWER-A (the AM validate crate's `amadjustmembers` returns a
        // soft-error result that cannot be a raw fn-ptr; `amoptions` is reached
        // by name). am_adjust_members additionally needs a conversion between the
        // seam's `types_opclass::OpFamilyMember` and the trimmed per-AM
        // `OpFamilyMember` the bt/hash adjustmembers callbacks mutate. Installing
        // these is a by-amoid AM-callback dispatch table + carrier reconcile in
        // the right owner (opclasscmds / reloptions), not amapi.c logic.
        // (am_adjust_members is consumed only via a brace-grouped `use ...::{...}`
        // import that the recurrence guard's call-site scanner does not resolve
        // to its seam crate, so it is not seen as "called" and needs no allowlist
        // entry; it remains uninstalled for the same contract reason.)
        ("backend_access_index_amapi", "am_reloptions"),
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
        // STILL PENDING (parallel-scan DSM infrastructure unported): the two
        // remaining seams resolve pointers into the DSM-resident parallel scan
        // blob, which the parallel index-scan infrastructure (the AM-specific
        // `BTParallelScanDescData` / `ps_offset_ins` arithmetic in shared memory)
        // owns and has not landed. A serial scan never reaches them; they
        // seam-and-panic (mirror-pg-and-panic). See DESIGN_DEBT.md.
        ("backend_access_index_indexam", "bt_resolve_parallel_scan"),
        ("backend_access_index_indexam", "index_scan_resolve_shared_info"),
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
        // DESIGN_DEBT: the plancache-facing search-path matcher seams are
        // declared in backend-catalog-namespace-pc-seams with a handle/CtxId
        // contract (opaque SearchPathMatcherHandle, CtxId context) because the
        // matcher's storage lives in plancache's long-lived querytree context.
        // The namespace owner's real impls are value-shaped
        // (GetSearchPathMatcher<'mcx>(Mcx)->SearchPathMatcher<'mcx>, etc.).
        // Unifying onto the value shape requires redesigning the already
        // merged/audited plancache's CachedPlanSource storage (it stores
        // SearchPathMatcherHandle, passes CtxId, and has no access to Mcx) —
        // a contract redesign of a downstream consumer, out of scope here.
        ("backend_catalog_namespace", "copy_search_path_matcher"),
        ("backend_catalog_namespace", "get_search_path_matcher"),
        // (restrict_search_path retired: RestrictSearchPath is guc.c's function
        // (guc.c:2246), now ported + installed by the merged guc owner
        // (backend-utils-misc-guc) and its seam re-homed to
        // backend-utils-misc-guc-seams. Consumers (matview, cluster) call it
        // there.)
        ("backend_catalog_namespace", "search_path_matches_current_environment"),
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
        ("backend_executor_execTuples", "cur_tuple_getattr"),
        ("backend_executor_execTuples", "exec_force_store_heap_tuple"),
        ("backend_executor_execTuples", "exec_store_generated_columns"),
        ("backend_executor_execTuples", "replace_cur_tuple_from_slot"),
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
        ("backend_postmaster_bgworker", "background_worker_handle_from_token"),
        // DESIGN_DEBT (TD-BUFMGR-AIO-GUC): three bufmgr-seams `::call`ed in live
        // consumers (backend-storage-aio-read-stream; the bgwriter loop) whose
        // values come from the unported aio.c / GUC machinery, not this owner:
        //   * maintenance_io_concurrency — the `maintenance_io_concurrency` GUC
        //     value; no backing GUC variable exists in the owner (only a doc note).
        //   * io_method_sync — the `io_method == IOMETHOD_SYNC` test; the `io_method`
        //     GUC/enum lives in the unported aio.c, not this owner.
        //   * bgwriter_flush_after — the `bgwriter_flush_after` GUC value (a
        //     bufmgr.c `int` global). The bgwriter loop reads it via
        //     WritebackContextInit; like checkpoint_flush_after it has no backing
        //     GUC variable installed in this owner (the guc-tables boot value is
        //     seeded in the GUC store but the seam is not `::set` by the owner —
        //     the GUC machinery installs it when it fully ports). Same class as
        //     maintenance_io_concurrency / io_method_sync. (checkpoint_flush_after
        //     escapes the guard only because it is `::call`ed inside this owner
        //     crate — the OUTWARD-seam exclusion — whereas bgwriter_flush_after is
        //     called from the bgwriter consumer.)
        // (buffer_manager_shmem_size / buffer_manager_shmem_init RETIRED: the owner
        // now installs both — BufferManagerShmemSize is the faithful add_size/
        // mul_size accumulator and BufferManagerShmemInit allocate-or-attaches the
        // four named buffer-pool regions via ShmemInitStruct, then publishes the
        // process-local pool view, mirroring procarray's ProcArrayShmemInit.)
        // DELETE each entry as the aio GUC source lands.
        ("backend_storage_buffer_bufmgr", "io_method_sync"),
        ("backend_storage_buffer_bufmgr", "maintenance_io_concurrency"),
        ("backend_storage_buffer_bufmgr", "bgwriter_flush_after"),
        // (The three SetLatch-by-proc latch seams — set_latch_by_proc_number /
        // set_latch_for_proc_pid / set_latch_for_procno — are now INSTALLED.
        // The latch<->proc handle spaces were unified: a `LatchHandle` is a
        // tagged union (`LatchKind::Local` registry slot vs `LatchKind::Proc`
        // proc number), and the latch unit resolves a proc-tagged handle to the
        // real `&ProcGlobal->allProcs[procno].procLatch` through the proc unit's
        // `with_proc_latch` seam — no separate side-table for proc latches.
        // `set_latch_for_proc_pid` maps PID->ProcNumber via procarray's
        // `BackendPidGetProc`.)
        ("backend_storage_ipc_pmsignal", "set_postmaster_death_watch_cloexec"),
        // DESIGN_DEBT: `initialize_fast_path_locks` is declared + consumed but the
        // owner (backend-storage-lmgr-proc, audited) has no impl yet — it needs the
        // lock.c fast-path lock table (per-PGPROC fpLockBits/fpRelId group layout)
        // which has not landed. Pay down when lock.c fast-path locks land. See
        // DESIGN_DEBT.md. (The clog.c group XID-status update set —
        // clog_group_first_* / *_clog_group_* — was retired once clog.c
        // TransactionGroupUpdateXidStatus + procarray's InitProcGlobal arena landed;
        // those 13 seams are now installed by inward_seams over ProcGlobal->
        // clogGroupFirst + the per-PGPROC clogGroup* fields.)
        ("backend_storage_lmgr_proc", "initialize_fast_path_locks"),
        // (has_bypassrls_privilege RESOLVED: the acl owner now installs it — a
        // superuser_arg short-circuit + AUTHOID-syscache `rolbypassrls` read, after
        // widening the AuthIdRow projection to carry rolbypassrls.)
        // RESOLVED (aclchk F1): `object_ownercheck` (catalog/aclchk.c) is now
        // installed by the ported `backend-catalog-aclchk` owner crate, over the F0
        // generic `object_owner_acl` syscache projection (the `get_object_catcache_oid`
        // dispatch) plus a `table_open` + `systable_beginscan` + `heap_deform_tuple`
        // fallback for cache-less catalogs (`cacheid == -1`). Allowlist entry removed.
        // DESIGN_DEBT (#159 K1 follow-on: plancache de-handle): every consumer-
        // facing plancache seam in backend-utils-cache-plancache-seams is written
        // against a VALUE-typed contract — `mcx: Mcx<'mcx>` allocation plus owned
        // `RawStmt<'mcx>` / `Node<'mcx>` / `PlannedStmt<'mcx>` / `TupleDescData
        // <'mcx>` / `PgVec<'mcx,_>` / `PgString<'mcx>` values keyed by an opaque
        // `CachedPlanSourceHandle` / `CachedPlanHandle`. The merged/audited owner
        // is built entirely on a handle REGISTRY: its real bodies (CreateCachedPlan
        // / CompleteCachedPlan / SaveCachedPlan / DropCachedPlan / GetCachedPlan /
        // ReleaseCachedPlan / CachedPlanGetTargetList) take/return handles
        // (RawStmtHandle, QueryListHandle, CtxId, TupleDescHandle) into an internal
        // `Rc<RefCell<CachedPlanSourceData>>` map and have no `Mcx`; the
        // `plansource_*` / `cached_plan_stmt_list` field accessors have no owner fn
        // at all (the data lives behind handles, not as `'mcx` values). Installing
        // these would require either forging fake values out of stored handles (a
        // forbidden token/pointer-registry hack, opacity-inherited-never-introduced)
        // or migrating plancache's whole CachedPlanSource/CachedPlan storage off
        // opaque handles onto owned `'mcx` values — the K1 plancache de-handle
        // redesign tracked in task #159, which also retires the CtxId fields. No
        // thin adapter bridges value<->handle here. Pay down with #159, not seam
        // wiring. See DESIGN_DEBT.md.
        ("backend_utils_cache_plancache", "cached_plan_get_target_list"),
        ("backend_utils_cache_plancache", "cached_plan_stmt_list"),
        ("backend_utils_cache_plancache", "complete_cached_plan"),
        ("backend_utils_cache_plancache", "create_cached_plan"),
        ("backend_utils_cache_plancache", "drop_cached_plan"),
        ("backend_utils_cache_plancache", "get_cached_plan"),
        ("backend_utils_cache_plancache", "plansource_command_tag"),
        ("backend_utils_cache_plancache", "plansource_fixed_result"),
        ("backend_utils_cache_plancache", "plansource_num_custom_plans"),
        ("backend_utils_cache_plancache", "plansource_num_generic_plans"),
        ("backend_utils_cache_plancache", "plansource_num_params"),
        ("backend_utils_cache_plancache", "plansource_param_types"),
        ("backend_utils_cache_plancache", "plansource_query_string"),
        ("backend_utils_cache_plancache", "plansource_result_desc"),
        ("backend_utils_cache_plancache", "release_cached_plan"),
        ("backend_utils_cache_plancache", "save_cached_plan"),
        // DESIGN_DEBT: `domain_check_input` (domains.c) checks a domain's
        // constraints. Its NOT NULL arm is trivial, but the DOM_CONSTRAINT_CHECK
        // arm requires `ExecCheck(con->check_exprstate, econtext)` — executor
        // expression evaluation (`ExecEvalExprSwitchContext` over a compiled
        // `ExprState`) in a standalone `ExprContext`. In this repo `check_exprstate`
        // is an opaque `ExprStateHandle` built only via the unported domains.c
        // executor bridge, `ExecCheck` has no seam, and the standalone-ExprContext
        // machinery is absent. Blocked on the execExpr/execExprInterp executor-eval
        // keystone. (The sibling `domain_get_base_input_info` IS installed.)
        ("backend_utils_cache_typcache", "domain_check_input"),
        // DESIGN_DEBT (TD-DFMGR-DYNLOADER): the dynamic-library / extension-hook
        // surface of dfmgr.c + miscinit.c. `load_archive_module_init` is
        // `load_external_function(filename, "_PG_archive_module_init", ...)` — it
        // `dlopen`s an archive-module `.so` and resolves its init symbol; the
        // dynamic loader (`load_external_function` / `load_file`) is inherently
        // unported in an idiomatic-Rust build (no `.so` ABI surface). `shmem_request_hook`
        // / `shmem_request_hook_present` read/invoke the `shmem_request_hook`
        // function pointer that miscinit.c owns and ONLY a loaded extension sets
        // (it is NULL in core PostgreSQL) — with no extension-load machinery
        // there is no body to install and the hook is correctly absent
        // (`_present` = false). Both are genuinely owner-unported, not a contract
        // mismatch: loud-panic (mirror-pg-and-panic) until/unless a dynamic
        // extension-loading subsystem lands. DELETE if that ever ports.
        ("backend_utils_fmgr_dfmgr", "load_archive_module_init"),
        ("backend_utils_fmgr_dfmgr", "shmem_request_hook"),
        ("backend_utils_fmgr_dfmgr", "shmem_request_hook_present"),
        // DESIGN_DEBT: owner-unported (narrowed). `setup_signal_handlers` is the
        // slot-sync worker's `pqsignal(SIGHUP, SignalHandlerForConfigReload)`
        // ... block (slotsync.c:1515-1522). interrupt.c (SignalHandlerForConfigReload)
        // and procsignal.c (procsignal_sigusr1_handler) are now BOTH merged, but the
        // postgres.c handler bodies it still wires — `die` / `StatementCancelHandler`
        // / `FloatExceptionHandler` — exist only as decls in
        // backend-tcop-postgres-seams whose owner backend-tcop-postgres is CATALOG
        // `todo`, so there is still no real body to install. Becomes a real install
        // when postgres.c lands. (The other 8 slot-sync bootstrap seams declared
        // alongside it ARE installed in miscinit's init_seams by delegating to their
        // now-ported owners.)
        ("backend_utils_init_miscinit", "setup_signal_handlers"),
        // RETIRED (task #161): `record_from_values` is now installed by funcapi's
        // init_seams(). The composite/record-Datum carrier bridge landed.
        // NOTE: `value_srf_unported` is now INSTALLED by funcapi's init_seams() as
        // an EXPLICIT honest seam-and-panic (mirror-pg-and-panic) — its body lives
        // in `srf_support::value_srf_unported` and panics loudly naming the missing
        // value-per-call SRF machinery. It is therefore no longer an uninstalled
        // contract divergence and must NOT be allowlisted here (the guard would
        // flag a stale entry).
        ("backend_utils_init_small", "init_process_globals"),
        // DESIGN_DEBT (TD-PORTAL-HANDLE): PREPARE/EXECUTE's `-pre-seams` slice of
        // portalmem.c is written against the parsestmt opaque handle newtypes
        // (`PortalHandle(String)`, `MemoryContextHandle(u64)`), while the owner's
        // real bodies (CreateNewPortal / PortalDefineQuery / PortalSetVisible /
        // GetPortalContext) work on the value-typed `types_portal::Portal`
        // (`Rc<RefCell<PortalData>>`) and a real `MemoryContext`. Installing the
        // handle seams needs a PortalHandle/MemoryContextHandle -> value registry
        // (forbidden token-registry hack / opacity-introduced) or migrating
        // PREPARE/EXECUTE off the opaque parsestmt handles onto owned portal
        // values (the K1 de-handle work, #159/#169). Handle-divergent.
        ("backend_utils_mmgr_portalmem", "create_new_portal"),
        ("backend_utils_mmgr_portalmem", "portal_define_query"),
        // TD-PORTAL-HANDLE, see the handle-divergent note above.
        ("backend_utils_mmgr_portalmem", "portal_get_portal_context"),
        ("backend_utils_mmgr_portalmem", "portal_set_visible"),
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
        // DESIGN_DEBT (TD-ANALYZE-PLANCACHE-HANDLE / #159 + TD-ANALYZE-REWRITE):
        // 19 parser/analyze.c seams `::call`ed in live consumers (chiefly
        // plancache, bootstrap) on the audited analyze owner, none installable:
        //
        //   * 16 plancache-facing (-pc-seams) seams are written against opaque
        //     `types_plancache` handle newtypes (QueryHandle / RawStmtHandle /
        //     AnalyzedQueryHandle / QueryListHandle / TargetListHandle /
        //     UtilityStmtHandle / RteFields / ParserSetupHandle / QueryEnvHandle)
        //     with no producer, while the owner's real bodies operate on owned
        //     `Query<'mcx>` / `RawStmt<'mcx>` values (three even have real logic on
        //     owned refs — stmt_requires_parse_analysis(&RawStmt),
        //     analyze_requires_snapshot(&RawStmt), query_requires_rewrite_plan(&Query)
        //     — but signature-diverge from the handle decl). Installing would forge
        //     values from handles (forbidden token-registry) or migrate plancache
        //     off opaque handles — the K1 plancache de-handle redesign (#159). The
        //     owner's Cargo.toml does not even dep the -pc-seams crate. Same blocker
        //     as the plancache pc-seam cluster above.
        //   * 2 reach the unported QueryRewrite (rewriter) leg:
        //     analyze_and_rewrite_varparams (varparam analyze+rewrite absent),
        //     run_post_parse_analyze_hook (hook is NULL by default, no body).
        //     (pg_analyze_and_rewrite_fixedparams now lands via tcop/postgres.c
        //     F1, which threads the canonical owned-value rewrite leg — entry
        //     retired below.)
        // DELETE these as #159 (de-handle) and the rewriter leg land.
        ("backend_parser_analyze", "analyze_and_rewrite_fixedparams"),
        ("backend_parser_analyze", "analyze_and_rewrite_varparams"),
        ("backend_parser_analyze", "analyze_and_rewrite_withcb"),
        ("backend_parser_analyze", "analyze_requires_snapshot"),
        // pg_analyze_and_rewrite_fixedparams is now installed by the
        // tcop/postgres.c owner (backend-tcop-postgres init_seams; the F1
        // simple-Query pipeline owns this function and installs it via the
        // canonical owned-value query_rewrite_canonical leg). Allowlist entry
        // retired.
        ("backend_parser_analyze", "query_can_set_tag"),
        ("backend_parser_analyze", "query_command_type_is_utility"),
        ("backend_parser_analyze", "query_cte_queries"),
        ("backend_parser_analyze", "query_has_cte_list"),
        ("backend_parser_analyze", "query_has_rtable"),
        ("backend_parser_analyze", "query_has_sublinks"),
        ("backend_parser_analyze", "query_requires_rewrite_plan"),
        ("backend_parser_analyze", "query_returning_list"),
        ("backend_parser_analyze", "query_rtable_fields"),
        ("backend_parser_analyze", "query_target_list"),
        ("backend_parser_analyze", "query_utility_stmt"),
        ("backend_parser_analyze", "run_post_parse_analyze_hook"),
        ("backend_parser_analyze", "stmt_requires_parse_analysis"),
        ("backend_parser_analyze", "walk_query_sublinks_for_locks"),
        // DESIGN_DEBT (TD-PARSETYPE-TYPENAME-CARRIER, narrowed): parse_type.c's
        // `typeStringToTypeName` drives `raw_parser(str, RAW_PARSE_TYPE_NAME)` and
        // extracts the single `TypeName` node. The grammar IS now ported
        // (backend-parser-gram merged; `base_yyparse` real + installed, handles
        // MODE_TYPE_NAME) — the original "gram.y unported" blocker is gone. What
        // remains is a TypeName carrier divergence: the seam returns an owned
        // `types_parsenodes::TypeName` (no lifetime) while the grammar produces an
        // arena `types_nodes::rawnodes::TypeName<'mcx>` (PgVec<'mcx, NodePtr>).
        // Installing needs an arena->owned TypeName bridge (a contract reconcile),
        // not a bare `::set`. See DESIGN_DEBT.md.
        ("backend_parser_driver", "raw_parse_type_name"),
        // (TD-TUPLESORT-INDEX-VARIANTS retired by F3b: the tuplesort unit now
        // installs tuplesort_begin_index_btree/hash/gist + putindextuplevalues +
        // getindextuple from its init_seams(), with real comparetup_index_* /
        // writetup_index / readtup_index / removeabbrev_index bodies.)
        // DESIGN_DEBT (TD-BUFMGR-DBASE-BUFFERS): dbcommands.c's `dbase_redo`
        // (XLOG_DBASE_CREATE_FILE_COPY / XLOG_DBASE_DROP) calls
        // `FlushDatabaseBuffers(dbid)` and `DropDatabaseBuffers(dbid)` — two
        // bufmgr.c per-database shared-buffer operations. The bufmgr owner is a
        // complete CATALOG unit but its F-decomp did not port these two
        // whole-database buffer sweeps (they scan NBuffers for matching
        // RelFileLocator.dbOid). The seams are declared on the owner so the
        // landed dbase_redo consumer can call them; they loud-panic until
        // bufmgr ports DropDatabaseBuffers/FlushDatabaseBuffers. Recorded in
        // DESIGN_DEBT.md. DELETE when bufmgr installs them.
        ("backend_storage_buffer_bufmgr", "drop_database_buffers"),
        ("backend_storage_buffer_bufmgr", "flush_database_buffers"),
        // DESIGN_DEBT (TD-INDEXING-PERCATALOG-OWNERS): backend-catalog-indexing's
        // per-catalog forming/mutation bodies have now been PORTED + installed in
        // family2.rs (pg_type insert/update/rename, pg_constraint, pg_depend/
        // pg_shdepend, pg_sequence, pg_class/pg_index, pg_largeobject,
        // pg_db_role_setting, namespace, the foreign-data catalogs, the cluster
        // open/close/delete engine pass-throughs, get_catalog_object_by_oid,
        // set_relation_rule_status, set_pg_class_*). Those allowlist entries were
        // therefore DELETED — the seams are real installs now.
        //
        // The entries that REMAIN below are still genuinely uninstalled+called:
        // the typecmds.c F3/F4 narrow single-column pg_type mutators (their
        // owning typecmds arms are not yet ported). DELETE each as its owner
        // installs it.
        //
        // The generic update_object_owner_tuple (alter.c AlterObjectOwner_internal)
        // is now INSTALLED by backend-catalog-indexing's family2 — it deforms the
        // re-fetched row, sets the owner column, re-serializes aclnewowner(acl,
        // old, new) into the aclitem[] varlena via the shared acl_new_owner_datum
        // codec, CatalogTupleUpdate + UnlockTuple — so its entry was DELETED.
        ("backend_catalog_indexing", "catalog_tuple_update_typowner_typacl_pg_type"),
        ("backend_catalog_indexing", "catalog_tuple_update_typnamespace_pg_type"),
        ("backend_catalog_indexing", "catalog_tuple_update_typnotnull_pg_type"),
        ("backend_catalog_indexing", "catalog_tuple_update_typdefault_pg_type"),
        ("backend_catalog_indexing", "catalog_tuple_update_attrs_pg_type"),
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
        // -- backend-nodes-copyfuncs (K1 plancache-handle de-handle keystone) --
        // DESIGN_DEBT (TD-COPYFUNCS-PLANCACHE-HANDLES): these 10 are the
        // `-pc-seams` slice of copyfuncs.c / list.c / setrefs.c::
        // extract_query_dependencies / clauses.c::expression_planner_with_deps.
        // They take/return the OPAQUE plancache token newtypes
        // (`QueryListHandle`/`PlannedStmtListHandle`/`RawStmtHandle`/
        // `AnalyzedQueryHandle`/`ExprHandle`/`QueryHandle` in types-plancache),
        // whose storage is owned by the unported parser/planner/plancache
        // subsystems. copyfuncs's real ported body (`copy_object`) operates on
        // value-typed `Node`s, NOT these tokens; there is no token->value
        // registry (forbidden — opacity-inherited-never-introduced). All 10 are
        // consumed only by the keystone-blocked plancache unit (#159 de-handle).
        // Install + DELETE when plancache is de-handled onto owned node values.
        ("backend_nodes_copyfuncs", "copy_query_list"),
        ("backend_nodes_copyfuncs", "copy_plan_list"),
        ("backend_nodes_copyfuncs", "copy_raw_stmt"),
        ("backend_nodes_copyfuncs", "copy_analyzed_query"),
        ("backend_nodes_copyfuncs", "copy_expr"),
        ("backend_nodes_copyfuncs", "query_list_elements"),
        ("backend_nodes_copyfuncs", "plan_list_elements"),
        ("backend_nodes_copyfuncs", "list_member_oid"),
        ("backend_nodes_copyfuncs", "extract_query_dependencies"),
        ("backend_nodes_copyfuncs", "expression_planner_with_deps"),
        //
        // -- backend-commands-trigger (F1 firing/DDL leg still todo) --
        // DESIGN_DEBT (TD-TRIGGER-F1): trigger.c is CATALOG `merged` only for
        // its F0 value-type keystone (the trigger value structs landed); F1 —
        // the trigger firing logic + DDL (RemoveTriggerById / renametrig) — is
        // still todo. The 23 accessor seams are keyed by the OPAQUE foreign
        // handles `types_ri_triggers::{TriggerDataRef, TriggerRef,
        // TupleTableSlotRef}` (u64 newtypes), which ri_triggers.c treats as
        // never-deref'd foreign tokens (sanctioned semantic-opacity per the F0
        // keystone note); the trigger manager's real value-typed TriggerData /
        // Trigger / TupleTableSlot do not back these tokens, and inventing a
        // token->value registry is forbidden. `RemoveTriggerById` / `renametrig`
        // are the unported DDL legs (no body in the owner). `slot_getattr` here
        // is the trigger-manager (handle) flavor ri_triggers.c calls — a
        // distinct seam crate from execTuples::slot_getattr; it stays uninstalled
        // until trigger F1 lands. Install + DELETE each as trigger F1 lands.
        ("backend_commands_trigger", "called_as_trigger"),
        ("backend_commands_trigger", "tg_event"),
        ("backend_commands_trigger", "tg_relation_oid"),
        ("backend_commands_trigger", "tg_relation_name"),
        ("backend_commands_trigger", "tg_relation_namespace"),
        ("backend_commands_trigger", "tg_relation_owner"),
        ("backend_commands_trigger", "tg_relation_is_partitioned"),
        ("backend_commands_trigger", "tg_relation_att_name"),
        ("backend_commands_trigger", "tg_relation_att_type"),
        ("backend_commands_trigger", "tg_relation_att_collation"),
        ("backend_commands_trigger", "tg_relation_tuple_satisfies_snapshot_self"),
        ("backend_commands_trigger", "tg_trigger"),
        ("backend_commands_trigger", "tg_trigslot"),
        ("backend_commands_trigger", "tg_newslot"),
        ("backend_commands_trigger", "tg_trigtuple"),
        ("backend_commands_trigger", "tg_newtuple"),
        ("backend_commands_trigger", "trigger_constraint"),
        ("backend_commands_trigger", "trigger_constrrelid"),
        ("backend_commands_trigger", "trigger_constrindid"),
        ("backend_commands_trigger", "trigger_name"),
        // The live trigger carriers `commands/constraint.c`'s unique_key_recheck
        // drives (the heap Relation + the OLD/NEW slot TID). Owned by the per-row
        // AFTER-trigger firing substrate (AfterTriggerExecute re-resolves the
        // Relation / materializes the slots), which is not yet ported — the
        // firing engine builds the TriggerData with tg_relation/tg_trigslot/
        // tg_newslot NULL and loud-panics on the per-row fetch. Install + DELETE
        // when that substrate lands.
        ("backend_commands_trigger", "tg_relation"),
        ("backend_commands_trigger", "slot_tid"),
        ("backend_commands_trigger", "slot_attisnull"),
        ("backend_commands_trigger", "slot_is_current_xact_tuple"),
        ("backend_commands_trigger", "slot_getattr"),
        ("backend_commands_trigger", "pk_datum_image_eq"),
        ("backend_commands_trigger", "RemoveTriggerById"),
        ("backend_commands_trigger", "renametrig"),
        //
        // -- backend-access-index-genam (relcache-build scan helpers unported) --
        // DESIGN_DEBT (TD-GENAM-RELCACHE-SCANS): the genam unit ported genam.c's
        // systable_* primitive engine (begin/getnext/endscan, installed) but NOT
        // these higher-level helpers. The 6 `relcache_*`/`scan_pg_*` seams are
        // relcache.c's own systable scans (RelationGetIndexList /
        // GetStatExtList / GetFKeyList / GetExclusionInfo / AttrDefaultFetch /
        // CheckNNConstraintFetch) — relcache calls them OUTWARD, but the scan
        // bodies (systable_beginscan over pg_index/pg_statistic_ext/pg_constraint/
        // pg_attrdef + per-row deform + DeconstructFkConstraintRow / get_opcode /
        // detoast) are not yet written in the genam owner (only the DTO structs
        // exist). `build_index_value_description` (the per-key out-function +
        // ACL-visibility render) is a genam.c function not yet bodied. Install +
        // DELETE each as the genam unit ports the corresponding scan/render body.
        // (`systable_inplace_update` — the buffer-locking begin/getnext retry +
        // `heap_inplace_lock`/`heap_inplace_update_and_unlock`/`heap_inplace_unlock`
        // loop — is now bodied + installed by the genam owner, so its allowlist
        // entry was removed.)
        ("backend_access_index_genam", "relcache_scan_pg_index"),
        // `relcache_scan_pg_rewrite` (full-Query cache-ownership keystone): the
        // `pg_rewrite` scan + per-row `Form_pg_rewrite` + `ev_qual`/`ev_action`
        // node-string decode `RelationBuildRuleLock` now consumes to build the
        // real value-typed `rd_rules` (RuleLock/RewriteRule with whole
        // `Query<'static>` action trees in the CacheMemoryContext arena). Only
        // the DTO struct (`ScannedPgRewrite`) exists in the genam owner; the
        // scan body is not yet written, so the seam loud-panics
        // (mirror-PG-and-panic) until genam ports it — exactly like the sibling
        // pg_index/pg_statistic_ext/pg_constraint scans here. Install + DELETE
        // when the genam owner adds the pg_rewrite scan-and-decode body.
        ("backend_access_index_genam", "relcache_scan_pg_rewrite"),
        ("backend_access_index_genam", "relcache_scan_pg_statistic_ext"),
        ("backend_access_index_genam", "relcache_scan_pg_constraint_fkeys"),
        ("backend_access_index_genam", "relcache_exclusion_info"),
        ("backend_access_index_genam", "scan_pg_attrdef"),
        ("backend_access_index_genam", "scan_pg_constraint_nncheck"),
        ("backend_access_index_genam", "build_index_value_description"),
        //
        // -- backend-utils-cache-relcache (FDW-routine cache slot not modeled) --
        // DESIGN_DEBT (TD-RELCACHE-FDWROUTINE): `relation_fdwroutine` /
        // `set_relation_fdwroutine` read/write the relcache entry's
        // `rd_fdwroutine` cache slot (foreign.c `GetFdwRoutineForRelation`
        // memoizes the resolved `FdwRoutine` there). `types_rel::RelationData`
        // does NOT model an `rd_fdwroutine` field yet, so the relcache owner has
        // no slot to read/cache into. The other 6 newly-surfaced relcache seams
        // (critical_relcaches_built / critical_shared_relcaches_built /
        // assert_could_get_relation / rd_indcollation / index_getprocid /
        // relation_set_new_relfilenumber) WERE installed in this lane — they back
        // onto existing owned state. Install + DELETE these two when the relcache
        // entry gains the `rd_fdwroutine` cache slot.
        ("backend_utils_cache_relcache", "relation_fdwroutine"),
        ("backend_utils_cache_relcache", "set_relation_fdwroutine"),
        //
        // -- backend-utils-cache-relcache (pg_node_tree decode unported) --
        // DESIGN_DEBT (TD-RELCACHE-INDEX-NODETREE): `BuildIndexInfo` (#334,
        // catalog/index.c) calls `RelationGetIndexExpressions` /
        // `RelationGetIndexPredicate` (and, for exclusion indexes,
        // `RelationGetExclusionInfo`) unconditionally, mirroring the C
        // `makeIndexInfo(... RelationGetIndexExpressions(index),
        // RelationGetIndexPredicate(index) ...)`. The relcache owner's bodies for
        // these delegate the `pg_index.indexprs`/`indpred` `stringToNode` +
        // eval_const_expressions + fix_opfuncids node-tree transform to
        // `nodexform_seam::index_{expressions,predicate}` (the node-tree string
        // reader), which is unported — so the relcache owner cannot install them
        // and they loud-panic (mirror-PG-and-panic) when reached. The live
        // `BuildIndexInfo` consumers (bootstrap catalogs, brin, amcheck) index
        // simple columns, where the C returns NIL without decoding; the panic
        // only fires on a real expression / predicate / exclusion index. Install
        // + DELETE these three when the node-tree decode (`stringToNode`) lands.
        ("backend_utils_cache_relcache", "relation_get_index_expressions"),
        ("backend_utils_cache_relcache", "relation_get_index_predicate"),
        ("backend_utils_cache_relcache", "relation_get_exclusion_info"),
        // DESIGN_DEBT (TD-RELCACHE-INDEX-NODETREE, cont.):
        // `RelationGetDummyIndexExpressions` (relcache.c) is `BuildDummyIndexInfo`'s
        // (#334, catalog/index.c) expression source — same `pg_index.indexprs`
        // `stringToNode` node-tree decode as `RelationGetIndexExpressions`, then
        // replaces every leaf with a null `Const` of the right type/typmod/coll.
        // It rides on the SAME unported node-tree string reader, so the relcache
        // owner cannot install it and it loud-panics until `stringToNode` lands.
        // The live `BuildDummyIndexInfo` consumer (TRUNCATE of an index) only
        // reaches the decode on an expression index; simple-column indexes return
        // NIL. Install + DELETE alongside the three entries above.
        ("backend_utils_cache_relcache", "relation_get_dummy_index_expressions"),
        //
        // -- backend-catalog-indexing (pg_attribute insert substrate unported) --
        // DESIGN_DEBT (TD-INDEXING-APPEND-ATTRIBUTE-TUPLES): `AppendAttributeTuples`
        // (#334, catalog/index.c) inserts one `pg_attribute` row per index column
        // (`InsertPgAttributeTuples(pg_attribute, indexTupDesc, InvalidOid,
        // attrs_extra, indstate)`), having first run `InitializeAttributeOids`
        // (scribble the new index's OID onto its stored descriptor's `attrelid`s).
        // The catalog-indexing owner owns pg_attribute writes but has not yet
        // ported `InsertPgAttributeTuples` (the `heap_form_tuple` over
        // `Form_pg_attribute` + `CatalogTuplesMultiInsertWithInfo` path) nor the
        // descriptor-mutation entry point, so it cannot install this and the seam
        // loud-panics (mirror-PG-and-panic). Reached only from `index_create`,
        // which is itself uninstalled (catalog-write driver substrate unported).
        // Install + DELETE when catalog-indexing ports `InsertPgAttributeTuples`.
        ("backend_catalog_indexing", "append_attribute_tuples"),
        //
        // -- backend-utils-cache-inval (OID-refetch wrapper unported) --
        // DESIGN_DEBT (TD-INVAL-OID-REFETCH): `cache_invalidate_heap_tuple`
        // (class_id, object_id) is the `CacheInvalidateHeapTuple(rel, tuple,
        // NULL)` reduction the typecmds ALTER DOMAIN paths need — re-fetch the
        // catalog row by OID (table_open(class_id) + syscache fetch by
        // object_id), then run the shared invalidation logic. The inval owner
        // HAS the shared engine (`cache_invalidate_heap_tuple_common(relation,
        // tuple, ...)`) but NOT the (classId, objectId) re-fetch wrapper around
        // it (it has no catalog open + OID syscache fetch by dynamic class). The
        // signature diverges (OID pair vs &RelationData + &HeapTupleData), so no
        // bare `::set` of the common body fits. Install + DELETE when the inval
        // owner adds the OID-keyed re-fetch wrapper.
        ("backend_utils_cache_inval", "cache_invalidate_heap_tuple"),
        // DESIGN_DEBT (TD-ENCNAMES-ICU): `is_encoding_supported_by_icu` is declared
        // in `backend-utils-mb-mbutils-seams` but its logic is `common/encnames.c`'s
        // `pg_enc2icu_tbl` (encnames.c:461), NOT mbutils.c — mbutils.c never calls
        // it. Its real owner is the encnames unit (unported in this model); the only
        // consumer is `recomputeNamespacePath`'s ICU branch (namespace.c:2323).
        // Wrong-homing the ICU name table in the mbutils owner would violate
        // ownership-by-C-source, so the mbutils owner deliberately does NOT install
        // it; it loud-panics until encnames lands. DELETE this entry when encnames is
        // ported and installs the seam.
        ("backend_utils_mb_mbutils", "is_encoding_supported_by_icu"),
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
        // DESIGN_DEBT (TD-DBROLESETTING-VARSETSTMT): `alter_database_setting`
        // (declared in `backend-catalog-pg-db-role-setting-seams`) is the
        // AlterDatabaseSet -> `AlterSetting(datid, InvalidOid, setstmt)` boundary.
        // The parser hands a `types_nodes::ddlnodes::VariableSetStmt<'mcx>` (the
        // arena parse-node layer), but the owner's ported `AlterSetting` consumes
        // `types_parsenodes::VariableSetStmt` (the owned-`String` layer); the two
        // parse-node models meet only here and no node-model converter for
        // VariableSetStmt is ported yet. The owner deliberately does NOT install
        // it (force-wiring would require a converter that doesn't exist); it
        // loud-panics until the VariableSetStmt node-model bridge lands. DELETE
        // this entry then.
        ("backend_catalog_pg_db_role_setting", "alter_database_setting"),
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
        // (audited). The eleven seams below are tablecmds.c functions belonging to
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
        ("backend_commands_tablecmds", "at_exec_change_owner"),
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
        //  * `relation_init_index_access_info` (relcache.c): the owner's
        //    `RelationInitIndexAccessInfo(&mut RelationData)` runs inside the
        //    registry build with a `&mut` entry; the relcache exposes no
        //    mutable-by-OID registry accessor a by-OID seam could use. Needs the
        //    registry-mutable-entry keystone. Bootstrap-only leg (non-bootstrap
        //    rebuilds the entry via the sinval flush at CommandCounterIncrement).
        //  * `create_unique_key_recheck_trigger` (the `CreateTrigger` call in
        //    index_constraint_create's deferrable leg): the trigger manager owner
        //    has not ported `CreateTrigger` yet. Loud-panics until trigger.c's
        //    CreateTrigger lands; only reached for a DEFERRABLE PK/UNIQUE.
        //
        // Delete each entry when its owner installs the seam.
        ("backend_bootstrap_bootstrap", "index_register"),
        ("backend_utils_cache_relcache", "relation_init_index_access_info"),
        ("backend_commands_trigger", "create_unique_key_recheck_trigger"),
    ];

    /// CATALOG.tsv unit statuses that mean the owner crate is COMPLETE — its
    /// declared seams are an installed contract, not a mid-port frontier where
    /// `mirror-pg-and-panic` legitimately keeps them panicking.
    fn is_complete_status(status: &str) -> bool {
        status == "merged" || status == "audited"
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
