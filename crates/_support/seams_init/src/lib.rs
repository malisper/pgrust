/// The transport provider for this process's client byte path (§2.4 seam,
/// docs/design/dst-and-wasm.md): which implementations get installed into
/// be_secure_seams::{secure_read,secure_write,secure_close,set_port_noblock}
/// and pqcomm_seams::{pq_init,modify_fe_be_wait_set_latch}. Resolved ONCE
/// here, during single-threaded boot — the hot path pays the same one
/// relaxed-load + indirect-call it always did, no per-byte branch. P4
/// sim-net is the third provider (in-memory duplex under the sim scheduler)
/// and installs into the SAME slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    /// Existing socket code (libc::recv/send + listen/accept): the default,
    /// byte-identical native arm.
    Socket,
    /// pgwire over stdin/stdout (pqcomm_stdio): --stdio-wire on any target;
    /// the wasm32-wasip1 client-server story (WASI p1 has no socket()).
    StdioWire,
    /// The deterministic in-memory duplex pair (pqcomm_simnet): --sim-net,
    /// `--cfg pgrust_sim` builds only (P4; the variant does not exist on
    /// product builds).
    #[cfg(pgrust_sim)]
    SimNet,
    /// Every connection is a PAIR OF FILE DESCRIPTORS handed to the server
    /// by its host, and the listener is a host-owned fd carrying one
    /// fixed-size record per new connection (pqcomm_hostpipes):
    /// --host-pipes on any target. Unlike StdioWire this is a REAL
    /// postmaster with N concurrent backend threads, so the provider
    /// installs the postmaster half of the seam (listen/accept) as well —
    /// and it needs no socket() anywhere, which is what makes a
    /// multi-session server possible on wasm32-wasip1-threads.
    HostPipes,
}

// One line per crate: the full seam-install closure for the postgres binary.
pub fn init_all() {
    init_all_with_transport(Transport::Socket)
}

// ---------------------------------------------------------------------------
// Serial-lease v2 boot posture (GL-SLEASE-2). The wait-report seam pair is
// the single choke point every C-parity blocking site brackets its sleep
// with; when the lease is armed (PGRUST_RUNTIME_SERIAL_LEASE=1) the seams
// point at these wrappers — the stock reporter plus the lease's donation
// hook — and ProcessInterrupts' admission tap is installed. Unarmed (the
// default): waitevent::init_seams() verbatim, no tap — the stock hot path
// is byte-identical (seams are set-once fn pointers; no extra branch
// exists anywhere). This selector lives HERE, not in waitevent, because it
// needs both the reporter impls and the lease engine (execmain), and boot
// wiring is this crate's charter. Umbra-model deletability (the GL-SLEASE-2
// letter): task-ified serial execution deletes this block and the seams
// revert to waitevent::init_seams() unconditionally.
// ---------------------------------------------------------------------------

fn slease_wait_start(wait_event_info: u32) {
    waitevent::pgstat_report_wait_start(wait_event_info);
    execmain::serial_lease_wait_hook_start();
}

fn slease_wait_end() {
    // Reacquire first (a try — never blocks), then clear the reported wait:
    // the donation overhead is attributed to the wait span it serves.
    execmain::serial_lease_wait_hook_end();
    waitevent::pgstat_report_wait_end();
}

fn init_waitevent_seams_with_lease_posture() {
    if execmain::serial_lease_armed() {
        if execmain::serial_lease_donation_enabled() {
            waitevent_seams::pgstat_report_wait_start::set(slease_wait_start);
            waitevent_seams::pgstat_report_wait_end::set(slease_wait_end);
            waitevent_seams::pgstat_set_wait_event_storage::set(
                waitevent::pgstat_set_wait_event_storage,
            );
            waitevent_seams::pgstat_reset_wait_event_storage::set(
                waitevent::pgstat_reset_wait_event_storage,
            );
        } else {
            // GL-SLEASE-3 ladder arm (PGRUST_RUNTIME_SERIAL_LEASE_DONATION=0):
            // armed WITHOUT donation — stock wait seams, admission tap only.
            // Admitted runs hold their permit across blocking waits (the v1
            // semantics, deliberately): attribution arm, never a shipping
            // posture.
            waitevent::init_seams();
        }
        postgres_seams::tap_serial_lease_admission::install(execmain::serial_lease_admission_tap);
    } else {
        waitevent::init_seams();
    }
}

pub fn init_all_with_transport(transport: Transport) {
    install_panic_hook();
    detoast::init_seams();
    ruleutils::init_seams();
    heaptoast::init_seams();
    printtup::init_seams();
    relation::init_seams();
    heapam_visibility::init_seams();
    genam::init_seams();
    indexam::init_seams();
    #[cfg(feature = "index-brin")]
    brin_build::init_seams();
    table::init_seams();
    tableam::init_seams();
    vacuumlazy::init_seams();
    commands_vacuum::init_seams();
    #[cfg(feature = "parallel")]
    vacuumparallel::init_seams();
    commands_analyze::init_seams();
    commands_tablespace::init_seams();
    sequence::init_seams();
    pg_constraint::init_seams();
    extension::init_seams();
    clog::init_seams();
    commit_ts::init_seams();
    multixact::init_seams();
    rmgr::init_seams();
    subtrans::init_seams();
    transam_xlog::init_seams();
    transam::init_seams();
    varsup::init_seams();
    xact::init_seams();
    xloginsert::init_seams();
    xlogreader::init_seams();
    xlogprefetcher::init_seams();
    xlogrecovery::init_seams();
    timeline::init_seams();
    xlogarchive::init_seams();
    xlogutils::init_seams();
    twophase::init_seams();
    twophase_config::init_seams();
    aclchk::init_seams();
    commands_amcmds::init_seams();
    amapi::init_seams();
    rls::init_seams();
    commands_policy::init_seams();
    commands_publicationcmds::init_seams();
    be_fsstubs::init_seams();
    large_object::init_seams();
    catalog_namespace::init_seams();
    catalog_dependency::init_seams();
    catalog_objectaddress::init_seams();
    tablecmds::init_seams();
    typecmds::init_seams();
    commands_alter::init_seams();
    schemacmds::init_seams();
    subscriptioncmds::init_seams();
    commands_cluster::init_seams();
    event_trigger::init_seams();
    catalog::init_seams();
    catalog_storage::init_seams();
    pg_database::init_seams();
    pg_db_role_setting::init_seams();
    indexcmds::init_seams();
    catalog_index::init_seams();
    pg_class::init_seams();
    pg_inherits::init_seams();
    dbcommands::init_seams();
    executils::init_seams();
    execexpr::init_seams();
    execjunk::init_seams();
    execmain::init_seams();
    execscan::init_seams();
    nodeseqscan::init_seams();
    nodesamplescan::init_seams();
    tablesample::init_seams();
    nodeindexscan::init_seams();
    nodetidscan::init_seams();
    nodetidrangescan::init_seams();
    nodeindexonlyscan::init_seams();
    nodeagg::init_seams();
    nodewindowagg::init_seams();
    nodesort::init_seams();
    nodegroup::init_seams();
    nodeunique::init_seams();
    nodelimit::init_seams();
    nodelockrows::init_seams();
    nodenestloop::init_seams();
    nodemergejoin::init_seams();
    nodematerial::init_seams();
    nodememoize::init_seams();
    tuplesort::init_seams();
    auth::init_seams();
    auth_oauth::init_seams();
    oauth_validators::init_seams();
    auth_scram::init_seams();
    crypt::init_seams();
    hba::init_seams();
    libpq_pqsignal::init_seams();
    pqcomm::init_seams();
    match transport {
        Transport::Socket => {
            be_secure::init_seams();
            pqcomm::init_socket_seams();
        }
        Transport::StdioWire => {
            pqcomm_stdio::init_transport_seams();
            pqcomm::init_socket_gucs();
        }
        // P4 sim-net: the third provider into the same slots, incl. the
        // virtual listen/accept pair the init_seams split freed.
        #[cfg(pgrust_sim)]
        Transport::SimNet => {
            pqcomm_simnet::init_transport_seams();
            pqcomm::init_socket_gucs();
        }
        // Host-pipes: the fourth provider into the same slots — both
        // halves, since this one runs the real postmaster (listen/accept
        // ride the same set-once pair the sim-net split freed).
        Transport::HostPipes => {
            pqcomm_hostpipes::init_transport_seams();
            pqcomm::init_socket_gucs();
        }
    }
    pqformat::init_seams();
    vars::init_seams();
    parser_driver::init_seams();
    parse_expr::init_seams();
    coerce::init_seams();
    parse_func::init_seams();
    parse_target::init_seams();
    parse_utilcmd::init_seams();
    parse_collate::init_seams();
    parse_relation::init_seams();
    parse_clause::init_seams();
    parser_analyze::init_seams();
    scan_fgram::init_seams();
    pg_sema::init_seams();
    autovacuum::init_seams();
    interrupt::init_seams();
    launch_backend::init_seams();
    pmchild::init_seams();
    postmaster::init_seams();
    auxprocess::init_seams();
    checkpointer::init_seams();
    bgwriter::init_seams();
    walwriter::init_seams();
    pgarch::init_seams();
    walsummarizer::init_seams();
    postmaster_startup::init_seams();
    syslogger::init_seams();
    #[cfg(feature = "replication")]
    launcher::init_seams();
    janitor::init_seams();
    #[cfg(feature = "replication")]
    walsender_config::init_seams();
    #[cfg(feature = "replication")]
    walsender::init_seams();
    #[cfg(feature = "replication")]
    syncrep::init_seams();
    #[cfg(feature = "replication")]
    walreceiverfuncs::init_seams();
    #[cfg(feature = "replication")]
    walreceiver::init_seams();
    #[cfg(feature = "backup")]
    basebackup::init_seams();
    #[cfg(feature = "replication")]
    slot::init_seams();
    #[cfg(feature = "replication")]
    reorderbuffer::init_seams();
    #[cfg(feature = "replication")]
    snapbuild::init_seams();
    rewriteheap::init_seams();
    #[cfg(feature = "replication")]
    logical::init_seams();
    #[cfg(feature = "replication")]
    origin::init_seams();
    #[cfg(feature = "replication")]
    logicalworker::init_seams();
    #[cfg(feature = "contrib-test_decoding")]
    test_decoding::init_seams();
    #[cfg(feature = "contrib-pgoutput")]
    pgoutput::init_seams();
    adt_formatting::init_seams();
    #[cfg(feature = "contrib-citext")]
    citext::init_seams();
    #[cfg(feature = "contrib-isn")]
    isn::init_seams();
    #[cfg(feature = "contrib-pg_surgery")]
    pg_surgery::init_seams();
    #[cfg(feature = "contrib-uuid_ossp")]
    uuid_ossp::init_seams();
    #[cfg(feature = "contrib-dblink")]
    dblink::init_seams();
    #[cfg(feature = "contrib-pg_prewarm")]
    pg_prewarm::init_seams();
    #[cfg(feature = "contrib-file_fdw")]
    file_fdw::init_seams();
    #[cfg(feature = "contrib-postgres_fdw")]
    postgres_fdw::init_seams();
    #[cfg(feature = "contrib-ltree")]
    ltree::init_seams();
    #[cfg(feature = "contrib-intarray")]
    intarray::init_seams();
    #[cfg(feature = "contrib-pgcrypto")]
    pgcrypto::init_seams();
    #[cfg(feature = "contrib-pg_stat_statements")]
    pg_stat_statements::init_seams();
    #[cfg(feature = "contrib-pg_buffercache")]
    pg_buffercache::init_seams();
    #[cfg(feature = "contrib-auto_explain")]
    auto_explain::init_seams();
    #[cfg(feature = "contrib-passwordcheck")]
    passwordcheck::init_seams();
    #[cfg(feature = "contrib-test_oat_hooks")]
    test_oat_hooks::init_seams();
    #[cfg(feature = "contrib-pgvector")]
    pgvector::init_seams();
    #[cfg(feature = "contrib-pgvector_hnsw")]
    pgvector_hnsw::init_seams();
    #[cfg(feature = "contrib-bloom")]
    bloom::init_seams();
    #[cfg(feature = "contrib-hstore")]
    hstore::init_seams();
    adt_expandedrecord::init_seams();
    #[cfg(feature = "contrib-pg_trgm")]
    pg_trgm::init_seams();
    #[cfg(feature = "contrib-btree_gist")]
    btree_gist::init_seams();
    #[cfg(feature = "contrib-btree_gin")]
    btree_gin::init_seams();
    #[cfg(feature = "contrib-cube")]
    contrib_cube::init_seams();
    #[cfg(feature = "contrib-earthdistance")]
    contrib_earthdistance::init_seams();
    #[cfg(feature = "contrib-seg")]
    contrib_seg::init_seams();
    #[cfg(feature = "contrib-unaccent")]
    unaccent::init_seams();
    #[cfg(feature = "contrib-pg_walinspect")]
    pg_walinspect::init_seams();
    #[cfg(feature = "contrib-injection_points")]
    injection_points::init_seams();
    #[cfg(feature = "contrib-test_custom_types")]
    test_custom_types::init_seams();
    #[cfg(feature = "contrib-sslinfo")]
    sslinfo::init_seams();
    #[cfg(feature = "contrib-fuzzystrmatch")]
    fuzzystrmatch::init_seams();
    #[cfg(feature = "contrib-tablefunc")]
    tablefunc::init_seams();
    #[cfg(feature = "contrib-lo")]
    contrib_lo::init_seams();
    #[cfg(feature = "contrib-tcn")]
    tcn::init_seams();
    #[cfg(feature = "contrib-pageinspect")]
    pageinspect::init_seams();
    #[cfg(feature = "contrib-pgstattuple")]
    pgstattuple::init_seams();
    #[cfg(feature = "contrib-pg_freespacemap")]
    pg_freespacemap::init_seams();
    #[cfg(feature = "contrib-amcheck")]
    amcheck::init_seams();
    #[cfg(feature = "contrib-tsm_system_rows")]
    tsm_system_rows::init_seams();
    #[cfg(feature = "contrib-tsm_system_time")]
    tsm_system_time::init_seams();
    #[cfg(feature = "contrib-pg_visibility")]
    pg_visibility::init_seams();
    #[cfg(feature = "contrib-pgrowlocks")]
    pgrowlocks::init_seams();
    #[cfg(feature = "contrib-pg_logicalinspect")]
    pg_logicalinspect::init_seams();
    #[cfg(feature = "contrib-pg_overexplain")]
    pg_overexplain::init_seams();
    session::init_seams();
    relpath::init_seams();
    rewrite_handler::init_seams();
    rewrite_define::init_seams();
    opclasscmds::init_seams();
    aio_core::init_seams();
    aio_uring::init_seams();
    bufmgr::init_seams();
    fd::init_seams();
    dsm_core::init_seams();
    ipc::init_seams();
    ipci::init_seams();
    syncscan::init_seams();
    latch::init_seams();
    pmsignal::init_seams();
    procarray::init_seams();
    procsignal::init_seams();
    shmem::init_seams();
    // The foreign-segment probe CreateLockFile needs when the data directory
    // was last held by C PostgreSQL (GL-SHMSEAM-1). pgrust creates no SysV
    // segment of its own, so this is a read-only install.
    sysv_shmem::init_seams();
    sinval::init_seams();
    standby::init_seams();
    waiteventset::init_seams();
    lmgr_proc::init_seams();
    s_lock::init_seams();
    condition_variable::init_seams();
    deadlock::init_seams();
    predicate::init_seams();
    #[cfg(feature = "parallel")]
    parallel::init_seams();
    #[cfg(feature = "parallel")]
    bgworker::init_seams();
    spi::init_seams();
    trigger::init_seams();
    ri_triggers::init_seams();
    pruneheap::init_seams();
    pg_string::init_seams();
    lmgr::init_seams();
    lock::init_seams();
    smgr::init_seams();
    sync::init_seams();
    freespace::init_seams();
    backend_startup::init_seams();
    tcop_dest::init_seams();
    postgres::init_seams();
    pquery::init_seams();
    explain::init_seams();
    commands_createas::init_seams();
    copy_cmd::init_seams();
    commands_matview::init_seams();
    execreplication::init_seams();
    prepare::init_seams();
    portalcmds::init_seams();
    portalmem::init_seams();
    commands_async::init_seams();
    utility::init_seams();
    backend_status::init_seams();
    backend_progress::init_seams();
    init_waitevent_seams_with_lease_posture();
    mcxt_stats::init_seams();
    mcxt_stats::set_slot_census(slot_census_line);
    pgstat::init_seams();
    adt_acl::init_seams();
    adt_timestamp::init_seams();
    pgtz::init_seams();
    adt_bool::init_seams();
    adt_float::init_seams();
    arrayfuncs::init_seams();
    pg_locale::init_seams();
    varlena::init_seams();
    adt_xml::init_seams();
    cache_syscache::init_seams();
    catcache::init_seams();
    inval::init_seams();
    lsyscache::init_seams();
    plancache::init_seams();
    planner::init_seams();
    costsize::init_seams();
    allpaths::init_seams();
    relcache::init_seams();
    relcache_build::init_seams();
    relmapper::init_seams();
    relfilenumbermap::init_seams();
    typcache::init_seams();
    clauses::init_seams();
    pg_enum::init_seams();
    pg_publication::init_seams();
    pg_subscription::init_seams();
    elog::init_seams();
    fmgr_core::init_seams();
    fmgr_core::register_late_builtins(adt_acl::builtins::ACL_BUILTINS);
    #[cfg(feature = "tsearch")]
    fmgr_core::register_late_builtins(adt_tsvector_stat::TS_STAT_BUILTINS);
    #[cfg(feature = "replication")]
    fmgr_core::register_late_builtins(slotfuncs::builtins::SLOTFUNCS_BUILTINS);
    #[cfg(feature = "replication")]
    slotfuncs::init_seams();
    #[cfg(feature = "replication")]
    slotsync::init_seams();
    fmgr_core::register_late_builtins(waitevent::funcs::WAITEVENT_BUILTINS);
    #[cfg(feature = "replication")]
    fmgr_core::register_late_builtins(logicalfuncs::LOGICALFUNCS_BUILTINS);
    fmgr_core::register_late_builtins(rls::RLS_BUILTINS);
    fmgr_core::register_late_builtins(rewrite_handler::REWRITE_BUILTINS);
    fmgr_core::register_late_builtins(opclasscmds::builtins::OPCLASS_BUILTINS);
    operatorcmds::init_seams();
    foreigncmds::init_seams();
    fmgr_core::register_late_builtins(adt_misc::MISC_BUILTINS);
    fmgr_core::register_late_builtins(genfile::GENFILE_BUILTINS);
    fmgr_core::register_late_builtins(pg_upgrade_support::PG_UPGRADE_SUPPORT_BUILTINS);
    fmgr_core::register_late_builtins(guc_funcs::GUC_FUNCS_BUILTINS);
    fmgr_core::register_late_builtins(prepare::PREPARE_BUILTINS);
    fmgr_core::register_late_builtins(mbutils::builtins::MBUTILS_BUILTINS);
    fmgr_core::register_late_builtins(dbcommands::builtins::DBCOMMANDS_BUILTINS);
    fmgr_core::register_late_builtins(collationcmds::builtins::COLLATIONCMDS_BUILTINS);
    multixactfuncs::register_builtins();
    fmgr_core::register_late_builtins(adt_rowtypes::ROWTYPES_BUILTINS);
    fmgr_core::register_late_builtins(xmlmap::builtins::XMLMAP_BUILTINS);
    fmgr_core::register_late_builtins(be_fsstubs::fmgr_builtins::FSSTUBS_BUILTINS);
    fmgr_core::register_late_builtins(partbounds::PARTBOUNDS_BUILTINS);
    #[cfg(feature = "tsearch")]
    fmgr_core::register_late_builtins(adt_tsquery_rewrite::TSQUERY_REWRITE_BUILTINS);
    sql_functions::init_seams();
    pg_proc::init_seams();
    regress_lib::init_seams();
    #[cfg(feature = "plpgsql")]
    plpgsql::init_seams();
    #[cfg(feature = "tsearch")]
    dict_snowball::init_seams();
    #[cfg(feature = "geo")]
    fmgr_core::register_late_builtins(adt_geo::builtins::GEO_BUILTINS);
    #[cfg(any(feature = "geo", feature = "index-gist"))]
    fmgr_core::register_late_builtins(gistproc::GISTPROC_BUILTINS);
    fmgr_core::register_late_builtins(commands_constraint::CONSTRAINT_BUILTINS);
    #[cfg(feature = "index-spgist")]
    fmgr_core::register_late_builtins(spgist_text::SPGIST_TEXT_BUILTINS);
    #[cfg(feature = "index-gist")]
    fmgr_core::register_late_builtins(rangetypes_gist::RANGETYPES_GIST_BUILTINS);
    #[cfg(feature = "index-spgist")]
    fmgr_core::register_late_builtins(rangetypes_spgist::RANGETYPES_SPGIST_BUILTINS);
    #[cfg(feature = "index-gist")]
    fmgr_core::register_late_builtins(network_gist::NETWORK_GIST_BUILTINS);
    #[cfg(feature = "tsearch")]
    fmgr_core::register_late_builtins(adt_tsginidx::builtins::TSGINIDX_BUILTINS);
    #[cfg(feature = "tsearch")]
    fmgr_core::register_late_builtins(adt_tsgistidx::TSGISTIDX_BUILTINS);
    #[cfg(feature = "index-spgist")]
    fmgr_core::register_late_builtins(network_spgist::NETWORK_SPGIST_BUILTINS);
    #[cfg(feature = "index-spgist")]
    fmgr_core::register_late_builtins(spgist_quadtree::SPGIST_QUAD_BUILTINS);
    #[cfg(feature = "index-spgist")]
    fmgr_core::register_late_builtins(spgist_kdtree::SPGIST_KD_BUILTINS);
    #[cfg(feature = "index-spgist")]
    fmgr_core::register_late_builtins(spgist_box::SPGIST_BOX_BUILTINS);
    #[cfg(feature = "index-brin")]
    fmgr_core::register_late_builtins(brin_minmax_multi::MINMAX_MULTI_BUILTINS);
    #[cfg(feature = "index-brin")]
    fmgr_core::register_late_builtins(brin_bloom::BLOOM_BUILTINS);
    #[cfg(feature = "index-brin")]
    fmgr_core::register_late_builtins(brin_funcs::BRIN_FUNCS_BUILTINS);
    fmgr_core::register_late_builtins(partitionfuncs::PARTITIONFUNCS_BUILTINS);
    fmgr_core::register_late_builtins(orderedsetaggs::ORDEREDSETAGGS_BUILTINS);
    fmgr_core::register_late_builtins(pg_controldata::PG_CONTROLDATA_BUILTINS);
    fmgr_core::register_late_builtins(pg_config::PG_CONFIG_BUILTINS);
    fmgr_core::register_late_builtins(shmem::SHMEM_BUILTINS);
    fmgr_core::register_late_builtins(aio_funcs::AIO_FUNCS_BUILTINS);
    #[cfg(feature = "replication")]
    fmgr_core::register_late_builtins(origin::ORIGIN_BUILTINS);
    #[cfg(feature = "replication")]
    fmgr_core::register_late_builtins(logicalrelation::LOGICALRELATION_BUILTINS);
    funcapi::init_seams();
    init_small::init_seams();
    miscinit::init_seams();
    mbutils::init_seams();
    conffiles::init_seams();
    guc_file::init_seams();
    guc_tables::init_seams();
    guc::init_seams();
    guc_funcs::init_seams();
    variable::init_seams();
    #[cfg(feature = "index-gin")]
    gin::init_seams();
    #[cfg(feature = "index-gin")]
    gin_funcs::init_seams();
    user::init_seams();
    ps_status::init_seams();
    queryenvironment::init_seams();
    stack_depth::init_seams();
    superuser::init_seams();
    timeout::init_seams();
    tuplestore::init_seams();
    resowner::init_seams();
    combocid::init_seams();
    snapmgr::init_seams();
    pg_prng::init_seams();
    regex_core::init_seams();
    adt_regexp::init_seams();
    #[cfg(feature = "tsearch")]
    ts_cache::init_hooks();
    seclabel::init();

    static EXTRA_BUILTINS: [&[types_fmgr::FmgrBuiltin]; 8] = [
        adt_misc::builtins::MISC_BUILTINS,
        catalog_namespace::builtins::NAMESPACE_BUILTINS,
        format_type::builtins::FORMAT_TYPE_BUILTINS,
        ruleutils::builtins::RULEUTILS_BUILTINS,
        statistics::builtins::STATISTICS_BUILTINS,
        stats_import::STATS_IMPORT_BUILTINS,
        // pgrust-native (reserved-range oids 9010/9011): the sqe census
        // SRFs (execmain sqeshell/stat.rs; created on demand by
        // scripts/sqe-stat-views.sql — no catalog delta by default).
        execmain::SQE_BUILTINS,
        // pgrust-native (reserved-range oids 9001/9002/9005): the
        // ephemeral-db janitor's pin/unpin/seal surface — TRUE builtins
        // whose pg_proc rows janitor::bootstrap backfills into every
        // database on first connection (no install script).
        janitor::JANITOR_BUILTINS,
    ];
    fmgr_core::install_extra_builtins(&EXTRA_BUILTINS);
}

// A PgError-payload panic is the ereport-through-infallible-C idiom (a sort
// comparator error unwinding through qsort, etc.): caught at the nearest
// catch_unwind boundary and re-raised as a client ERROR, matching C's
// ereport/longjmp — never a crash. Suppress its default "panicked at" line so
// crash gates don't misread the error path as a backend crash. Every other
// payload (string panic!/unwrap/assert, PanicExitThread abort) keeps the loud
// default hook.
fn install_panic_hook() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let p = info.payload();
            if p.is::<elog::PgError>() || p.is::<Box<elog::PgError>>() {
                return;
            }
            default_hook(info);
            // The statement the backend was executing — the panic line alone
            // is unactionable from a fuzz soak without it (r21 alter.rs:3954).
            elog::with_debug_query_string(|q| {
                if let Some(q) = q {
                    eprintln!("panicking backend query: {q}");
                }
            });
        }));
    });
}

// memgrowth-discriminator (suspect-A census): one line over every raw-Rust
// slot registry — thread-local Vec slabs invisible to the context ledger.
// Installed into mcxt_stats so both the `pgrust: memctx` debug command and
// the pg_log_backend_memory_contexts interrupt dump (which executes on the
// TARGET backend thread) report the calling backend's registries.
fn slot_census_line() -> String {
    let (qd_len, qd_cap, qd_free) = execmain::slot_census();
    let (sl_len, sl_cap, sl_free) = pquery::stmt_list::slot_census();
    let (qe_len, qe_cap, qe_free) = queryenvironment::hold::slot_census();
    let (ts_len, ts_cap, ts_free) = tuplestore::hold::slot_census();
    let (ro_len, ro_cap, ro_free) = resowner::arena_census();
    format!(
        "slot census: querydesc len={qd_len} cap={qd_cap} free={qd_free}; \
stmt_list len={sl_len} cap={sl_cap} free={sl_free}; \
queryenv len={qe_len} cap={qe_cap} free={qe_free}; \
tuplestore len={ts_len} cap={ts_cap} free={ts_free}; \
resowner len={ro_len} cap={ro_cap} free={ro_free} arena_bytes={}",
        ro_cap * 1024
    )
}
