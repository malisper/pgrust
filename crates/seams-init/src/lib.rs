//! Startup aggregator: calls every ported crate's `init_seams()`.
//!
//! This crate contains NO logic and NO `set()` calls of its own — one line
//! per ported crate, nothing else. Each crate wires its own seams in its own
//! `init_seams()`; this is just the place that invokes them all.

pub fn init_all() {
    // One line per ported crate, kept sorted:
    contrib_amcheck_verify_nbtree::init_seams();
    backend_archive_shell_archive::init_seams();
    backend_access_common_detoast::init_seams();
    backend_access_common_heaptuple::init_seams();
    backend_access_common_indextuple::init_seams();
    backend_access_common_relation::init_seams();
    backend_access_common_reloptions::init_seams();
    backend_access_common_tidstore::init_seams();
    backend_access_common_tupdesc::init_seams();
    backend_access_gin_core_probe::init_seams();
    backend_access_gin_ginget::init_seams();
    backend_access_gin_ginscan::init_seams();
    backend_access_hashvalidate::init_seams();
    backend_access_heap_heapam::init_seams();
    backend_access_heap_heapam_visibility::init_seams();
    backend_access_heap_heaptoast::init_seams();
    backend_access_heap_pruneheap::init_seams();
    backend_access_heap_vacuumlazy::init_seams();
    backend_access_heap_visibilitymap::init_seams();
    backend_access_index_amapi::init_seams();
    backend_access_index_genam::init_seams();
    backend_access_index_indexam::init_seams();
    backend_access_spg_proc::init_seams();
    backend_access_spg_quadtree::init_seams();
    backend_access_nbt_dedup::init_seams();
    backend_access_nbt_xlog::init_seams();
    backend_access_nbtree_nbtree::init_seams();
    backend_access_rmgrdesc_small::init_seams();
    backend_access_rmgrdesc_xactdesc::init_seams();
    backend_access_table_table::init_seams();
    backend_access_table_tableam::init_seams();
    backend_access_brin_xlog::init_seams();
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
    backend_backup_server::init_seams();
    backend_backup_sink::init_seams();
    backend_bootstrap_bootstrap::init_seams();
    backend_catalog_catalog::init_seams();
    backend_catalog_namespace::init_seams();
    backend_catalog_objectaccess::init_seams();
    backend_catalog_objectaddress::init_seams();
    backend_catalog_pg_cast::init_seams();
    backend_catalog_pg_class::init_seams();
    backend_catalog_pg_conversion::init_seams();
    backend_catalog_pg_db_role_setting::init_seams();
    backend_catalog_pg_constraint::init_seams();
    backend_catalog_pg_depend::init_seams();
    backend_catalog_pg_enum::init_seams();
    backend_catalog_pg_range::init_seams();
    backend_catalog_pg_largeobject::init_seams();
    backend_catalog_pg_namespace::init_seams();
    backend_catalog_pg_shdepend::init_seams();
    backend_catalog_toasting::init_seams();
    backend_commands_amcmds::init_seams();
    backend_commands_cluster::init_seams();
    backend_commands_comment::init_seams();
    backend_commands_conversioncmds::init_seams();
    backend_commands_copyto::init_seams();
    backend_commands_define::init_seams();
    backend_commands_dropcmds::init_seams();
    backend_commands_explain::init_seams();
    backend_commands_foreigncmds::init_seams();
    backend_commands_functioncmds::init_seams();
    backend_commands_opclasscmds::init_seams();
    backend_commands_matview::init_seams();
    backend_commands_portalcmds::init_seams();
    backend_executor_execAmi::init_seams();
    backend_executor_execCurrent::init_seams();
    backend_executor_execExpr::init_seams();
    backend_executor_execExprInterp::init_seams();
    backend_executor_execJunk::init_seams();
    backend_executor_execMain::init_seams();
    backend_executor_execParallel::init_seams();
    backend_executor_execPartition::init_seams();
    backend_executor_execProcnode::init_seams();
    backend_executor_execScan::init_seams();
    backend_executor_execTuples::init_seams();
    backend_executor_execUtils::init_seams();
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
    backend_executor_nodeSeqscan::init_seams();
    backend_executor_nodeSetOp::init_seams();
    backend_executor_nodeSubqueryscan::init_seams();
    backend_executor_nodeSort::init_seams();
    backend_executor_nodeSubplan::init_seams();
    backend_executor_tqueue::init_seams();
    backend_executor_nodeUnique::init_seams();
    backend_executor_nodeValuesscan::init_seams();
    backend_geqo_all::init_seams();
    backend_lib_bloomfilter::init_seams();
    backend_lib_dshash::init_seams();
    backend_main_main::init_seams();
    backend_libpq_pqcomm::init_seams();
    backend_libpq_pqformat::init_seams();
    backend_libpq_pqsignal::init_seams();
    backend_nodes_copyfuncs::init_seams();
    backend_nodes_core::init_seams();
    backend_nodes_equalfuncs::init_seams();
    backend_access_hash_core::init_seams();
    backend_access_hash_entry::init_seams();
    backend_nodes_extensible::init_seams();
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
    backend_optimizer_path_small::init_seams();
    backend_optimizer_util_joininfo::init_seams();
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
    backend_optimizer_plan_subselect_pullup::init_seams();
    backend_optimizer_util_inherit_predtest::init_seams();
    backend_optimizer_util_pathnode::init_seams();
    backend_parser_coerce::init_seams();
    backend_parser_parse_oper::init_seams();
    backend_parser_parse_type::init_seams();
    backend_parser_relation::init_seams();
    backend_parser_analyze::init_seams();
    backend_parser_small1::init_seams();
    backend_parser_gram_core::init_seams();
    backend_port_atomics::init_seams();
    backend_postmaster_autovacuum::init_seams();
    backend_postmaster_bgworker::init_seams();
    backend_postmaster_interrupt::init_seams();
    backend_postmaster_launch_backend::init_seams();
    backend_postmaster_pgarch::init_seams();
    backend_postmaster_startup::init_seams();
    backend_postmaster_syslogger::init_seams();
    backend_postmaster_walsummarizer::init_seams();
    backend_regex_core::init_seams();
    backend_replication_logical_applyparallelworker::init_seams();
    backend_replication_logical_conflict::init_seams();
    backend_replication_logical_launcher::init_seams();
    backend_replication_logical_logical::init_seams();
    backend_replication_logical_origin::init_seams();
    backend_replication_logical_proto::init_seams();
    backend_replication_logical_slotsync::init_seams();
    backend_replication_syncrep_scanner::init_seams();
    backend_replication_slot::init_seams();
    backend_replication_walreceiver::init_seams();
    backend_replication_walreceiverfuncs::init_seams();
    backend_rmgrdesc_next::init_seams();
    backend_rewrite_core::init_seams();
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
    backend_storage_ipc_standby::init_seams();
    backend_storage_large_object::init_seams();
    backend_storage_lmgr_condition_variable::init_seams();
    backend_storage_lmgr_deadlock::init_seams();
    backend_storage_lmgr_lmgr::init_seams();
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
    backend_tcop_fastpath::init_seams();
    backend_timezone_localtime::init_seams();
    backend_timezone_pgtz::init_seams();
    backend_timezone_strftime::init_seams();
    backend_tsearch_ispell_regis::init_seams();
    backend_tsearch_spell::init_seams();
    backend_utils_activity_small::init_seams();
    backend_utils_activity_waitevent::init_seams();
    backend_utils_activity_xact::init_seams();
    backend_utils_adt_misc2::init_seams();
    backend_utils_adt_acl::init_seams();
    backend_utils_adt_array_selfuncs::init_seams();
    backend_utils_adt_arrayfuncs::init_seams();
    backend_utils_adt_arrayutils::init_seams();
    backend_utils_adt_char::init_seams();
    backend_utils_adt_format_type::init_seams();
    backend_utils_adt_geo_ops::init_seams();
    backend_utils_adt_json::init_seams();
    backend_utils_adt_multirangetypes::init_seams();
    backend_utils_adt_numeric::init_seams();
    backend_utils_adt_numutils::init_seams();
    backend_utils_adt_pg_locale_icu::init_seams();
    backend_utils_adt_quote::init_seams();
    backend_utils_adt_range_selfuncs::init_seams();
    backend_utils_adt_rangetypes::init_seams();
    backend_utils_adt_regexp::init_seams();
    backend_utils_adt_scalar_datum_core::init_seams();
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
    backend_utils_fmgr_core::init_seams();
    backend_utils_fmgr_dfmgr::init_seams();
    backend_utils_fmgr_funcapi::init_seams();
    backend_utils_hash_dynahash::init_seams();
    backend_utils_init_miscinit::init_seams();
    backend_utils_init_postinit::init_seams();
    backend_utils_init_small::init_seams();
    backend_utils_mb_conv_string_helpers::init_seams();
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
    backend_utils_sort_sortsupport::init_seams();
    backend_utils_sort_storage::init_seams();
    backend_utils_time_combocid::init_seams();
    backend_utils_time_snapmgr::init_seams();
    common_checksum_helper::init_seams();
    common_hashfn::init_seams();
    common_ip::init_seams();
    common_pglz::init_seams();
    common_prng_base64::init_seams();
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
        // DESIGN_DEBT (TD-CREATE-PARTIAL-BITMAP-PATHS): `create_partial_bitmap_paths`
        // is owned by `optimizer/path/allpaths.c`, NOT costsize.c — but the
        // indxpath.c port declared it in `backend-optimizer-path-costsize-seams`
        // (the nearest path-layer seam crate) since allpaths.c has no crate yet.
        // costsize is COMPLETE, so the guard flags the uninstalled seam; the real
        // owner allpaths.c is unported, so the loud-panic is correct mirror-pg-
        // and-panic. Re-home onto an `allpaths-seams` crate and DELETE this entry
        // when allpaths.c lands.
        ("backend_optimizer_path_costsize", "create_partial_bitmap_paths"),
        // DESIGN_DEBT (TD-GIN-EXTRACT-QUERY): `gin_extract_query` is the GIN
        // `extractQueryFn` fmgr dispatch (`FunctionCall7Coll(...)` with by-pointer
        // out-params) that `ginscan.c`'s `ginNewScanKey` invokes. Its real owner
        // is the fmgr GIN-call dispatcher (still unported) — the SAME owner as the
        // already-uninstalled `gin_extract_value` / `gin_compare_entries` /
        // `gin_consistent_call_{bool,tri}` substrate seams. It is declared in
        // `backend-access-gin-ginutil-seams` (the GIN substrate seam crate, the
        // first cyclic GIN caller) so the guard attributes it to the COMPLETE
        // `ginutil` owner; but ginutil does not call it (ginscan does), so the
        // OUTWARD-seam exclusion that covers the sibling gin substrate seams does
        // not fire. It is genuinely uninstalled / loud-panic (mirror-pg-and-panic)
        // until the fmgr GIN dispatcher lands. DELETE this entry when it does.
        ("backend_access_gin_ginutil", "gin_extract_query"),
        // DESIGN_DEBT (TD-GIN-COMPARE-PARTIAL): `gin_compare_partial` is the GIN
        // `comparePartialFn` fmgr dispatch (`DatumGetInt32(FunctionCall4Coll(...))`)
        // that `ginget.c`'s `collectMatchBitmap` / `matchPartialInPendingList`
        // invoke. Its real owner is the fmgr GIN-call dispatcher (still unported)
        // — the SAME owner as `gin_extract_query` / `gin_extract_value` /
        // `gin_compare_entries`. It is declared in `backend-access-gin-ginutil-seams`
        // (the GIN substrate seam crate) so the guard attributes it to the COMPLETE
        // `ginutil` owner; but ginutil does not call it (ginget does). Genuinely
        // uninstalled / loud-panic (mirror-pg-and-panic) until the fmgr GIN
        // dispatcher lands. DELETE this entry when it does.
        ("backend_access_gin_ginutil", "gin_compare_partial"),
        // DESIGN_DEBT (TD-PATHNODE-JOINRELS-GAP): pathnode.c's
        // `can_create_unique_path` and `install_dummy_append_path` are NOT yet
        // ported in the otherwise-complete `backend-optimizer-util-pathnode`
        // crate; joinrels.c (backend-optimizer-path-joinrels) calls them through
        // the pathnode-seams decls and seam-and-panics until pathnode ports them.
        ("backend_optimizer_util_pathnode", "can_create_unique_path"),
        ("backend_optimizer_util_pathnode", "install_dummy_append_path"),
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
        // DESIGN_DEBT: tableam.c's table-AM dispatch wrappers reach the concrete
        // access method through `rel->rd_tableam` (the TableAmRoutine vtable). The
        // heap AM provider (access/heap/heapam_handler.c) and the vtable resolver
        // (access/table/tableamapi.c, GetTableAmRoutine) are BOTH unported (CATALOG
        // status `todo`: backend-access-heap-heapam-handler / backend-access-small-
        // core), so the owner has no value-typed body to install for the
        // provider-facing seams: get_table_am_routine (tableamapi.c) and the
        // relation_toast_am / relation_needs_toast_table vtable callbacks
        // (heapam_handler.c). table_parallelscan_reinitialize likewise dispatches a
        // vtable callback (relation_parallelscan_reinitialize) with no in-unit body.
        // (table_beginscan / table_scan_getnextslot{,_direction} /
        // table_relation_set_new_filelocator retired: the COPY/seqscan scan model
        // was reconciled onto tableam.c's value-typed `TableScanDesc<'mcx>` and the
        // owner now installs them — the ScanToken divergence is resolved.) Pay down
        // the rest by porting heapam_handler.c + tableamapi.c. See DESIGN_DEBT.md.
        ("backend_access_table_tableam", "get_table_am_routine"),
        ("backend_access_table_tableam", "table_parallelscan_reinitialize"),
        ("backend_access_table_tableam", "table_relation_needs_toast_table"),
        ("backend_access_table_tableam", "table_relation_toast_am"),
        // DESIGN_DEBT (TD-INDEXBUILDSCAN): provider-unported.
        // `table_index_build_scan` (tableam.h) dispatches to the heap AM's
        // `heapam_index_build_range_scan` (heapam_handler.c, still `todo`).
        // hashbuild / hashbuildempty call it; it becomes a real install once
        // heapam_handler.c lands. See DESIGN_DEBT.md.
        ("backend_access_table_tableam", "table_index_build_scan"),
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
        // DESIGN_DEBT (TD-FUNCCMDS-MISHOMED): aclcheck_error_type (aclchk.c) and
        // get_language_oid (proclang.c) are declared in
        // backend-commands-functioncmds-seams because functioncmds was their
        // first consumer; their real owners (aclchk.c / proclang.c) are still
        // unported, so neither is installed. objectaddress's resolution engine
        // (#112) is now a second consumer (TRANSFORM/LANGUAGE arms +
        // check_object_ownership type/transform arms), making them `::call`ed in
        // non-test code while the dir-owner functioncmds is COMPLETE — hence the
        // allowlist. They become real installs when aclchk.c / proclang.c land
        // (or are re-homed to their proper -seams crates).
        ("backend_commands_functioncmds", "aclcheck_error_type"),
        ("backend_commands_functioncmds", "get_language_oid"),
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
        // DESIGN_DEBT: `pg_localtime` is `timezone/localtime.c`'s function but its
        // seam is declared in `backend-timezone-pgtz-seams` (dfmgr/pgtz reach it).
        // It is correctly installed at runtime by backend-timezone-localtime's
        // init_seams() (wired into init_all), so the call path never panics; only
        // the guard's name-prefix attribution flags it because the pgtz owner
        // crate landed and flipped pgtz-seams into "complete owner" status. Pay
        // down by relocating the decl to a backend-timezone-localtime-seams crate.
        ("backend_timezone_pgtz", "pg_localtime"),
        ("backend_utils_adt_acl", "has_bypassrls_privilege"),
        ("backend_utils_adt_acl", "object_ownercheck"),
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
        ("backend_utils_cache_typcache", "domain_check_input"),
        ("backend_utils_fmgr_dfmgr", "load_archive_module_init"),
        ("backend_utils_fmgr_dfmgr", "shmem_request_hook"),
        ("backend_utils_fmgr_dfmgr", "shmem_request_hook_present"),
        // DESIGN_DEBT: provider-unported. `setup_signal_handlers` is the
        // slot-sync worker's `pqsignal(SIGHUP, SignalHandlerForConfigReload)`
        // ... block (slotsync.c:1515-1522). Its handler bodies
        // (SignalHandlerForConfigReload / StatementCancelHandler / die /
        // FloatExceptionHandler / procsignal_sigusr1_handler) live in
        // interrupt.c / postgres.c / procsignal.c, none of which is ported, so
        // there is no real body to install. (The other 8 slot-sync bootstrap
        // seams declared alongside it ARE installed in miscinit's init_seams by
        // delegating to their now-ported owners.)
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
        ("backend_utils_mmgr_portalmem", "copy_param_list_into_portal"),
        // DESIGN_DEBT (TD-PORTAL-COPYIN): the deep-copy-into-portal-context seams
        // copy foreign objects (param lists / tuple descriptors / planned stmts)
        // into the portal's `'static`-lifetime owned arenas. That copy
        // infrastructure (copyParamList / CreateTupleDescCopy into a portal/hold
        // context that outlives the source transaction) lands with the
        // tuplestore/tupdesc copy owners; until then these stay seam-and-panic
        // (matching the pre-port `todo` state) rather than being wrongly stubbed.
        ("backend_utils_mmgr_portalmem", "copy_tup_desc_into_hold_context"),
        // TD-PORTAL-HANDLE, see the handle-divergent note above.
        ("backend_utils_mmgr_portalmem", "create_new_portal"),
        ("backend_utils_mmgr_portalmem", "portal_define_query"),
        // TD-PORTAL-COPYIN, see the deep-copy note above.
        ("backend_utils_mmgr_portalmem", "portal_define_query_select"),
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
        // DESIGN_DEBT (TD-XLOGRECOVERY-PAGEREAD): the recovery driver's
        // `ReadRecord` retry loop (xlogrecovery #13 F1, readrecord.rs) reaches the
        // WAL page-read driver solely through the prefetcher read-record seams,
        // and inspects the decoded record through the `RecordRef`-keyed
        // `xlog_rec_*` accessors. These five seams are declared but NOT installed:
        //   * prefetcher_begin_read / prefetcher_read_record — declared in
        //     backend-access-transam-xlogprefetcher-seams with the explicit note
        //     "NOT installed: the page-read driver is not yet ported." The
        //     "merged" xlogprefetcher unit ported only the prefetch-STATS shmem
        //     (XLogPrefetchShmemSize/Init); the recovery read-record entry points
        //     wrap the genuinely-unported hard-core WAL file I/O (XLogPageRead /
        //     WaitForWALToBecomeAvailable / XLogFileRead{,AnyTLI} + restore_command
        //     fetching), so they stay seam-and-panic until that driver lands.
        //   * xlog_rec_rmid / xlog_rec_info / xlog_rec_total_len — declared in
        //     xlogreader-seams keyed by the opaque `RecordRef(u64)` handle, but the
        //     merged xlogreader models the record as a borrowed `&XLogReaderState`,
        //     not a handle registry. The handle->reader mapping is owned by that
        //     same unported page-read driver, so xlogreader cannot install them
        //     without a forbidden token registry / a contract redesign.
        // All five become real installs when the page-read driver (xlogprefetcher
        // recovery leg) lands. See DESIGN_DEBT.md.
        ("backend_access_transam_xlogprefetcher", "prefetcher_begin_read"),
        ("backend_access_transam_xlogprefetcher", "prefetcher_read_record"),
        ("backend_access_transam_xlogreader", "xlog_rec_info"),
        ("backend_access_transam_xlogreader", "xlog_rec_rmid"),
        ("backend_access_transam_xlogreader", "xlog_rec_total_len"),
        // DESIGN_DEBT (TD-PARSETYPE-RAWGRAMMAR): parse_type.c's
        // `typeStringToTypeName` drives `raw_parser(str, RAW_PARSE_TYPE_NAME)`
        // and extracts the single `TypeName` node. The owner of `raw_parser`
        // (backend-parser-driver, audited) cannot install this seam yet because
        // the bison grammar it drives (`base_yyparse`, gram.y) is not ported —
        // any raw-parse call reaches the still-unported grammar and panics
        // (mirror-pg-and-panic). Becomes a real install once gram.y lands and
        // the driver can convert its `RAW_PARSE_TYPE_NAME` output to a
        // `types_parsenodes::TypeName`. See DESIGN_DEBT.md.
        ("backend_parser_driver", "raw_parse_type_name"),
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
            if cols.len() < 4 {
                continue;
            }
            let status = cols[2].trim();
            let crate_col = cols[3];
            if !is_complete_status(status) {
                continue;
            }
            for c in crate_col.split(|ch| ch == '+' || ch == ',') {
                let c = c.trim();
                if !c.is_empty() {
                    complete.insert(c.to_string());
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
                // Skip to the first '{' and drop the balanced block.
                let mut j = i + needle.len();
                while j < bytes.len() && bytes[j] as char != '{' {
                    j += 1;
                }
                if j >= bytes.len() {
                    break;
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

    /// Parse `ident::ident::call(` triples out of one source string, resolving
    /// the leading crate ident through `aliases` (`use ... as <alias>;`).
    fn collect_call_sites(
        src: &str,
        aliases: &std::collections::HashMap<String, String>,
        out: &mut std::collections::HashSet<(String, String)>,
    ) {
        let bytes = src.as_bytes();
        let needle = b"::call";
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
                let installed = has_init_seams
                    && (owner_src.contains(&pat1) || owner_src.contains(&pat2));
                if installed {
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
