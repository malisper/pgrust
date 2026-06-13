//! Startup aggregator: calls every ported crate's `init_seams()`.
//!
//! This crate contains NO logic and NO `set()` calls of its own — one line
//! per ported crate, nothing else. Each crate wires its own seams in its own
//! `init_seams()`; this is just the place that invokes them all.

pub fn init_all() {
    // One line per ported crate, kept sorted:
    backend_access_common_detoast::init_seams();
    backend_access_common_heaptuple::init_seams();
    backend_access_common_relation::init_seams();
    backend_access_common_reloptions::init_seams();
    backend_access_common_tupdesc::init_seams();
    backend_access_hashvalidate::init_seams();
    backend_access_heap_heaptoast::init_seams();
    backend_access_heap_vacuumlazy::init_seams();
    backend_access_index_indexam::init_seams();
    backend_access_nbtree_nbtree::init_seams();
    backend_access_rmgrdesc_small::init_seams();
    backend_access_rmgrdesc_xactdesc::init_seams();
    backend_access_table_table::init_seams();
    backend_access_table_tableam::init_seams();
    backend_access_transam_generic_xlog::init_seams();
    backend_access_transam_parallel::init_seams();
    backend_access_transam_timeline::init_seams();
    backend_access_transam_transam::init_seams();
    backend_access_transam_twophase::init_seams();
    backend_access_transam_xact::init_seams();
    backend_access_transam_xlog::init_seams();
    backend_access_transam_xlogprefetcher::init_seams();
    backend_access_transam_xlogstats::init_seams();
    backend_access_transam_xlogutils::init_seams();
    backend_catalog_catalog::init_seams();
    backend_catalog_namespace::init_seams();
    backend_catalog_objectaccess::init_seams();
    backend_catalog_pg_depend::init_seams();
    backend_catalog_pg_shdepend::init_seams();
    backend_commands_cluster::init_seams();
    backend_commands_copyto::init_seams();
    backend_commands_foreigncmds::init_seams();
    backend_commands_matview::init_seams();
    backend_commands_portalcmds::init_seams();
    backend_executor_execAmi::init_seams();
    backend_executor_execParallel::init_seams();
    backend_executor_execPartition::init_seams();
    backend_executor_execUtils::init_seams();
    backend_executor_instrument::init_seams();
    backend_executor_nodeAgg::init_seams();
    backend_executor_nodeAppend::init_seams();
    backend_executor_nodeBitmapHeapscan::init_seams();
    backend_executor_nodeForeignscan::init_seams();
    backend_executor_nodeHash::init_seams();
    backend_executor_nodeHashjoin::init_seams();
    backend_executor_nodeIndexonlyscan::init_seams();
    backend_executor_nodeMaterial::init_seams();
    backend_executor_nodeMemoize::init_seams();
    backend_executor_nodeMergejoin::init_seams();
    backend_executor_nodeModifyTable::init_seams();
    backend_executor_nodeSeqscan::init_seams();
    backend_executor_nodeSort::init_seams();
    backend_executor_nodeSubplan::init_seams();
    backend_lib_dshash::init_seams();
    backend_libpq_pqcomm::init_seams();
    backend_libpq_pqformat::init_seams();
    backend_libpq_pqsignal::init_seams();
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
    backend_replication_slot::init_seams();
    backend_replication_walreceiver::init_seams();
    backend_rmgrdesc_next::init_seams();
    backend_storage_file_buffile::init_seams();
    backend_storage_file_fd::init_seams();
    backend_storage_ipc::init_seams();
    backend_storage_ipc_dsm_core::init_seams();
    backend_storage_ipc_latch::init_seams();
    backend_storage_ipc_procsignal::init_seams();
    backend_storage_ipc_shm_mq::init_seams();
    backend_storage_ipc_shm_toc::init_seams();
    backend_storage_ipc_shmem::init_seams();
    backend_storage_ipc_standby::init_seams();
    backend_storage_lmgr_condition_variable::init_seams();
    backend_storage_lmgr_deadlock::init_seams();
    backend_storage_lmgr_lmgr::init_seams();
    backend_storage_lmgr_lwlock::init_seams();
    backend_storage_lmgr_s_lock::init_seams();
    backend_storage_page_checksum::init_seams();
    backend_storage_sync::init_seams();
    backend_tcop_backend_startup::init_seams();
    backend_timezone_localtime::init_seams();
    backend_timezone_strftime::init_seams();
    backend_tsearch_ispell_regis::init_seams();
    backend_tsearch_spell::init_seams();
    backend_utils_activity_small::init_seams();
    backend_utils_activity_xact::init_seams();
    backend_utils_adt_acl::init_seams();
    backend_utils_adt_arrayfuncs::init_seams();
    backend_utils_adt_format_type::init_seams();
    backend_utils_adt_multirangetypes::init_seams();
    backend_utils_adt_numutils::init_seams();
    backend_utils_adt_pg_locale_icu::init_seams();
    backend_utils_adt_range_selfuncs::init_seams();
    backend_utils_adt_rangetypes::init_seams();
    backend_utils_adt_regexp::init_seams();
    backend_utils_adt_ri_triggers::init_seams();
    backend_utils_cache_attoptcache::init_seams();
    backend_utils_cache_inval::init_seams();
    backend_utils_cache_relfilenumbermap::init_seams();
    backend_utils_cache_relmapper::init_seams();
    backend_utils_cache_spccache::init_seams();
    backend_utils_cache_syscache::init_seams();
    backend_utils_cache_ts_cache::init_seams();
    backend_utils_cache_typcache::init_seams();
    backend_utils_error::init_seams();
    backend_utils_fmgr_core::init_seams();
    backend_utils_fmgr_dfmgr::init_seams();
    backend_utils_init_miscinit::init_seams();
    backend_utils_init_postinit::init_seams();
    backend_utils_init_small::init_seams();
    backend_utils_mb_wstrcmp::init_seams();
    backend_utils_mb_wstrncmp::init_seams();
    backend_utils_misc_guc_file::init_seams();
    backend_utils_misc_more::init_seams();
    backend_utils_misc_pg_rusage::init_seams();
    backend_utils_misc_queryenvironment::init_seams();
    backend_utils_misc_sampling::init_seams();
    backend_utils_misc_timeout::init_seams();
    backend_utils_mmgr_freepage::init_seams();
    backend_utils_mmgr_portalmem::init_seams();
    backend_utils_sort_sortsupport::init_seams();
    backend_utils_time_combocid::init_seams();
    backend_utils_time_snapmgr::init_seams();
    common_ip::init_seams();
    interfaces_libpq_legacy_pqsignal::init_seams();
    port_crc32c::init_seams();
    port_pgsleep::init_seams();
    port_pqsignal::init_seams();
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

            let mut set_count = 0usize;
            for f in &files {
                let txt = fs::read_to_string(f).unwrap_or_default();
                if let Some(body) = init_seams_body(&txt) {
                    set_count += body.matches("::set(").count()
                        + body.matches("::set (").count();
                }
            }
            if set_count == 0 {
                continue; // empty or no init_seams -> nothing to wire
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
}
