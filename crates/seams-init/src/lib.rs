//! Startup aggregator: calls every ported crate's `init_seams()`.
//!
//! This crate contains NO logic and NO `set()` calls of its own — one line
//! per ported crate, nothing else. Each crate wires its own seams in its own
//! `init_seams()`; this is just the place that invokes them all.

pub fn init_all() {
    // One line per ported crate, kept sorted:
    backend_access_common_detoast::init_seams();
    backend_access_common_heaptuple::init_seams();
    backend_access_common_indextuple::init_seams();
    backend_access_common_relation::init_seams();
    backend_access_common_reloptions::init_seams();
    backend_access_common_tidstore::init_seams();
    backend_access_common_tupdesc::init_seams();
    backend_access_gin_core_probe::init_seams();
    backend_access_hashvalidate::init_seams();
    backend_access_heap_heaptoast::init_seams();
    backend_access_heap_vacuumlazy::init_seams();
    backend_access_index_indexam::init_seams();
    backend_access_nbt_dedup::init_seams();
    backend_access_nbtree_nbtree::init_seams();
    backend_access_rmgrdesc_small::init_seams();
    backend_access_rmgrdesc_xactdesc::init_seams();
    backend_access_table_table::init_seams();
    backend_access_table_tableam::init_seams();
    backend_access_transam_clog::init_seams();
    backend_access_transam_commit_ts::init_seams();
    backend_access_transam_generic_xlog::init_seams();
    backend_access_transam_parallel::init_seams();
    backend_access_transam_subtrans::init_seams();
    backend_access_transam_timeline::init_seams();
    backend_access_transam_transam::init_seams();
    backend_access_transam_twophase::init_seams();
    backend_access_transam_varsup::init_seams();
    backend_access_transam_xact::init_seams();
    backend_access_transam_xlog::init_seams();
    backend_access_transam_xlogprefetcher::init_seams();
    backend_access_transam_xlogstats::init_seams();
    backend_access_transam_xlogutils::init_seams();
    backend_backup_server::init_seams();
    backend_backup_sink::init_seams();
    backend_bootstrap_bootstrap::init_seams();
    backend_catalog_catalog::init_seams();
    backend_catalog_namespace::init_seams();
    backend_catalog_objectaccess::init_seams();
    backend_catalog_pg_class::init_seams();
    backend_catalog_pg_db_role_setting::init_seams();
    backend_catalog_pg_constraint::init_seams();
    backend_catalog_pg_depend::init_seams();
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
    backend_commands_foreigncmds::init_seams();
    backend_commands_matview::init_seams();
    backend_commands_portalcmds::init_seams();
    backend_executor_execAmi::init_seams();
    backend_executor_execCurrent::init_seams();
    backend_executor_execExpr::init_seams();
    backend_executor_execExprInterp::init_seams();
    backend_executor_execJunk::init_seams();
    backend_executor_execParallel::init_seams();
    backend_executor_execPartition::init_seams();
    backend_executor_execProcnode::init_seams();
    backend_executor_execScan::init_seams();
    backend_executor_execTuples::init_seams();
    backend_executor_execUtils::init_seams();
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
    backend_executor_nodeGatherMerge::init_seams();
    backend_executor_nodeGroup::init_seams();
    backend_executor_nodeHash::init_seams();
    backend_executor_nodeHashjoin::init_seams();
    backend_executor_nodeBitmapIndexscan::init_seams();
    backend_executor_nodeIndexonlyscan::init_seams();
    backend_executor_nodeLimit::init_seams();
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
    backend_lib_hyperloglog::init_seams();
    backend_main_main::init_seams();
    backend_libpq_pqcomm::init_seams();
    backend_libpq_pqformat::init_seams();
    backend_libpq_pqsignal::init_seams();
    backend_nodes_copyfuncs::init_seams();
    backend_nodes_core::init_seams();
    backend_nodes_extensible::init_seams();
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
    backend_storage_file_buffile::init_seams();
    backend_storage_file_fd::init_seams();
    backend_storage_file_fileset::init_seams();
    backend_storage_freespace::init_seams();
    backend_storage_ipc::init_seams();
    backend_storage_ipc_dsm_core::init_seams();
    backend_storage_ipc_dsm_registry::init_seams();
    backend_storage_ipc_latch::init_seams();
    backend_storage_ipc_pmsignal::init_seams();
    backend_storage_ipc_procsignal::init_seams();
    backend_storage_ipc_shm_mq::init_seams();
    backend_storage_ipc_shm_toc::init_seams();
    backend_storage_ipc_shmem::init_seams();
    backend_storage_ipc_sinval::init_seams();
    backend_storage_ipc_standby::init_seams();
    backend_storage_lmgr_condition_variable::init_seams();
    backend_storage_lmgr_deadlock::init_seams();
    backend_storage_lmgr_lmgr::init_seams();
    backend_storage_lmgr_proc::init_seams();
    backend_storage_lmgr_lwlock::init_seams();
    backend_storage_lmgr_s_lock::init_seams();
    backend_storage_page::init_seams();
    backend_storage_page_checksum::init_seams();
    backend_storage_sync::init_seams();
    backend_tcop_backend_startup::init_seams();
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
        ("backend_access_common_reloptions", "index_build_local_reloptions"),
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
        ("backend_access_heap_heaptoast", "heap_tuple_header_get_datum"),
        // DESIGN_DEBT: indexam scan seams diverge on the scan-descriptor model.
        // The seam decls (backend-access-index-indexam-seams) are written against
        // a node-driven model — `types_nodes::IndexScanDescData`/`ParallelIndex-
        // ScanDescData`, `Rc<SnapshotData>`, `SlotId`+`EStateData` heap-fetch, and
        // node-state rescan (`IndexOnlyScanState`/`BitmapIndexScanState`) — and the
        // live consumers (nodeIndexonlyscan, nodeBitmapIndexscan, nbtree) call them
        // with those types. The owner crate faithfully ported indexam.c against the
        // C-faithful `types_tableam::relscan::IndexScanDescData` model (by-value
        // `SnapshotData`, `&mut TupleTableSlot`, scan-key slices) and dispatches
        // through the `IndexAmRoutine` vtable typed on that struct. These are two
        // independent scan-descriptor structs in different crates (plus nbtree's own
        // `NbtScan` view), never reconciled — no thin adapter can forward between
        // them. Paying this down is a contract redesign unifying the index-AM
        // scan-descriptor model across indexam/genam/nodes/nbtree, not seam wiring.
        // See DESIGN_DEBT.md.
        ("backend_access_index_indexam", "bt_resolve_parallel_scan"),
        ("backend_access_index_indexam", "index_beginscan"),
        ("backend_access_index_indexam", "index_beginscan_bitmap"),
        ("backend_access_index_indexam", "index_beginscan_parallel"),
        ("backend_access_index_indexam", "index_endscan"),
        ("backend_access_index_indexam", "index_fetch_heap"),
        ("backend_access_index_indexam", "index_getbitmap"),
        ("backend_access_index_indexam", "index_getnext_tid"),
        ("backend_access_index_indexam", "index_markpos"),
        ("backend_access_index_indexam", "index_parallelrescan"),
        ("backend_access_index_indexam", "index_parallelscan_estimate"),
        ("backend_access_index_indexam", "index_parallelscan_initialize"),
        ("backend_access_index_indexam", "index_rescan"),
        ("backend_access_index_indexam", "index_rescan_bis"),
        ("backend_access_index_indexam", "index_restrpos"),
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
        // Separately, the COPY/seqscan scan seams model the AM-owned scan state as
        // an opaque `ScanToken(u64)`, but tableam.c was ported with the C-faithful
        // value-typed `TableScanDesc<'mcx>` (no ScanToken->descriptor registry
        // exists, and inventing one would forge opacity). So the ScanToken-shaped
        // table_beginscan / table_scan_getnextslot{,_direction} and
        // table_relation_set_new_filelocator have no matching body either. The
        // bitmap-scan table_endscan / table_rescan are VALUE-typed (bm-seams) and
        // DO match the ported bodies — those ARE installed (not listed here). Pay
        // down by porting heapam_handler.c + tableamapi.c (the provider seams) and
        // unifying the COPY/seqscan scan model onto the value descriptor (the
        // ScanToken seams). See DESIGN_DEBT.md.
        ("backend_access_table_tableam", "get_table_am_routine"),
        ("backend_access_table_tableam", "table_beginscan"),
        ("backend_access_table_tableam", "table_parallelscan_reinitialize"),
        ("backend_access_table_tableam", "table_relation_needs_toast_table"),
        ("backend_access_table_tableam", "table_relation_set_new_filelocator"),
        ("backend_access_table_tableam", "table_relation_toast_am"),
        ("backend_access_table_tableam", "table_scan_getnextslot"),
        ("backend_access_table_tableam", "table_scan_getnextslot_direction"),
        // DESIGN_DEBT (TD-GETDATABASEPATH): provider-unported. `GetDatabasePath`
        // is `common/relpath.c`'s function, not catalog.c's — the seam was
        // mis-homed onto backend-catalog-catalog-seams (this owner's stable
        // contract, `(db_oid, spc_oid) -> PgResult<String>`, owned String, no
        // mcx). Its genuine owner crate `backend-common-relpath` (relpath.c) is
        // unported; the canonical seam already has a value-shaped home in
        // `backend-common-relpath-seams` (mcx -> PgString). Installing the path
        // arithmetic here would re-home relpath.c's logic into the wrong TU
        // (and the two seam contracts diverge: owned String vs Mcx/PgString).
        // Install once relpath.c lands as its own owner; consumers (inval.c
        // at_eoxact, relmapper relmap_redo) then move to the relpath seam.
        ("backend_catalog_catalog", "get_database_path"),
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
        // DESIGN_DEBT: RestrictSearchPath() is guc.c's function (mis-homed onto
        // backend-catalog-namespace-seams). Its body is solely
        // set_config_option("search_path", GUC_SAFE_SEARCH_PATH, PGC_USERSET,
        // PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false). It is blocked on the
        // unported GUC owner (backend-utils-misc-guc): set_config_option itself
        // is declared-but-uninstalled, and the existing seam lacks the
        // GUC_ACTION_SAVE/changeVal/elevel parameters this call needs, plus the
        // GUC_SAFE_SEARCH_PATH constant is unported. Install once guc lands.
        ("backend_catalog_namespace", "restrict_search_path"),
        ("backend_catalog_namespace", "search_path_matches_current_environment"),
        // xlog reconciled out: CATALOG status corrected merged->needs-decomp
        // (chore/xlog-catalog-honest, task #111). An incomplete owner legitimately
        // seam-and-panics its unported surface (mirror-pg-and-panic), so the guard
        // no longer flags it (condition (b) false) — these entries went stale.
        // DESIGN_DEBT (TD-GUC-UNPORTED): these guc.c/guc_funcs.c functions are
        // mis-homed onto backend-commands-functioncmds-seams because functioncmds
        // was the first consumer; their decls' dir-owner resolves to functioncmds,
        // which does NOT install them.
        //   * extract_set_variable_args (guc_funcs.c, ExtractSetVariableArgs) is
        //     now genuinely INSTALLED by its real owner backend-utils-misc-guc-funcs
        //     (task #163 W2) via that crate's init_seams() — a cross-crate re-home
        //     install the dir-owner guard cannot see, so the allowlist entry stays
        //     to suppress the false "functioncmds didn't install it" flag.
        //   * GUCArrayAdd/Delete/Reset (guc.c) still have NO impl: the GUC array
        //     helpers live in guc.c, not yet ported. Consumed by functioncmds
        //     (ddl_core) + pg-db-role-setting.
        ("backend_commands_functioncmds", "extract_set_variable_args"),
        ("backend_commands_functioncmds", "guc_array_add"),
        ("backend_commands_functioncmds", "guc_array_delete"),
        ("backend_commands_functioncmds", "guc_array_reset"),
        // DESIGN_DEBT (TD-EXECEXPR-PARAMSETEQ): `exec_build_param_set_equal`'s seam
        // decl (backend-executor-execExpr-seams) still carries the pre-owned-model
        // shape — trailing `parent: &mut PlanStateData` + `estate: &mut EStateData`
        // and NO `mcx` — and nodeMemoize calls it with that shape, while the owner's
        // real ExecBuildParamSetEqual body follows the crate's owned model
        // (`mcx`-first, result `desc`/`ops` passed directly, no parent/estate — the
        // same reconciliation the installed sibling `exec_build_hash32_expr` already
        // received). Installing it requires reconciling decl + call-site onto the
        // owned shape, which is the executor de-handle / contract-reconcile work
        // (#112, #167/#169), not pure seam wiring. Until then it stays
        // declared-but-uninstalled (would panic only on the nodeMemoize non-binary
        // key-equality path). See DESIGN_DEBT.md.
        ("backend_executor_execExpr", "exec_build_param_set_equal"),
        // DESIGN_DEBT: the `_owned` variants take owned `&mut PlanStateNode` /
        // `&mut EStateData` trees, but the owner's real ExecInitParallelPlan /
        // ExecParallelReinitialize bodies (and every `sup::*::call`) operate over
        // handle-space (PlanStateHandle / EStateHandle opaque usize newtypes).
        // Bridging owned-tree -> handle requires a parallel-planstate registry
        // that does not exist yet (the seam doc comment names it explicitly), or a
        // crate-wide rewrite of the handle-based body onto owned trees. Blocked on
        // that registry / contract redesign — do not force-wire a fake handle.
        ("backend_executor_execParallel", "exec_init_parallel_plan_owned"),
        ("backend_executor_execParallel", "exec_parallel_reinitialize_owned"),
        ("backend_executor_execPartition", "exec_cleanup_tuple_routing"),
        ("backend_executor_execPartition", "exec_find_partition"),
        ("backend_executor_execPartition", "exec_setup_partition_tuple_routing"),
        ("backend_executor_execProcnode", "clear_param_execplan"),
        ("backend_executor_execProcnode", "exec_set_param_plan_for_pending"),
        ("backend_executor_execProcnode", "link_subplan_planstate"),
        ("backend_executor_execProcnode", "mark_param_execplan_pending"),
        ("backend_executor_execProcnode", "param_execplan_pending"),
        ("backend_executor_execTuples", "cur_tuple_getattr"),
        ("backend_executor_execTuples", "exec_copy_slot_minimal_tuple"),
        ("backend_executor_execTuples", "exec_fetch_slot_minimal_tuple"),
        ("backend_executor_execTuples", "exec_fetch_slot_minimal_tuple_copy"),
        ("backend_executor_execTuples", "exec_force_store_heap_tuple"),
        ("backend_executor_execTuples", "exec_force_store_minimal_tuple"),
        ("backend_executor_execTuples", "exec_materialize_slot"),
        ("backend_executor_execTuples", "exec_scan_slot_descriptor"),
        ("backend_executor_execTuples", "exec_store_first_datum"),
        ("backend_executor_execTuples", "exec_store_generated_columns"),
        ("backend_executor_execTuples", "exec_store_minimal_tuple"),
        ("backend_executor_execTuples", "exec_store_virtual_tuple"),
        ("backend_executor_execTuples", "execute_attr_map_slot"),
        ("backend_executor_execTuples", "execute_attr_map_slot_explicit"),
        ("backend_executor_execTuples", "pad_name_cstring_columns"),
        ("backend_executor_execTuples", "replace_cur_tuple_from_slot"),
        ("backend_executor_execTuples", "slot_getattr"),
        ("backend_executor_execTuples", "slot_getattr_by_id"),
        ("backend_executor_execTuples", "slot_getsomeattr"),
        ("backend_executor_execTuples", "slot_natts"),
        // backend-foreign-foreign owns foreign/foreign.c's READ accessors + the
        // FDW-routine resolution, which it installs. The remaining seams in
        // backend-foreign-foreign-seams are name-attributed to this owner but are
        // NOT foreign.c functions — they belong to two other unported domains and
        // cannot be installed here without faking (opacity-inherited-never-introduced).
        // (1) pg_foreign_* catalog DML + options decode + IMPORT + the dynamic
        //     validator dispatch: these are commands/foreigncmds.c machinery
        //     (heap_form_tuple + CatalogTupleInsert/Update, SearchSysCacheCopy1,
        //     GetNewOidWithIndex, SysCacheGetAttr decode, aclnewowner,
        //     OidFunctionCall2(fdwvalidator), pg_parse_query RawStmt projection),
        //     all needing the pg_foreign_* catalog-write substrate that is unported.
        // (2) FDW-provider callbacks (node->fdwroutine->X) dispatch through a
        //     runtime FDW vtable; no FDW provider (postgres_fdw/contrib) is ported,
        //     so there is nothing to install. See DESIGN_DEBT.md.
        ("backend_foreign_foreign", "begin_direct_modify"),
        ("backend_foreign_foreign", "begin_foreign_scan"),
        ("backend_foreign_foreign", "end_direct_modify"),
        ("backend_foreign_foreign", "end_foreign_scan"),
        ("backend_foreign_foreign", "estimate_dsm_foreign_scan"),
        ("backend_foreign_foreign", "fdw_import_foreign_schema"),
        ("backend_foreign_foreign", "fdw_lookup_by_name"),
        ("backend_foreign_foreign", "fdw_options"),
        ("backend_foreign_foreign", "fdw_owner_row_by_name"),
        ("backend_foreign_foreign", "fdw_owner_row_by_oid"),
        ("backend_foreign_foreign", "fdw_set_owner"),
        ("backend_foreign_foreign", "foreign_async_configure_wait"),
        ("backend_foreign_foreign", "foreign_async_notify"),
        ("backend_foreign_foreign", "foreign_async_request"),
        ("backend_foreign_foreign", "import_classify_raw_stmt"),
        ("backend_foreign_foreign", "import_set_schemaname"),
        ("backend_foreign_foreign", "initialize_dsm_foreign_scan"),
        ("backend_foreign_foreign", "initialize_worker_foreign_scan"),
        ("backend_foreign_foreign", "insert_fdw"),
        ("backend_foreign_foreign", "insert_foreign_table"),
        ("backend_foreign_foreign", "insert_server"),
        ("backend_foreign_foreign", "insert_usermapping"),
        ("backend_foreign_foreign", "iterate_direct_modify"),
        ("backend_foreign_foreign", "iterate_foreign_scan"),
        ("backend_foreign_foreign", "recheck_foreign_scan"),
        ("backend_foreign_foreign", "reinitialize_dsm_foreign_scan"),
        ("backend_foreign_foreign", "rescan_foreign_scan"),
        ("backend_foreign_foreign", "server_lookup_by_name"),
        ("backend_foreign_foreign", "server_options"),
        ("backend_foreign_foreign", "server_owner_row_by_name"),
        ("backend_foreign_foreign", "server_owner_row_by_oid"),
        ("backend_foreign_foreign", "server_set_owner"),
        ("backend_foreign_foreign", "shutdown_foreign_scan"),
        ("backend_foreign_foreign", "stamp_scan_slot_tableoid"),
        ("backend_foreign_foreign", "update_fdw"),
        ("backend_foreign_foreign", "update_server"),
        ("backend_foreign_foreign", "update_usermapping"),
        ("backend_foreign_foreign", "usermapping_oid"),
        ("backend_foreign_foreign", "usermapping_options"),
        ("backend_foreign_foreign", "validate_options"),
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
        ("backend_executor_execTuples", "store_virtual_values"),
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
        ("backend_postmaster_interrupt", "install_crash_exit_sigquit_handler"),
        ("backend_postmaster_interrupt", "pqinitmask_set_blocksig"),
        // DESIGN_DEBT (TD-LATCH-PROC-BRIDGE): the three SetLatch-by-proc seams
        // resolve another backend's PGPROC-embedded `procLatch` to a
        // `LatchHandle` and set it. The owner (backend-storage-ipc-latch) names
        // a latch by its position in this crate's own append-only `LATCHES`
        // registry (`lookup_latch` panics on any handle not minted by
        // `allocate_latch`). proc.c's `proc_latch` seam DOES return a handle,
        // but it mints it from the proc number (`proc_latch_handle(procno)`),
        // which is a DIFFERENT, unregistered handle space — `SetLatch` on it
        // panics "invalid LatchHandle". Unifying the two (registering each
        // PGPROC's `procLatch` into the latch registry, or sharing one handle
        // space) is the latch<->proc PGPROC-latch integration bridge that does
        // not exist yet; proc.c's own `set_proc_latch` already aborts loudly on
        // exactly this boundary. `set_latch_for_proc_pid` is additionally
        // blocked on procarray.c (unported, status=todo) for the PID->proc
        // lookup (`BackendPidGetProc`). Install once that bridge lands; do not
        // force-wire a handle from the wrong space.
        ("backend_storage_ipc_latch", "set_latch_by_proc_number"),
        ("backend_storage_ipc_latch", "set_latch_for_proc_pid"),
        ("backend_storage_ipc_latch", "set_latch_for_procno"),
        // DESIGN_DEBT (provider-unported): `xlog_request_wal_receiver_reply` is
        // declared on backend-replication-walreceiverfuncs-seams but its real
        // body is `XLogRequestWalReceiverReply()` in xlogrecovery.c, NOT
        // walreceiverfuncs.c. The walreceiverfuncs owner documents this and
        // deliberately does not install it. The true owner crate
        // (backend-access-transam-xlogrecovery) is unported — only its empty
        // -seams crate exists — so there is no real impl to ::set yet. Consumed
        // by xact redo (backend-access-transam-xact). Install from xlogrecovery's
        // init_seams once that owner lands.
        ("backend_replication_walreceiverfuncs", "xlog_request_wal_receiver_reply"),
        ("backend_storage_ipc_pmsignal", "set_postmaster_death_watch_cloexec"),
        // DESIGN_DEBT: the `backend-storage-ipc-shm-toc-seams` facade declares
        // `shm_toc_estimate_{chunk,keys}` keyed on `&mut types_nodes::ParallelContext`,
        // but that owned `ParallelContext` is the TRIMMED model — it carries only an
        // opaque `toc: Opaque` and has NO real `estimator: shm_toc_estimator` field
        // (it is "storage-owned, opaque here"). The real estimator lives in
        // `backend-access-transam-parallel`'s own context store, addressed by
        // `ShmTocEstimatorHandle`, and the genuine estimate logic IS installed there
        // via the handle-keyed `backend_access_transam_parallel_seams` facade
        // (delegating to `backend_storage_ipc_shm_toc::shm_toc_estimate_{chunk,keys}`).
        // The shm-toc owner cannot install this `&mut ParallelContext` facade with real
        // logic: there is no in-struct estimator to operate on, and synthesizing one
        // would diverge from the handle-store model. Pay down with the ParallelContext
        // de-handle keystone (give the owned ParallelContext a real `estimator` field),
        // after which the owner installs these directly. Provider-unported / K-gated.
        ("backend_storage_ipc_shm_toc", "shm_toc_estimate_chunk"),
        ("backend_storage_ipc_shm_toc", "shm_toc_estimate_keys"),
        // DESIGN_DEBT: these 25 proc.c seams are declared + consumed but the owner
        // (backend-storage-lmgr-proc, audited) has no impl for them — they need the
        // cross-unit PGPROC/ProcGlobal-arena wiring (procarray add/remove + clog.c
        // TransactionGroupUpdateXidStatus group-commit machinery + lock.c fast-path
        // locks) that has not landed yet. The clog_group_* / *_clog_group_* set is
        // the XID-status group-update batch (clog.c clogGroupFirst CAS list + the
        // per-PGPROC clogGroupMember/Next/MemberXid/Page/Status fields); the
        // my_proc_{xmin,xid,vxid,subxids}/proc_subxids/store_{top,sub}xid_in_proc
        // accessors read/write live PGPROC xact state that procarray owns. Pay down
        // when procarray (task #121) + clog group-update land. See DESIGN_DEBT.md.
        ("backend_storage_lmgr_proc", "clog_group_first_compare_exchange"),
        ("backend_storage_lmgr_proc", "clog_group_first_exchange"),
        ("backend_storage_lmgr_proc", "clog_group_first_read"),
        ("backend_storage_lmgr_proc", "init_proc_global"),
        ("backend_storage_lmgr_proc", "initialize_fast_path_locks"),
        ("backend_storage_lmgr_proc", "my_proc_clog_group_member"),
        ("backend_storage_lmgr_proc", "my_proc_clog_group_next"),
        ("backend_storage_lmgr_proc", "my_proc_subxids"),
        ("backend_storage_lmgr_proc", "my_proc_vxid"),
        ("backend_storage_lmgr_proc", "my_proc_xid"),
        ("backend_storage_lmgr_proc", "my_proc_xmin"),
        ("backend_storage_lmgr_proc", "proc_clog_group_member_page"),
        ("backend_storage_lmgr_proc", "proc_clog_group_member_update"),
        ("backend_storage_lmgr_proc", "proc_clog_group_next"),
        ("backend_storage_lmgr_proc", "proc_global_semas"),
        ("backend_storage_lmgr_proc", "proc_global_shmem_size"),
        ("backend_storage_lmgr_proc", "proc_subxids"),
        ("backend_storage_lmgr_proc", "set_my_proc_clog_group_member"),
        ("backend_storage_lmgr_proc", "set_my_proc_clog_group_member_data"),
        ("backend_storage_lmgr_proc", "set_my_proc_clog_group_next"),
        ("backend_storage_lmgr_proc", "set_my_proc_xmin"),
        ("backend_storage_lmgr_proc", "set_proc_clog_group_member"),
        ("backend_storage_lmgr_proc", "set_proc_clog_group_next"),
        ("backend_storage_lmgr_proc", "store_subxid_in_proc"),
        ("backend_storage_lmgr_proc", "store_top_xid_in_proc"),
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
        ("backend_utils_adt_misc2", "make_expanded_object_read_only_internal_v"),
        // DESIGN_DEBT: the generic range I/O procs `range_in`/`range_out`/
        // `range_recv`/`range_send` (rangetypes.c) parse/render a range by
        // calling the *element* subtype's I/O proc through the fmgr Datum lane
        // (InputFunctionCallSafe / OutputFunctionCall / ReceiveFunctionCall /
        // SendFunctionCall on `cache->typioproc`). That per-element fmgr
        // dispatch is not ported into this unit, so the real kernels in
        // `range_io.rs` deliberately mirror-pg-and-panic. Installing the seams
        // would only forward a call into a guaranteed panic, so they are held
        // here until the element-I/O fmgr lane lands. Consumed by multirange I/O
        // (backend-utils-adt-multirangetypes::typcache_io).
        ("backend_utils_adt_rangetypes", "range_in"),
        ("backend_utils_adt_rangetypes", "range_out"),
        ("backend_utils_adt_rangetypes", "range_recv"),
        ("backend_utils_adt_rangetypes", "range_send"),
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
        // DESIGN_DEBT: `record_from_values` returns a composite *record* `Datum`
        // (`heap_form_tuple` + `HeapTupleGetDatum`). The composite-Datum bridge
        // (`HeapTupleGetDatum` / the `FormedTuple`->record-Datum carrier) is
        // unported workspace-wide — `types_tuple::Datum` (`TupleValue` ByVal/ByRef)
        // is a scalar byte lane with no Composite/record arm, so the owner cannot
        // construct the return value faithfully. K1-gated on the FormedTuple->
        // HeapTuple carrier bridge (task #161); install once that lands.
        ("backend_utils_fmgr_funcapi", "record_from_values"),
        // DESIGN_DEBT: `value_srf_unported` is the value-per-call SRF protocol
        // (`SRF_FIRSTCALL_INIT`/`SRF_PERCALL_SETUP`/`SRF_RETURN_NEXT`/`_DONE` over a
        // `FuncCallContext` with `multi_call_memory_ctx`/`user_fctx`). funcapi only
        // models the materialize-mode tuplestore path; the value-SRF owner is not
        // yet landed, so this seam is declared genuinely-unported and panics loudly
        // (consumers wrap it in `unreachable!`). Install when the value-SRF
        // machinery is ported.
        ("backend_utils_fmgr_funcapi", "value_srf_unported"),
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
        ("backend_utils_misc_guc_file", "at_eoxact_guc"),
        ("backend_utils_misc_guc_file", "guc_check_errdetail"),
        ("backend_utils_misc_guc_file", "guc_check_errhint"),
        ("backend_utils_misc_guc_file", "new_guc_nest_level"),
        ("backend_utils_misc_guc_file", "set_config_with_handle"),
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
