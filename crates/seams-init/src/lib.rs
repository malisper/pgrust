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
    backend_catalog_pg_depend::init_seams();
    backend_catalog_pg_namespace::init_seams();
    backend_catalog_pg_shdepend::init_seams();
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
    backend_utils_misc_guc_file::init_seams();
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
        ("backend_access_heap_heaptoast", "heap_tuple_header_get_datum"),
        ("backend_access_table_tableam", "table_relation_set_new_filelocator"),
        // xlog reconciled out: CATALOG status corrected merged->needs-decomp
        // (chore/xlog-catalog-honest, task #111). An incomplete owner legitimately
        // seam-and-panics its unported surface (mirror-pg-and-panic), so the guard
        // no longer flags it (condition (b) false) — these entries went stale.
        ("backend_commands_user", "is_reserved_name"),
        ("backend_executor_execPartition", "exec_cleanup_tuple_routing"),
        ("backend_executor_execPartition", "exec_find_partition"),
        ("backend_executor_execPartition", "exec_setup_partition_tuple_routing"),
        ("backend_executor_execTuples", "exec_fetch_slot_minimal_tuple_copy"),
        ("backend_executor_execTuples", "exec_force_store_heap_tuple"),
        ("backend_executor_execTuples", "exec_force_store_minimal_tuple"),
        ("backend_executor_execTuples", "exec_materialize_slot"),
        ("backend_executor_execTuples", "exec_store_generated_columns"),
        ("backend_executor_execTuples", "execute_attr_map_slot"),
        ("backend_executor_execTuples", "execute_attr_map_slot_explicit"),
        ("backend_executor_execTuples", "slot_getattr"),
        ("backend_executor_execTuples", "slot_getattr_by_id"),
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
        ("backend_postmaster_interrupt", "install_crash_exit_sigquit_handler"),
        ("backend_postmaster_interrupt", "pqinitmask_set_blocksig"),
        ("backend_storage_ipc_pmsignal", "set_postmaster_death_watch_cloexec"),
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
        ("backend_utils_cache_typcache", "domain_check_input"),
        ("backend_utils_fmgr_dfmgr", "load_archive_module_init"),
        ("backend_utils_fmgr_dfmgr", "shmem_request_hook"),
        ("backend_utils_fmgr_dfmgr", "shmem_request_hook_present"),
        ("backend_utils_init_small", "init_process_globals"),
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
