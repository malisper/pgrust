//! Startup aggregator: calls every ported crate's `init_seams()`.
//!
//! This crate contains NO logic and NO `set()` calls of its own — one line
//! per ported crate, nothing else. Each crate wires its own seams in its own
//! `init_seams()`; this is just the place that invokes them all.

pub fn init_all() {
    // One line per ported crate, kept sorted:
    backend_access_common_heaptuple::init_seams();
    backend_access_hashvalidate::init_seams();
    backend_access_heap_heaptoast::init_seams();
    backend_access_table_table::init_seams();
    backend_access_table_tableam::init_seams();
    backend_access_transam_xlogstats::init_seams();
    backend_executor_execAmi::init_seams();
    backend_executor_execUtils::init_seams();
    backend_executor_nodeMaterial::init_seams();
    backend_libpq_pqcomm::init_seams();
    backend_libpq_pqsignal::init_seams();
    backend_port_atomics::init_seams();
    backend_postmaster_interrupt::init_seams();
    backend_postmaster_syslogger::init_seams();
    backend_storage_ipc_dsm_core::init_seams();
    backend_storage_ipc_procsignal::init_seams();
    backend_storage_lmgr_lwlock::init_seams();
    backend_storage_page_checksum::init_seams();
    backend_utils_activity_small::init_seams();
    backend_utils_cache_attoptcache::init_seams();
    backend_utils_cache_relfilenumbermap::init_seams();
    backend_utils_cache_spccache::init_seams();
    backend_utils_cache_syscache::init_seams();
    backend_utils_cache_ts_cache::init_seams();
    backend_utils_error::init_seams();
    backend_utils_mb_wstrcmp::init_seams();
    backend_utils_mb_wstrncmp::init_seams();
    backend_utils_misc_pg_rusage::init_seams();
    backend_utils_misc_queryenvironment::init_seams();
    backend_utils_misc_sampling::init_seams();
    backend_utils_time_combocid::init_seams();
    interfaces_libpq_legacy_pqsignal::init_seams();
}
