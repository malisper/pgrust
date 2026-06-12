//! Startup aggregator: calls every ported crate's `init_seams()`.
//!
//! This crate contains NO logic and NO `set()` calls of its own — one line
//! per ported crate, nothing else. Each crate wires its own seams in its own
//! `init_seams()`; this is just the place that invokes them all.

pub fn init_all() {
    // One line per ported crate, kept sorted:
    backend_access_common_heaptuple::init_seams();
    backend_storage_page_checksum::init_seams();
    backend_utils_activity_small::init_seams();
    backend_utils_mb_wstrcmp::init_seams();
    backend_utils_mb_wstrncmp::init_seams();
    backend_utils_misc_pg_rusage::init_seams();
}
