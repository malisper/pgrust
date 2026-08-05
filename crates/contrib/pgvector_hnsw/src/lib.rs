//! pgvector 0.8.5 hnsw AM (hnsw.c/hnswutils.c/hnswinsert.c/hnswscan.c/
//! hnswvacuum.c), serial on-disk rendering; the in-memory build phase lives in
//! pgvector_hnsw_build. DIVERGENCES (recorded): no parallel build (C falls back
//! to serial when no workers launch); iterative-scan memory cap approximates
//! C's MemoryContextMemAllocated with per-tuple estimates; level RNG uses the
//! ported pg_global_prng (same generator, per-backend seeding).

pub mod insert;
pub mod layout;
pub mod scan;
pub mod utils;
pub mod vacuum;

pub use insert::hnswinsert;
pub use scan::{hnswbeginscan, hnswendscan, hnswgettuple, hnswrescan};
pub use vacuum::{hnswbulkdelete, hnswbulkdelete_collect, hnswvacuumcleanup};

use types_core::Oid;
use types_error::PgResult;

pub fn hnswvalidate(_opclassoid: Oid) -> PgResult<bool> {
    Ok(true)
}

use std::cell::Cell;
thread_local! {
    static EF_SEARCH: Cell<i32> = const { Cell::new(40) };
    static ITERATIVE_SCAN: Cell<i32> = const { Cell::new(0) };
    static MAX_SCAN_TUPLES: Cell<i32> = const { Cell::new(20000) };
    static SCAN_MEM_MULTIPLIER: Cell<f64> = const { Cell::new(1.0) };
}

// hnsw.* GUC backing (C's hnsw_ef_search &co globals in hnsw.c).
pub fn init_seams() {
    use guc_tables::GucVarAccessors;
    guc_tables::vars::hnsw_ef_search.install_if_absent(GucVarAccessors {
        get: || EF_SEARCH.with(|c| c.get()),
        set: |v| EF_SEARCH.with(|c| c.set(v)),
    });
    guc_tables::vars::hnsw_iterative_scan.install_if_absent(GucVarAccessors {
        get: || ITERATIVE_SCAN.with(|c| c.get()),
        set: |v| ITERATIVE_SCAN.with(|c| c.set(v)),
    });
    guc_tables::vars::hnsw_max_scan_tuples.install_if_absent(GucVarAccessors {
        get: || MAX_SCAN_TUPLES.with(|c| c.get()),
        set: |v| MAX_SCAN_TUPLES.with(|c| c.set(v)),
    });
    guc_tables::vars::hnsw_scan_mem_multiplier.install_if_absent(GucVarAccessors {
        get: || SCAN_MEM_MULTIPLIER.with(|c| c.get()),
        set: |v| SCAN_MEM_MULTIPLIER.with(|c| c.set(v)),
    });
}
