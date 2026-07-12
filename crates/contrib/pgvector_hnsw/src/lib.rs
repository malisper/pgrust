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
