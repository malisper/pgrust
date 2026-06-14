//! Port of `src/backend/executor/nodeAgg.c` — the Agg executor node.
//!
//! Decomposed into the family modules below, each fully filled with the
//! C-faithful body (the residual `panic!`s are honest seam-and-panic into the
//! still-unported plan-node / nodeFuncs expression vocabulary, not scaffold
//! stubs).
//!
//! Function families (one module file each, see the module docs):
//!
//! - [`node_lifecycle`] — node end / rescan / the `ExecAgg` driver and its
//!   setup helpers (phase/set selection, input fetch, column analysis,
//!   per-trans build).
//! - [`exec_init_agg`] — `ExecInitAgg`, split out of `node_lifecycle` because
//!   the C function (~854 lines) dwarfs the rest of the lifecycle family.
//! - [`transition`] — initializing and advancing transition state (the
//!   transfn driver, ordered/distinct paths).
//! - [`finalize`] — running final functions and projecting the result.
//! - [`sorted_grouping`] — the AGG_PLAIN / AGG_SORTED retrieve path.
//! - [`hash_grouping`] — hash-table build / lookup / retrieve and sizing.
//! - [`spill`] — hash-agg spill files, limits, metrics, and batch refill.
//! - [`aggapi`] — the support-function-callable API (`AggCheckCallContext`
//!   etc.) and the parallel-instrumentation entry points.
//!
//! Owned logic stays here; calls below the executor-node layer go through the
//! owners' seam crates (execExpr / execProcnode / execTuples / execUtils for
//! the executor neighbours; execGrouping for the tuple hash table; fmgr for
//! transfn/finalfn invocation; lsyscache / syscache / aclchk for the catalog
//! reads in init; tuplesort + logtape + hyperloglog for the sort and spill
//! paths; tcop/postgres for `CHECK_FOR_INTERRUPTS`).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(dead_code)]
#![allow(unused_variables)]

pub mod aggapi;
pub mod exec_init_agg;
pub mod finalize;
pub mod hash_grouping;
pub mod node_lifecycle;
pub mod sorted_grouping;
pub mod spill;
pub mod transition;

// Re-export the public interface (nodeAgg.h + the AggState support API).
pub use aggapi::{
    AggCheckCallContext, AggGetAggref, AggGetTempMemoryContext, AggRegisterCallback,
    AggStateIsShared, ExecAggEstimate, ExecAggInitializeDSM, ExecAggInitializeWorker,
    ExecAggRetrieveInstrumentation,
};
pub use exec_init_agg::ExecInitAgg;
pub use hash_grouping::hash_agg_entry_size;
pub use node_lifecycle::{ExecAgg, ExecEndAgg, ExecReScanAgg};
pub use spill::hash_agg_set_limits;

// ---------------------------------------------------------------------------
// Shared constants (nodeAgg.c file scope)
// ---------------------------------------------------------------------------

/// `CHUNKHDRSZ` — `sizeof(MemoryChunk)` (nodeAgg.c:321), used in the per-tuple
/// hash-entry size estimate. In a standard (non-`MEMORY_CONTEXT_CHECKING`)
/// 64-bit build `MemoryChunk` is a single `uint64 hdrmask` = 8 bytes.
pub const CHUNKHDRSZ: usize = 8;

/// `HASHAGG_PARTITION_FACTOR` — partition fan-out growth factor for spilling.
pub const HASHAGG_PARTITION_FACTOR: f64 = 1.50;
/// `HASHAGG_MIN_PARTITIONS` — minimum number of spill partitions.
pub const HASHAGG_MIN_PARTITIONS: i32 = 4;
/// `HASHAGG_MAX_PARTITIONS` — maximum number of spill partitions.
pub const HASHAGG_MAX_PARTITIONS: i32 = 1024;
/// `HASHAGG_READ_BUFFER_SIZE` — read buffer per spill input tape
/// (nodeAgg.c:307 = `BLCKSZ` = 8192).
pub const HASHAGG_READ_BUFFER_SIZE: usize = types_core::BLCKSZ;
/// `HASHAGG_WRITE_BUFFER_SIZE` — write buffer per spill output tape
/// (nodeAgg.c:308 = `BLCKSZ` = 8192).
pub const HASHAGG_WRITE_BUFFER_SIZE: usize = types_core::BLCKSZ;
/// `HASHAGG_HLL_BIT_WIDTH` — register-index bit width for the HLL estimators.
pub const HASHAGG_HLL_BIT_WIDTH: u8 = 5;

/// `pg_nextpower2_64`-friendly cap on bucket counts (mirrors C's
/// `hash_choose_num_buckets` clamping).
pub const HASHAGG_MAX_NBUCKETS_HINT: i64 = i64::MAX / 2;

// ---------------------------------------------------------------------------
// Shared helper struct (nodeAgg.c file scope): FindColsContext
// ---------------------------------------------------------------------------

/// `FindColsContext` (nodeAgg.c) — walker state for `find_cols`, classifying
/// referenced colnos as aggregated vs unaggregated.
#[derive(Debug, Default)]
pub struct FindColsContext<'mcx> {
    /// `bool is_aggref` — is the walk currently under an Aggref?
    pub is_aggref: bool,
    /// `Bitmapset *aggregated` — column references under an aggref.
    pub aggregated: Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>,
    /// `Bitmapset *unaggregated` — other column references.
    pub unaggregated: Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>,
}

/// Install this unit's seams. This unit owns `backend-executor-nodeAgg-pq-seams`
/// (the `ExecAgg*` parallel-instrumentation methods); `aggapi::init_seams`
/// installs all four.
pub fn init_seams() {
    aggapi::init_seams();
}
