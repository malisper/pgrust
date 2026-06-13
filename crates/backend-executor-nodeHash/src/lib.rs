//! `nodeHash.c` — routines to hash relations for hashjoin (PostgreSQL 18.3).
//!
//! SCAFFOLD: every function has its real, C-faithful signature and a
//! `todo!("decomp")` body. Bodies are filled in per family module by later
//! agents.
//!
//! Function families (one module per family, so bodies fill in parallel):
//! - [`exec_hash`]    — the Hash executor-node lifecycle and the `MultiExec`
//!   build entry points (`ExecHash`, `MultiExecHash`, `MultiExecPrivateHash`,
//!   `MultiExecParallelHash`, `ExecInitHash`, `ExecEndHash`, `ExecReScanHash`,
//!   `ExecShutdownHash`).
//! - [`hash_table`]  — the serial in-memory hash table: create / size /
//!   build / probe / grow / reset plus the dense-allocator
//!   (`ExecHashTableCreate`, `ExecChooseHashTableSize`,
//!   `ExecHashTableDestroy`, `ExecHashIncreaseBatchSize`,
//!   `ExecHashIncreaseNumBatches`, `ExecHashIncreaseNumBuckets`,
//!   `ExecHashTableInsert`, `ExecHashGetBucketAndBatch`, `ExecScanHashBucket`,
//!   `ExecPrepHashTableForUnmatched`, `ExecScanHashTableForUnmatched`,
//!   `ExecHashTableReset`, `ExecHashTableResetMatchFlags`, `dense_alloc`,
//!   `get_hash_memory_limit`).
//! - [`skew`]        — the skew-optimization hashtable (`ExecHashBuildSkewHash`,
//!   `ExecHashGetSkewBucket`, `ExecHashSkewTableInsert`,
//!   `ExecHashRemoveNextSkewBucket`).
//! - [`parallel`]    — the Parallel Hash Join shared-memory machinery (the
//!   ~23 `ExecParallelHash*` / detach routines).
//! - [`instrument`]  — instrumentation and the parallel-DSM node hooks
//!   (`ExecHashEstimate`, `ExecHashInitializeDSM`, `ExecHashInitializeWorker`,
//!   `ExecHashRetrieveInstrumentation`, `ExecHashAccumInstrumentation`).
//!
//! Operations below the executor-node layer go through the owners' seam
//! crates: child dispatch (execProcnode), expr eval (execExpr), slot/econtext
//! ops (execTuples/execUtils), instrumentation (instrument), the skew-hash
//! catalog/fmgr lookups (syscache/lsyscache/fmgr), the tuple-spill callback
//! (nodeHashjoin), and the whole DSM/parallel stack (dsa / lwlock / barrier /
//! buffile / shared-tuplestore).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

pub mod exec_hash;
pub mod hash_table;
pub mod instrument;
pub mod parallel;
pub mod skew;

use types_core::Size;

// ===========================================================================
//                          Constants & macros
// ===========================================================================

/// `MAXIMUM_ALIGNOF` — 8 on 64-bit PostgreSQL.
pub(crate) const MAXIMUM_ALIGNOF: usize = 8;

/// `MAXALIGN(LEN)` (c.h) — round `len` up to `MAXIMUM_ALIGNOF`.
#[inline]
pub(crate) const fn MAXALIGN(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `MaxAllocSize` (memutils.h) — `0x3fffffff` (1 GB - 1).
pub(crate) const MaxAllocSize: usize = 0x3fff_ffff;

/// `SizeofMinimalTupleHeader` (htup_details.h) — `offsetof(MinimalTupleData,
/// t_bits)` = 23 on 64-bit PostgreSQL.
pub(crate) const SizeofMinimalTupleHeader: usize = 23;

/// `BLCKSZ` (pg_config.h) — default block size, 8192.
pub(crate) const BLCKSZ: usize = 8192;

/// `HJTUPLE_OVERHEAD` (hashjoin.h) — `MAXALIGN(sizeof(HashJoinTupleData))`.
/// In the owned model the per-tuple header is `next` (8) + `hashvalue` (4),
/// MAXALIGNed to 16; restated as a constant for the byte-accounting that
/// mirrors C's pointer arithmetic.
pub(crate) const HJTUPLE_OVERHEAD: usize = MAXALIGN(8 + 4);

/// `HASH_CHUNK_HEADER_SIZE` (hashjoin.h) — `MAXALIGN(sizeof(HashMemoryChunkData))`.
pub(crate) const HASH_CHUNK_HEADER_SIZE: usize = MAXALIGN(8 * 3 + 8);

/// `SKEW_BUCKET_OVERHEAD` (hashjoin.h) — `MAXALIGN(sizeof(HashSkewBucket))`.
pub(crate) const SKEW_BUCKET_OVERHEAD: usize = MAXALIGN(4 + 8);

// Re-export the hashjoin vocabulary the bodies and callers use.
pub use types_nodes::nodehash::{
    BucketAndBatch, HASH_CHUNK_SIZE, HASH_CHUNK_THRESHOLD, INVALID_SKEW_BUCKET_NO,
    SKEW_HASH_MEM_PERCENT, SKEW_MIN_OUTER_FRACTION,
};

/// Silence the "unused crate dependency" lint on seam crates whose calls live
/// only inside `todo!()` bodies for now. (Removed once bodies land.)
#[allow(unused_imports)]
mod _seam_deps {
    use backend_executor_execAmi_seams as _execAmi;
    use backend_executor_execExpr_seams as _execExpr;
    use backend_executor_execProcnode_seams as _execProcnode;
    use backend_executor_execTuples_seams as _execTuples;
    use backend_executor_execUtils_seams as _execUtils;
    use backend_executor_instrument_seams as _instrument;
    use backend_executor_nodeHashjoin_seams as _nodeHashjoin;
    use backend_storage_file_buffile_seams as _buffile;
    use backend_storage_ipc_barrier_seams as _barrier;
    use backend_storage_lmgr_lwlock_seams as _lwlock;
    use backend_utils_cache_lsyscache_seams as _lsyscache;
    use backend_utils_cache_syscache_seams as _syscache;
    use backend_utils_fmgr_fmgr_seams as _fmgr;
    use backend_utils_mmgr_dsa_seams as _dsa;
    use backend_utils_sort_storage_seams as _sort_storage;
    use types_datum as _types_datum;
    use types_storage as _types_storage;
    use types_condvar as _types_condvar;
}

// ===========================================================================
//                              Seam installation
// ===========================================================================

/// Install this unit's own outward-facing seams (the parallel-context node
/// hooks consumers reach through `backend-executor-nodeHash-pq-seams`). Empty
/// until the bodies land; wired into `seams-init` now so the slot exists.
pub fn init_seams() {
    // backend_executor_nodeHash_pq_seams::exec_hash_estimate::set(...);
    // backend_executor_nodeHash_pq_seams::exec_hash_initialize_dsm::set(...);
    // backend_executor_nodeHash_pq_seams::exec_hash_initialize_worker::set(...);
    // backend_executor_nodeHash_pq_seams::exec_hash_retrieve_instrumentation::set(...);
}

/// Silence the unused-`Size` import warning in the scaffold.
#[allow(dead_code)]
const _: fn() -> Size = || 0;
