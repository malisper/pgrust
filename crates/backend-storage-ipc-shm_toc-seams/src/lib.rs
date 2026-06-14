//! Seam declarations for the `backend-storage-ipc-shm_toc` unit
//! (`storage/ipc/shm_toc.c`) plus the `ParallelContext`/`ParallelWorkerContext`
//! field reads the parallel index-scan node setup needs.
//!
//! The owning units install these from their `init_seams()` when they land;
//! until then a call panics loudly. These cross the DSM TOC, which holds the
//! `ParallelIndexScanDesc` keyed by the plan node id; the owned model stores
//! and retrieves the real descriptor rather than raw bytes.

#![allow(non_snake_case)]

use types_execparallel::{ParallelContextHandle, ParallelWorkerContextHandle};

seam_core::seam!(
    /// `pcxt->nworkers` (access/parallel.h): the number of workers this
    /// parallel context plans to launch.
    pub fn pcxt_nworkers(pcxt: ParallelContextHandle) -> i32
);

seam_core::seam!(
    /// `shm_toc_estimate_chunk(&pcxt->estimator, size)` +
    /// `shm_toc_estimate_keys(&pcxt->estimator, 1)` (shm_toc.h): reserve DSM
    /// space for one chunk of `size` bytes (the parallel index-scan
    /// descriptor) and one TOC key.
    pub fn estimate_chunk_and_key(pcxt: ParallelContextHandle, size: usize)
);

seam_core::seam!(
    /// `piscan = shm_toc_allocate(pcxt->toc, len);
    /// index_parallelscan_initialize(...); shm_toc_insert(pcxt->toc,
    /// plan_node_id, piscan)` (shm_toc.c): allocate the parallel index-scan
    /// descriptor in the context's DSM TOC and register it under the plan node
    /// id, returning the live descriptor. Fallible on OOM / `ereport(ERROR)`.
    pub fn toc_allocate_and_insert_piscan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        pcxt: ParallelContextHandle,
        plan_node_id: i32,
        descriptor: types_nodes::ParallelIndexScanDescData,
    ) -> types_error::PgResult<types_nodes::ParallelIndexScanDesc<'mcx>>
);

seam_core::seam!(
    /// `piscan = shm_toc_lookup(pwcxt->toc, plan_node_id, false)` (shm_toc.c):
    /// retrieve the parallel index-scan descriptor a worker attaches to, by
    /// plan node id. Fallible on the not-found `ereport(ERROR)` (noError=false).
    pub fn toc_lookup_piscan<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        pwcxt: ParallelWorkerContextHandle,
        plan_node_id: i32,
    ) -> types_error::PgResult<types_nodes::ParallelIndexScanDesc<'mcx>>
);

seam_core::seam!(
    /// `node->biss_SharedInfo = shm_toc_allocate(pcxt->toc, size); shm_toc_insert(
    /// pcxt->toc, plan_node_id, ...); memset(0); num_workers = pcxt->nworkers`
    /// (nodeBitmapIndexscan.c `ExecBitmapIndexScanInitializeDSM`): allocate a
    /// zeroed `SharedIndexScanInstrumentation` (header + `nworkers` per-worker
    /// slots) in the context's DSM TOC, register it under the plan node id, and
    /// return the live shared struct. Fallible on OOM / `ereport(ERROR)`.
    pub fn toc_allocate_and_insert_bitmap_instr<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        pcxt: ParallelContextHandle,
        plan_node_id: i32,
        descriptor: types_nodes::SharedIndexScanInstrumentation,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::SharedIndexScanInstrumentation>>
);

seam_core::seam!(
    /// `node->biss_SharedInfo = shm_toc_lookup(pwcxt->toc, plan_node_id, false)`
    /// (nodeBitmapIndexscan.c `ExecBitmapIndexScanInitializeWorker`): retrieve
    /// the `SharedIndexScanInstrumentation` a worker attaches to, by plan node
    /// id. Fallible on the not-found `ereport(ERROR)` (noError=false).
    pub fn toc_lookup_bitmap_instr<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        pwcxt: ParallelWorkerContextHandle,
        plan_node_id: i32,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::SharedIndexScanInstrumentation>>
);
