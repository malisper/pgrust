//! Seam declarations for the shared-memory table-of-contents estimator
//! (`backend/storage/ipc/shm_toc.c`), trimmed to what the parallel-scan node
//! entry points reach.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The `ParallelContext.estimator` and `toc` are
//! DSM-owned; the node only adds to the estimate (allocate/insert/lookup of
//! the per-node chunk are folded into the FDW initialize/reinitialize/worker
//! callbacks, which receive the context).

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use types_error::PgResult;
use ::nodes::ParallelContext;

seam_core::seam!(
    /// `shm_toc_estimate_chunk(&pcxt->estimator, nbytes)`: reserve space for a
    /// chunk of `nbytes` in the parallel-coordination DSM estimate.
    pub fn shm_toc_estimate_chunk(pcxt: &mut ParallelContext, nbytes: usize) -> PgResult<()>
);

seam_core::seam!(
    /// `shm_toc_estimate_keys(&pcxt->estimator, nkeys)`: reserve `nkeys` TOC
    /// key slots in the parallel-coordination DSM estimate.
    pub fn shm_toc_estimate_keys(pcxt: &mut ParallelContext, nkeys: usize) -> PgResult<()>
);
