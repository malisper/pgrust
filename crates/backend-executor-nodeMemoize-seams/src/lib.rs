//! Seam declarations for `backend-executor-nodeMemoize` (`nodeMemoize.c`, the
//! `Memoize` caching scan node).
//!
//! This owned `-seams` crate holds ONLY the inward parallel-executor entry
//! points the node installs in `init_seams()` — the four `exec_memoize_*` hooks
//! `backend-executor-execParallel` dispatches to generically over the live
//! `PlanState` tree (`PlanStateHandle`). Every *outward* call the node makes
//! (the executor slot/expr/context substrate, the `simplehash` hash/equality
//! leaves, the catalog hash-function lookups, `fmgr`, the outer-child dispatch,
//! the cache memory budget, the interrupt check, and the DSM estimate/serialize
//! via the orthogonal owned `shm_toc` support seams) goes through the *owner*
//! subsystem's `-seams` crate, with the node-side marshaling living in the node
//! crate. The four parallel hooks below keep their `PlanStateHandle` ABI because
//! `execParallel` dispatches them generically over the `PlanState` tree; the
//! node crate resolves the handle to the live owned `MemoizeScanState` and runs
//! the owned `ExecMemoize{Estimate,InitializeDSM,InitializeWorker,
//! RetrieveInstrumentation}` entry points.

#![allow(unused_doc_comments)]
#![allow(non_snake_case)]

use types_error::PgResult;
use types_execparallel::{ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle};

// ===========================================================================
// Parallel-executor entry points (execParallel.c dispatch over PlanState).
//
// These are the only seams this crate owns; the node crate installs them in
// `init_seams()`. The C control flow lives in the node crate over the owned
// `MemoizeScanState` (each shim resolves the dispatch `PlanStateHandle` to it);
// the DSM `SharedMemoizeInfo` chunk estimate/serialize goes through the
// orthogonal owned `shm_toc` support seams.
// ===========================================================================

seam_core::seam!(pub fn exec_memoize_estimate(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_memoize_initialize_dsm(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_memoize_initialize_worker(node: PlanStateHandle, pwcxt: ParallelWorkerContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_memoize_retrieve_instrumentation(node: PlanStateHandle) -> PgResult<()>);
