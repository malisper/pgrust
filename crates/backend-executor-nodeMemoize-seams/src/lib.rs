//! Seam declarations for `backend-executor-nodeMemoize` (`nodeMemoize.c`, the
//! `Memoize` caching scan node).
//!
//! This owned `-seams` crate holds ONLY the inward parallel-executor entry
//! points the node installs in `init_seams()` — the four `exec_memoize_*` hooks
//! `backend-executor-execParallel` dispatches to generically over the live
//! `PlanState` tree (`PlanStateHandle`). Every *outward* call the node makes
//! (the executor slot/expr/context substrate, the `simplehash` hash/equality
//! leaves, the catalog hash-function lookups, `fmgr`, the outer-child dispatch,
//! the cache memory budget, the interrupt check, and the handle-addressed
//! parallel-instrumentation accessors) goes through the *owner* subsystem's
//! `-seams` crate, with the node-side marshaling living in the node crate.

#![allow(unused_doc_comments)]
#![allow(non_snake_case)]

use types_error::PgResult;
use types_execparallel::{ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle};

// ===========================================================================
// Parallel-executor entry points (execParallel.c dispatch over PlanState).
//
// These are the only seams this crate owns; the node crate installs them in
// `init_seams()`. The C control flow lives in the node crate; the
// handle-addressed reads/writes of the live `MemoizeState` and the DSM
// `SharedMemoizeInfo` chunk go through the parallel-executor support seams.
// ===========================================================================

seam_core::seam!(pub fn exec_memoize_estimate(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_memoize_initialize_dsm(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_memoize_initialize_worker(node: PlanStateHandle, pwcxt: ParallelWorkerContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_memoize_retrieve_instrumentation(node: PlanStateHandle) -> PgResult<()>);
