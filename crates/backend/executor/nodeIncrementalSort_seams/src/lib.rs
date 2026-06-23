//! Seam declarations for parallel-executor methods on `exec_incrementalsort` nodes.
//!
//! Installed by the owning node crate's `init_seams()` when it lands;
//! until then a call panics loudly.
#![allow(unused_doc_comments)]
#![allow(non_snake_case)]

use ::execparallel::{ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle};
use ::types_error::PgResult;

seam_core::seam!(pub fn exec_incrementalsort_estimate(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_incrementalsort_initialize_dsm(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_incrementalsort_initialize_worker(node: PlanStateHandle, pwcxt: ParallelWorkerContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_incrementalsort_retrieve_instrumentation(node: PlanStateHandle) -> PgResult<()>);
