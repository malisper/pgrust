//! Seam declarations for parallel-executor methods on `exec_append` nodes.
//!
//! Installed by the owning node crate's `init_seams()` when it lands;
//! until then a call panics loudly.
#![allow(unused_doc_comments)]
#![allow(non_snake_case)]

use types_execparallel::{ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle};
use types_error::PgResult;

seam_core::seam!(pub fn exec_append_estimate(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_append_initialize_dsm(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_append_reinitialize_dsm(node: PlanStateHandle, pcxt: ParallelContextHandle) -> PgResult<()>);
seam_core::seam!(pub fn exec_append_initialize_worker(node: PlanStateHandle, pwcxt: ParallelWorkerContextHandle) -> PgResult<()>);
