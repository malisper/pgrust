//! Outward seam declarations for `optimizer/prep/prepunion.c`, which has no
//! crate yet. `grouping_planner` (planner.c) calls `plan_set_operations` for a
//! query with `setOperations`; the owning prepunion.c unit will install this
//! when it lands. Until then a call panics loudly ("mirror PG and panic").
//!
//! This crate has NO owner directory, so the
//! `every_declared_seam_is_installed_by_its_owner` guard skips it.

// Note: not `#![no_std]` — `seam_core::seam!` expands to `::std::sync::OnceLock`,
// so any crate invoking it must link `std`.
#![allow(non_snake_case)]

use ::mcx::Mcx;
use ::types_error::PgResult;
use ::pathnodes::planner_run::PlannerRun;
use pathnodes::{PlannerInfo, RelId};

seam_core::seam!(
    /// `plan_set_operations(root)` (prepunion.c) — construct Paths for the
    /// query's set operations (UNION/INTERSECT/EXCEPT). Returns the
    /// [`RelId`] of the upper rel holding the set-op result paths
    /// (`root->processed_tlist` is filled as a side effect). The owner is
    /// prepunion.c (no crate yet).
    pub fn plan_set_operations<'mcx>(
        mcx: Mcx<'mcx>,
        run: &mut PlannerRun<'mcx>,
        root: &mut PlannerInfo,
    ) -> PgResult<RelId>
);
