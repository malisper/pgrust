//! Outward seam declarations for the upper-relation path/target machinery that
//! `grouping_planner` (planner.c) calls but whose owner crates are NOT yet
//! ported in this repo. Each call panics loudly until its real owner installs
//! it ("mirror PG and panic").
//!
//! These are the upper-rel builders that turn `query_planner`'s scan/join
//! `RelOptInfo` into the grouping/window/distinct/ordered/final upper rels:
//!
//!   * `create_pathtarget` (tlist.c / pathtarget owner) — build a `PathTarget`
//!     from `root->processed_tlist`. NO crate owns `PathTarget` construction
//!     yet; `RelOptInfo::reltarget` is an owned `Box<PathTarget>` with no arena
//!     id, so a `PathTarget` cannot cross a value seam — this seam hands back
//!     nothing and exists only as the precise STOP point naming the unported
//!     owner. `grouping_planner` reaches it immediately after `query_planner`
//!     for *every* non-set-op query, so the upper half of grouping_planner is
//!     gated here.
//!
//! This crate has NO owner directory, so the
//! `every_declared_seam_is_installed_by_its_owner` guard skips it.

// Note: not `#![no_std]` — `seam_core::seam!` expands to `::std::sync::OnceLock`,
// so any crate invoking it must link `std`.
#![allow(non_snake_case)]

use types_error::PgResult;
use types_pathnodes::PlannerInfo;

seam_core::seam!(
    /// `create_pathtarget(root, tlist)` (tlist.c) — the first unported
    /// upper-rel builder `grouping_planner` reaches. The owner (the
    /// `PathTarget` construction half of tlist.c, plus the whole upper-rel
    /// path machinery: `make_group_input_target` / `make_sort_input_target` /
    /// `make_window_input_target` / `split_pathtarget_at_srfs` /
    /// `apply_scanjoin_target_to_paths` / `create_grouping_paths` /
    /// `create_window_paths` / `create_distinct_paths` / `create_ordered_paths`
    /// / `preprocess_grouping_sets` / `preprocess_minmax_aggregates`) is NOT
    /// ported. Returns nothing because `PathTarget` has no arena handle in this
    /// model (it is an owned `Box<PathTarget>` on `RelOptInfo`), so the value
    /// cannot cross the seam; the call is a precise loud panic until pathtarget
    /// / upper-path machinery lands.
    pub fn create_pathtarget_for_processed_tlist(root: &mut PlannerInfo) -> PgResult<()>
);
