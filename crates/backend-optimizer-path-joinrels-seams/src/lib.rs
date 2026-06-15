//! Seam declarations for `optimizer/path/joinrels.c`, arena-shaped over
//! [`types_pathnodes::PlannerInfo`].
//!
//! indxpath.c's loop-count / joinrel-size estimators skip relations that have
//! been proven empty (`IS_DUMMY_REL`); the dummy-rel test reads the rel's
//! cheapest path subtype, which is joinrels.c's `is_dummy_rel`. Defaults to a
//! loud panic until joinrels.c is ported.

use types_pathnodes::{PlannerInfo, RelId};

seam_core::seam!(
    /// `is_dummy_rel(rel)` (joinrels.c) — true if the rel is known to produce no
    /// rows (its cheapest_total_path is a dummy Append with no subpaths).
    pub fn is_dummy_rel(root: &PlannerInfo, rel: RelId) -> bool
);
