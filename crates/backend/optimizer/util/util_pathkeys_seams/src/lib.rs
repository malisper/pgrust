//! Seam declarations for `optimizer/util/pathkeys.c`, arena-shaped over
//! [`pathnodes::PlannerInfo`].
//!
//! indxpath.c's `build_index_paths` asks pathkeys.c whether the query has
//! useful pathkeys, builds the index's own ordering pathkeys, and truncates them
//! to the useful prefix. These live in pathkeys.c; we cross that boundary here.
//! Defaults to a loud panic until pathkeys.c is ported.

extern crate alloc;

use alloc::vec::Vec;

use pathnodes::{IndexOptInfo, PathKey, PlannerInfo, RelId, ScanDirection};

seam_core::seam!(
    /// `has_useful_pathkeys(root, rel)` (pathkeys.c) — true if the query has any
    /// pathkeys (sort orderings) that could be useful for `rel`.
    pub fn has_useful_pathkeys(root: &PlannerInfo, rel: RelId) -> bool
);

seam_core::seam!(
    /// `build_index_pathkeys(root, index, scandir)` (pathkeys.c) — build the
    /// list of pathkeys describing the sort order of `index` scanned in the
    /// given direction. Returns the pathkey values (the C `List *` of `PathKey
    /// *`). Takes the index by reference (it lives in the rel's indexlist, not
    /// the arena).
    pub fn build_index_pathkeys(
        root: &mut PlannerInfo,
        index: &IndexOptInfo,
        scandir: ScanDirection
    ) -> Vec<PathKey>
);

seam_core::seam!(
    /// `truncate_useless_pathkeys(root, rel, pathkeys)` (pathkeys.c) — drop the
    /// trailing pathkeys of `pathkeys` that are of no use for `rel`'s query.
    pub fn truncate_useless_pathkeys(
        root: &PlannerInfo,
        rel: RelId,
        pathkeys: Vec<PathKey>
    ) -> Vec<PathKey>
);
