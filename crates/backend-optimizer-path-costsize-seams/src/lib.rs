//! Seam declarations for the `backend-optimizer-path-costsize` unit
//! (`optimizer/path/costsize.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! The `random_page_cost` / `seq_page_cost` GUC globals deliberately have no
//! getter seams: per the no-ambient-global-seams rule, consumers take the
//! values as explicit parameters.

use types_error::PgResult;
use types_pathnodes::{PathId, PlannerInfo, RelId};

seam_core::seam!(
    /// `cost_bitmap_tree_node(path, &cost, &selec)` (costsize.c) — returned as a
    /// `(cost, selectivity)` tuple. The bitmap-tree path crosses as its `PathId`
    /// arena handle; the provider dispatches on the arena `PathNode` subtype.
    pub fn cost_bitmap_tree_node(root: &PlannerInfo, path: PathId) -> (f64, f64)
);

seam_core::seam!(
    /// `enable_indexonlyscan` (costsize.c GUC) — whether index-only scans are
    /// enabled. Read by `check_index_only`.
    pub fn enable_indexonlyscan() -> bool
);

seam_core::seam!(
    /// `create_partial_bitmap_paths(root, rel, bitmapqual)` (costsize.c) — build
    /// the partial (parallel) BitmapHeapPath(s) for the rel and `add_partial_path`
    /// them. The bitmapqual crosses as its `PathId` arena handle.
    pub fn create_partial_bitmap_paths(
        root: &mut PlannerInfo,
        rel: RelId,
        bitmapqual: PathId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `clamp_row_est(nrows)` (costsize.c): force a row-count estimate to a
    /// sane value — `rint()` it and clamp to at least one row. Pure math;
    /// cannot `ereport`.
    pub fn clamp_row_est(nrows: f64) -> f64
);

seam_core::seam!(
    /// `clamp_cardinality_to_long(x)` (costsize.c): cast a `Cardinality`
    /// (`double`) to a sane `long` (here `i64`). `NaN` -> `i64::MAX`; `x <= 0`
    /// -> 0; otherwise `x` if it is strictly below `i64::MAX` as a double, else
    /// `i64::MAX`. Pure math; cannot `ereport`.
    pub fn clamp_cardinality_to_long(x: f64) -> i64
);
