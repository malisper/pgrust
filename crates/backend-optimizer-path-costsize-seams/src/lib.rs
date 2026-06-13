//! Seam declarations for the `backend-optimizer-path-costsize` unit
//! (`optimizer/path/costsize.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! The `random_page_cost` / `seq_page_cost` GUC globals deliberately have no
//! getter seams: per the no-ambient-global-seams rule, consumers take the
//! values as explicit parameters.

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
