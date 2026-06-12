//! Seam declarations for the `backend-optimizer-path-costsize` unit
//! (`optimizer/path/costsize.c`): reads of its GUC-assigned cost globals.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `random_page_cost` (costsize.c): the `random_page_cost` GUC.
    pub fn random_page_cost() -> f64
);

seam_core::seam!(
    /// `seq_page_cost` (costsize.c): the `seq_page_cost` GUC.
    pub fn seq_page_cost() -> f64
);
