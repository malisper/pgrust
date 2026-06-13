//! Seam declarations for the `backend-catalog-index` unit
//! (`catalog/index.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `ResetReindexState(nestLevel)` — forget any active REINDEX at abort.
    pub fn reset_reindex_state(nest_level: i32)
);
