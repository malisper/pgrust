//! Seam declarations for the `backend-replication-logical-logical` unit
//! (`replication/logical/logical.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `ResetLogicalStreamingState()` — reset logical streaming state on
    /// abort.
    pub fn reset_logical_streaming_state()
);
