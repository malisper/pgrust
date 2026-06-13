//! Inward seam declarations for the `backend-replication-logical-logical` unit
//! (`replication/logical/logical.c`).
//!
//! These are the entry points other (cyclic-partner) subsystems call back into
//! logical decoding through. `logical.c` installs them from its `init_seams()`.
//! Until then a call panics loudly.

#![allow(non_snake_case)]

use types_error::PgResult;
use types_logical::ReorderBufferCallback;

seam_core::seam!(
    /// `ResetLogicalStreamingState()` — reset logical streaming state on
    /// abort.
    pub fn reset_logical_streaming_state()
);

seam_core::seam!(
    /// Re-enter the crate's ReorderBuffer-driven `*_cb_wrapper` selected by
    /// `cb`, with `ctx == cache->private_data` (the runtime resolves the live
    /// decoding context). The reorderbuffer owner's trampolines call this.
    /// Mirrors the C wrapper failure surface: any wrapper can `ereport`.
    pub fn dispatch_reorderbuffer_callback(cb: ReorderBufferCallback) -> PgResult<()>
);
