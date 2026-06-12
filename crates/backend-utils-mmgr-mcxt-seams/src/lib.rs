//! Seam declarations for the `backend-utils-mmgr-mcxt` unit
//! (`src/backend/utils/mmgr/mcxt.c`). The owning unit installs these from its
//! `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `MemoryContextSwitchTo(TopMemoryContext)` (`mcxt.c` /
    /// `utils/palloc.h`): make `TopMemoryContext` the current allocation
    /// context.
    pub fn switch_to_top_memory_context()
);
