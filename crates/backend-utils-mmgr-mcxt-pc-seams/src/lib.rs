//! plancache's slice of the memory-context surface (`utils/mmgr/mcxt.c`,
//! `utils/mmgr/aset.c`). plancache manipulates the C context *tree*
//! (`AllocSetContextCreate`, `MemoryContextSwitchTo`, `…SetParent`,
//! `…GetParent`, `…Delete`, the identifier setters, and
//! `CurrentMemoryContext`/`CacheMemoryContext` access). This repo's mcx
//! allocator has no ambient current-context, so this surface is owned by the
//! eventual mctx-remainder port, which installs these; until then a call
//! panics loudly. `CtxId` is the opaque context identity (`0` is NULL).

use types_plancache::CtxId;
use types_error::PgResult;

seam_core::seam!(
    /// `CurrentMemoryContext`.
    pub fn current_memory_context() -> PgResult<CtxId>
);

seam_core::seam!(
    /// `CacheMemoryContext`.
    pub fn cache_memory_context() -> PgResult<CtxId>
);

seam_core::seam!(
    /// `AllocSetContextCreate(parent, name, ALLOCSET_START_SMALL_SIZES)`.
    pub fn alloc_set_context_create_small(parent: CtxId, name: &'static str) -> PgResult<CtxId>
);

seam_core::seam!(
    /// `MemoryContextSwitchTo(context)`, returning the previous context.
    pub fn memory_context_switch_to(context: CtxId) -> PgResult<CtxId>
);

seam_core::seam!(
    /// `MemoryContextSetParent(context, parent)`.
    pub fn memory_context_set_parent(context: CtxId, parent: CtxId) -> PgResult<()>
);

seam_core::seam!(
    /// `MemoryContextGetParent(context)`.
    pub fn memory_context_get_parent(context: CtxId) -> PgResult<CtxId>
);

seam_core::seam!(
    /// `MemoryContextDelete(context)`.
    pub fn memory_context_delete(context: CtxId) -> PgResult<()>
);

seam_core::seam!(
    /// `MemoryContextSetIdentifier(context, id)` — `id` is borrowed from a
    /// long-lived `query_string`.
    pub fn memory_context_set_identifier(context: CtxId, id: &str) -> PgResult<()>
);

seam_core::seam!(
    /// `MemoryContextCopyAndSetIdentifier(context, id)`.
    pub fn memory_context_copy_and_set_identifier(context: CtxId, id: &str) -> PgResult<()>
);
