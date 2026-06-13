//! Seam declarations for the block-reference-table builder
//! (`common/blkreftable.c`): create an empty table, record limit blocks and
//! modified blocks, and serialize it.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The table is named by an opaque
//! [`BlockRefTableHandle`] (the C `BlockRefTable *`).

extern crate alloc;

use mcx::Mcx;
use types_blkreftable::BlockRefTableHandle;
use types_core::{BlockNumber, ForkNumber};
use types_error::PgResult;
use types_storage::RelFileLocator;

seam_core::seam!(
    /// `CreateEmptyBlockRefTable()` (blkreftable.c) — allocate an empty table
    /// in `mcx` (the backend palloc's it in `CurrentMemoryContext` and stores
    /// that context in `brtab->mcxt`). `Err` is the allocation's OOM.
    pub fn create_empty_block_ref_table<'mcx>(mcx: Mcx<'mcx>) -> PgResult<BlockRefTableHandle>
);

seam_core::seam!(
    /// `BlockRefTableSetLimitBlock(brtab, rlocator, forknum, limit_block)` —
    /// note that only blocks `>= limit_block` of this fork should be tracked.
    /// Inserts into the table's hash (allocates), so the OOM `ereport(ERROR)`
    /// is `Err`.
    pub fn block_ref_table_set_limit_block(
        brtab: BlockRefTableHandle,
        rlocator: RelFileLocator,
        forknum: ForkNumber,
        limit_block: BlockNumber,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `BlockRefTableMarkBlockModified(brtab, rlocator, forknum, blocknum)` —
    /// record that the block was modified. Inserts into the table's hash
    /// (allocates), so the OOM `ereport(ERROR)` is `Err`.
    pub fn block_ref_table_mark_block_modified(
        brtab: BlockRefTableHandle,
        rlocator: RelFileLocator,
        forknum: ForkNumber,
        blocknum: BlockNumber,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `WriteBlockRefTable(brtab, write_callback, write_callback_arg)`
    /// (blkreftable.c) — serialize the table. The backend streams the bytes
    /// through a write callback; the port returns the serialized bytes
    /// directly, allocated in `mcx`. `Err` carries the serialization OOM /
    /// `ereport(ERROR)`.
    pub fn write_block_ref_table<'mcx>(
        mcx: Mcx<'mcx>,
        brtab: BlockRefTableHandle,
    ) -> PgResult<mcx::PgVec<'mcx, u8>>
);
