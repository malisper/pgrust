//! Seam declarations for the block-reference-table builder
//! (`common/blkreftable.c`): create an empty table, record limit blocks and
//! modified blocks, and serialize it.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The table is named by an opaque
//! [`BlockRefTableHandle`] (the C `BlockRefTable *`).

extern crate alloc;

use mcx::Mcx;
use types_blkreftable::{BlockRefTableHandle, BlockRefTableReaderHandle};
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
    /// `BlockRefTableGetEntry(brtab, rlocator, forknum, &limit_block)`
    /// (blkreftable.c) — look up the entry for this relation fork. Returns
    /// `Some(limit_block)` if an entry exists (the C non-NULL return that also
    /// writes `*limit_block`), `None` if not. Used for the whole-database
    /// existence test, where only the entry's presence and limit block matter.
    pub fn block_ref_table_get_entry(
        brtab: BlockRefTableHandle,
        rlocator: RelFileLocator,
        forknum: ForkNumber,
    ) -> Option<BlockNumber>
);

seam_core::seam!(
    /// `BlockRefTableGetEntry(brtab, rlocator, forknum, &limit_block)` followed
    /// by `BlockRefTableEntryGetBlocks(entry, start_blkno, stop_blkno, blocks,
    /// nblocks)` (blkreftable.c). The `BlockRefTableEntry` lives inside the
    /// table behind the opaque handle, so the lookup and the per-entry
    /// block-extraction are bundled into one owner seam. Returns
    /// `Some((limit_block, blocks))` when the entry exists (the modified block
    /// numbers in `[start_blkno, stop_blkno)`, at most `nblocks`), or `None`
    /// when there is no entry for this relation fork.
    pub fn block_ref_table_get_entry_blocks<'mcx>(
        mcx: Mcx<'mcx>,
        brtab: BlockRefTableHandle,
        rlocator: RelFileLocator,
        forknum: ForkNumber,
        start_blkno: BlockNumber,
        stop_blkno: BlockNumber,
        nblocks: usize,
    ) -> PgResult<Option<(BlockNumber, mcx::PgVec<'mcx, BlockNumber>)>>
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

// ---------------------------------------------------------------------------
// Reader side: incremental on-disk reading of a block-reference table.
// `pg_wal_summary_contents` drives these over a `BlockRefTableReaderHandle`
// produced by the walsummary owner's `wal_summary_create_reader` seam (which
// itself calls `common_blkreftable::create_block_ref_table_reader` directly — a
// plain pub fn, not a seam — to mint the handle).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `BlockRefTableReaderNextRelation(reader, &rlocator, &forknum,
    /// &limit_block)` (blkreftable.c) — advance the reader to the next
    /// relation fork, returning its `(rlocator, forknum, limit_block)`.
    /// Returns `Ok(None)` at end-of-table (the C `false` return). `Err`
    /// carries the read-callback / format `ereport(ERROR)` (relayed through
    /// the reader's `error_callback`).
    pub fn block_ref_table_reader_next_relation(
        reader: BlockRefTableReaderHandle,
    ) -> PgResult<Option<(RelFileLocator, ForkNumber, BlockNumber)>>
);

seam_core::seam!(
    /// `BlockRefTableReaderGetBlocks(reader, blocks, nblocks)` (blkreftable.c)
    /// — fetch up to `nblocks` modified block numbers of the current relation
    /// fork, returning them in order. An empty vector signals that the current
    /// fork is exhausted (the C `0` return). `Err` carries the read-callback /
    /// format `ereport(ERROR)`.
    pub fn block_ref_table_reader_get_blocks<'mcx>(
        mcx: Mcx<'mcx>,
        reader: BlockRefTableReaderHandle,
        nblocks: usize,
    ) -> PgResult<mcx::PgVec<'mcx, BlockNumber>>
);

seam_core::seam!(
    /// `DestroyBlockRefTableReader(reader)` (blkreftable.c) — free the reader
    /// and its buffers. Infallible in C.
    pub fn destroy_block_ref_table_reader(reader: BlockRefTableReaderHandle)
);
