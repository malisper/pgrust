//! Seams for `src/backend/access/heap/hio.c` — the heap "insertion
//! organization": placing a tuple onto a page (`RelationPutHeapTuple`) and
//! finding/extending a page with enough free space
//! (`RelationGetBufferForTuple`).
//!
//! Every declaration here is **outward**: a function `hio.c` reaches in an
//! owner crate that lives across a dependency cycle (the buffer manager, the
//! opaque-`Page` predicates/mutators, the free-space map, the relation-extension
//! lock, the visibility map, and the relcache/`rel.h` field reads). Each
//! defaults to the [`seam_core::seam!`] loud panic and is installed by its owner
//! when it lands — there is no silent fallback, nothing fabricates a buffer, a
//! page predicate, a free-space answer, or a visibility-map pin.
//!
//! The relation crosses each seam as its bare [`Oid`] identity
//! (`RelationGetRelid`, the relcache key); the substrate re-resolves the live
//! relation from the relcache. The buffer handles cross as plain [`Buffer`]
//! integers (the buffer manager's own indices), so threading several at once —
//! the `otherBuffer` lock-ordering path holds two, `RelationAddBlocks` pins up
//! to `MAX_BUFFERS_TO_EXTEND_BY` (64) — is sound here; the multi-pin requirement
//! lands on whatever runtime installs these slots. This bare-`Oid` convention
//! mirrors the established `backend-access-heap-vacuumlazy-seams` precedent for
//! the same heap-page substrate.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;

use types_core::{BlockNumber, Buffer, OffsetNumber, Oid, Size};
use types_error::PgResult;
use types_storage::buf::ExtendedRelation;
use types_tuple::heaptuple::ItemPointerData;

// --- bufmgr.c — buffer read / pin / lock / dirty ---------------------------

seam_core::seam!(
    /// `LockBuffer(buffer, mode)` (`storage/buffer/bufmgr.c`). `mode` is
    /// `BUFFER_LOCK_UNLOCK` / `BUFFER_LOCK_SHARE` / `BUFFER_LOCK_EXCLUSIVE`.
    pub fn lock_buffer(buffer: Buffer, mode: i32) -> PgResult<()>
);

seam_core::seam!(
    /// `ConditionalLockBuffer(buffer)` (`storage/buffer/bufmgr.c`) — try to
    /// acquire the content lock without blocking; returns whether it succeeded.
    pub fn conditional_lock_buffer(buffer: Buffer) -> PgResult<bool>
);

seam_core::seam!(
    /// `MarkBufferDirty(buffer)` (`storage/buffer/bufmgr.c`).
    pub fn mark_buffer_dirty(buffer: Buffer) -> PgResult<()>
);

seam_core::seam!(
    /// `ReleaseBuffer(buffer)` (`storage/buffer/bufmgr.c`) — drop one pin.
    pub fn release_buffer(buffer: Buffer) -> PgResult<()>
);

seam_core::seam!(
    /// `UnlockReleaseBuffer(buffer)` (`storage/buffer/bufmgr.c`) — unlock the
    /// content lock then drop one pin.
    pub fn unlock_release_buffer(buffer: Buffer) -> PgResult<()>
);

seam_core::seam!(
    /// `IncrBufferRefCount(buffer)` (`storage/buffer/bufmgr.c`) — add a pin to
    /// an already-pinned buffer without re-reading it.
    pub fn incr_buffer_ref_count(buffer: Buffer) -> PgResult<()>
);

seam_core::seam!(
    /// `BufferGetBlockNumber(buffer)` (`storage/buffer/bufmgr.c`).
    pub fn buffer_get_block_number(buffer: Buffer) -> PgResult<BlockNumber>
);

seam_core::seam!(
    /// `BufferGetPageSize(buffer)` (`storage/bufmgr.h`) — the usable page size
    /// (== `BLCKSZ` for ordinary relations).
    pub fn buffer_get_page_size(buffer: Buffer) -> PgResult<Size>
);

seam_core::seam!(
    /// `ReadBufferExtended(relation, MAIN_FORKNUM, target_block, mode,
    /// strategy)` (`storage/buffer/bufmgr.c`). `mode` is one of the `RBM_*`
    /// read-buffer modes. `has_strategy` is `bistate->strategy != NULL` (the
    /// bulk-insert BULKWRITE strategy is applied by the runtime when set).
    pub fn read_buffer_extended(
        rel: Oid,
        target_block: BlockNumber,
        mode: i32,
        has_strategy: bool,
    ) -> PgResult<Buffer>
);

seam_core::seam!(
    /// `ReadBuffer(relation, target_block)` (`storage/buffer/bufmgr.c`) ==
    /// `ReadBufferExtended` with the default strategy on the main fork.
    pub fn read_buffer(rel: Oid, target_block: BlockNumber) -> PgResult<Buffer>
);

seam_core::seam!(
    /// `ExtendBufferedRelBy(BMR_REL(relation), MAIN_FORKNUM, strategy,
    /// EB_LOCK_FIRST, extend_by, victim_buffers, &extend_by)`
    /// (`storage/buffer/bufmgr.c`). `has_strategy` is `bistate->strategy !=
    /// NULL`. Extends by up to `extend_by` pages, returning the first new block,
    /// the (pinned) victim buffers, and the actual extension count. The first
    /// returned page is exclusive-locked (`EB_LOCK_FIRST`).
    pub fn extend_buffered_rel_by(
        rel: Oid,
        has_strategy: bool,
        extend_by: u32,
    ) -> PgResult<ExtendedRelation>
);

// --- bufpage.c — opaque-`Page` predicates / mutators -----------------------

seam_core::seam!(
    /// `PageIsAllVisible(BufferGetPage(buffer))` (`storage/bufpage.h`).
    pub fn page_is_all_visible(buffer: Buffer) -> PgResult<bool>
);

seam_core::seam!(
    /// `PageIsNew(BufferGetPage(buffer))` (`storage/bufpage.h`) — the page has
    /// never been initialized (`pd_upper == 0`).
    pub fn page_is_new(buffer: Buffer) -> PgResult<bool>
);

seam_core::seam!(
    /// `PageGetMaxOffsetNumber(BufferGetPage(buffer))` (`storage/bufpage.h`).
    pub fn page_get_max_offset_number(buffer: Buffer) -> PgResult<OffsetNumber>
);

seam_core::seam!(
    /// `PageGetHeapFreeSpace(BufferGetPage(buffer))` (`storage/page/bufpage.c`)
    /// — free space available for a new heap tuple (accounts for line-pointer
    /// limits).
    pub fn page_get_heap_free_space(buffer: Buffer) -> PgResult<Size>
);

seam_core::seam!(
    /// `PageInit(BufferGetPage(buffer), BufferGetPageSize(buffer), 0)`
    /// (`storage/page/bufpage.c`) — initialize a fresh page.
    pub fn page_init(buffer: Buffer) -> PgResult<()>
);

seam_core::seam!(
    /// `PageAddItem(BufferGetPage(buffer), (Item) tuple->t_data, tuple->t_len,
    /// InvalidOffsetNumber, false, true)` (`storage/page/bufpage.c`) — add the
    /// prepared heap tuple's data to the page, returning the assigned offset (or
    /// `InvalidOffsetNumber` on failure). `image` is the tuple's full
    /// contiguous on-disk byte image (`tuple->t_data[0 .. t_len]`, header +
    /// null-bitmap + column bytes), which the caller serializes from its
    /// `FormedTuple` via `heap_tuple_to_disk_image` — the decoded
    /// `HeapTupleData` carries only the fixed header, not the user-data area, so
    /// the contiguous image must cross the boundary as bytes (the C `Item`).
    pub fn page_add_item(buffer: Buffer, image: &[u8]) -> PgResult<OffsetNumber>
);

seam_core::seam!(
    /// Write `ctid` into the `t_ctid` field of the stored heap-tuple header at
    /// `offnum` on the page: the
    /// `((HeapTupleHeader) PageGetItem(...))->t_ctid = ctid` step of
    /// `RelationPutHeapTuple` (done only for non-speculative insertions).
    /// Seamed because it reads/writes the opaque page.
    pub fn set_stored_tuple_ctid(
        buffer: Buffer,
        offnum: OffsetNumber,
        ctid: ItemPointerData,
    ) -> PgResult<()>
);

// --- freespace.c — the free-space map --------------------------------------

seam_core::seam!(
    /// `GetPageWithFreeSpace(relation, len)` (`storage/freespace/freespace.c`).
    pub fn get_page_with_free_space(rel: Oid, len: Size) -> PgResult<BlockNumber>
);

seam_core::seam!(
    /// `RecordAndGetPageWithFreeSpace(relation, old_page, old_avail, needed)`
    /// (`storage/freespace/freespace.c`).
    pub fn record_and_get_page_with_free_space(
        rel: Oid,
        old_page: BlockNumber,
        old_avail: Size,
        needed: Size,
    ) -> PgResult<BlockNumber>
);

seam_core::seam!(
    /// `RecordPageWithFreeSpace(relation, heap_blk, space_avail)`
    /// (`storage/freespace/freespace.c`).
    pub fn record_page_with_free_space(
        rel: Oid,
        heap_blk: BlockNumber,
        space_avail: Size,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `FreeSpaceMapVacuumRange(relation, start, end)`
    /// (`storage/freespace/freespace.c`).
    pub fn free_space_map_vacuum_range(
        rel: Oid,
        start: BlockNumber,
        end: BlockNumber,
    ) -> PgResult<()>
);

// --- lmgr.c — the relation-extension lock waiter count ----------------------

seam_core::seam!(
    /// `RelationExtensionLockWaiterCount(relation)` (`storage/lmgr/lmgr.c`).
    pub fn relation_extension_lock_waiter_count(rel: Oid) -> PgResult<u32>
);

// --- visibilitymap.c — the all-visible vm pins ------------------------------

seam_core::seam!(
    /// `visibilitymap_pin(relation, heap_blk, &vmbuf)`
    /// (`access/heap/visibilitymap.c`) — ensure the vm page for `heap_blk` is
    /// pinned, returning the (possibly newly pinned) vm buffer.
    pub fn visibilitymap_pin(
        rel: Oid,
        heap_blk: BlockNumber,
        vmbuf: Buffer,
    ) -> PgResult<Buffer>
);

seam_core::seam!(
    /// `visibilitymap_pin_ok(heap_blk, vmbuf)`
    /// (`access/heap/visibilitymap.c`) — whether `vmbuf` already covers
    /// `heap_blk`.
    pub fn visibilitymap_pin_ok(heap_blk: BlockNumber, vmbuf: Buffer) -> PgResult<bool>
);

// --- relcache (utils/cache/relcache.c) + rel.h macros -----------------------

seam_core::seam!(
    /// `RelationGetNumberOfBlocks(relation)` (`storage/bufmgr.h`) — block count
    /// of the main fork.
    pub fn relation_get_number_of_blocks(rel: Oid) -> PgResult<BlockNumber>
);

seam_core::seam!(
    /// `RelationGetTargetBlock(relation)` (`utils/rel.h`) ==
    /// `rd_smgr ? rd_smgr->smgr_targblock : InvalidBlockNumber`.
    pub fn relation_get_target_block(rel: Oid) -> PgResult<BlockNumber>
);

seam_core::seam!(
    /// `RelationSetTargetBlock(relation, target_block)` (`utils/rel.h`) —
    /// writes `RelationGetSmgr(relation)->smgr_targblock`.
    pub fn relation_set_target_block(rel: Oid, target_block: BlockNumber) -> PgResult<()>
);

seam_core::seam!(
    /// `RelationGetTargetPageFreeSpace(relation, HEAP_DEFAULT_FILLFACTOR)`
    /// (`utils/rel.h`) == `BLCKSZ * (100 - fillfactor) / 100`, reading the
    /// relation's `StdRdOptions.fillfactor` (defaulting to `defaultff`).
    pub fn relation_get_target_page_free_space(rel: Oid, defaultff: i32) -> PgResult<Size>
);

seam_core::seam!(
    /// `RELATION_IS_LOCAL(relation)` (`utils/rel.h`) == `rd_islocaltemp ||
    /// rd_createSubid != InvalidSubTransactionId`.
    pub fn relation_is_local(rel: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `RelationGetRelationName(relation)` (`utils/rel.h`) — used only to format
    /// the "should be empty but is not" error. Returns an owned copy.
    pub fn relation_get_relation_name(rel: Oid) -> PgResult<String>
);
