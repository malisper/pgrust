//! BRIN WAL opcodes (`access/brin_xlog.h`), the resource-manager id, and thin
//! pass-throughs to the substrate seams (relcache / freespace / lmgr /
//! xloginsert) used by both `brin_pageops.c` and `brin_revmap.c`.

use utils_error::PgResult;
use types_core::primitive::BlockNumber;
use rel::Relation;
use types_core::RmgrId;
use types_storage::buf::Buffer;

pub use types_core::primitive::InvalidBlockNumber;

// ===========================================================================
// brin_xlog.h opcodes (high nibble of the WAL info byte).
// ===========================================================================

/// `XLOG_BRIN_CREATE_INDEX` (0x00) — emitted by `brin.c`'s `brinbuild` /
/// `brinbuildempty` (the build driver, which lives in
/// `backend-access-brin-insert-vacuum` and drives the metapage creation through
/// [`crate::brin_create_metapage`] / [`crate::brin_create_empty_metapage`]).
pub const XLOG_BRIN_CREATE_INDEX: u8 = 0x00;
pub const XLOG_BRIN_INSERT: u8 = 0x10;
pub const XLOG_BRIN_UPDATE: u8 = 0x20;
pub const XLOG_BRIN_SAMEPAGE_UPDATE: u8 = 0x30;
pub const XLOG_BRIN_REVMAP_EXTEND: u8 = 0x40;
pub const XLOG_BRIN_DESUMMARIZE: u8 = 0x50;

/// `XLOG_BRIN_INIT_PAGE` (brin_xlog.h) — page should be re-initialized.
pub const XLOG_BRIN_INIT_PAGE: u8 = 0x80;

/// `RM_BRIN_ID` (rmgrlist.h) — the BRIN resource-manager id (13). Not yet in
/// `types-wal`; grounded here, identical to how `hash-core` carries
/// `RM_HASH_ID` until those ids migrate to the shared types crate.
pub const RM_BRIN_ID: RmgrId = 13;

// ===========================================================================
// relcache (utils/rel.h) — plain field reads via the relcache seam.
// ===========================================================================

/// `RelationNeedsWAL(rel)`.
pub fn relation_needs_wal(rel: &Relation<'_>) -> bool {
    relcache_seams::relation_needs_wal::call(rel)
}

/// `RELATION_IS_LOCAL(rel)`.
pub fn relation_is_local(rel: &Relation<'_>) -> bool {
    relcache_seams::relation_is_local::call(rel)
}

/// `RelationGetNumberOfBlocks(rel)`.
pub fn relation_get_number_of_blocks(rel: &Relation<'_>) -> PgResult<BlockNumber> {
    relcache_seams::relation_get_number_of_blocks::call(rel)
}

// ===========================================================================
// freespace (freespace.c).
// ===========================================================================

/// `GetPageWithFreeSpace(rel, spaceNeeded)`.
pub fn get_page_with_free_space(rel: &Relation<'_>, space_needed: usize) -> PgResult<BlockNumber> {
    freespace_seams::get_page_with_free_space::call(rel, space_needed)
}

/// `RecordPageWithFreeSpace(rel, heapBlk, spaceAvail)`.
pub fn record_page_with_free_space(
    rel: &Relation<'_>,
    heap_blk: BlockNumber,
    space_avail: usize,
) -> PgResult<()> {
    freespace_seams::record_page_with_free_space::call(rel, heap_blk, space_avail)
}

/// `RecordAndGetPageWithFreeSpace(rel, oldPage, oldSpaceAvail, spaceNeeded)`.
pub fn record_and_get_page_with_free_space(
    rel: &Relation<'_>,
    old_page: BlockNumber,
    old_space_avail: usize,
    space_needed: usize,
) -> PgResult<BlockNumber> {
    freespace_seams::record_and_get_page_with_free_space::call(
        rel,
        old_page,
        old_space_avail,
        space_needed,
    )
}

/// `FreeSpaceMapVacuumRange(rel, start, end)`.
pub fn free_space_map_vacuum_range(
    rel: &Relation<'_>,
    start: BlockNumber,
    end: BlockNumber,
) -> PgResult<()> {
    freespace_seams::free_space_map_vacuum_range::call(rel, start, end)
}

// ===========================================================================
// lmgr (lmgr.c) — relation-extension lock guard.
// ===========================================================================

pub use lmgr_seams::lock_relation_for_extension;
pub use lmgr_seams::RelationExtensionLockGuard;

// ===========================================================================
// xloginsert (xloginsert.c).
// ===========================================================================

/// `XLogBeginInsert()`.
pub fn xlog_begin_insert() -> PgResult<()> {
    xloginsert_seams::xlog_begin_insert::call()
}

/// `XLogRegisterData(data, len)`.
pub fn xlog_register_data(data: &[u8]) -> PgResult<()> {
    xloginsert_seams::xlog_register_data::call(data)
}

/// `XLogRegisterBuffer(block_id, buffer, flags)`.
pub fn xlog_register_buffer(block_id: u8, buffer: Buffer, flags: u8) -> PgResult<()> {
    xloginsert_seams::xlog_register_buffer::call(block_id, buffer, flags)
}

/// `XLogRegisterBufData(block_id, data, len)`.
pub fn xlog_register_buf_data(block_id: u8, data: &[u8]) -> PgResult<()> {
    xloginsert_seams::xlog_register_buf_data::call(block_id, data)
}

/// `XLogInsert(rmid, info)`.
pub fn xlog_insert_record(rmid: RmgrId, info: u8) -> PgResult<types_core::XLogRecPtr> {
    xloginsert_seams::xlog_insert_record::call(rmid, info)
}

/// `log_newpage_buffer(buffer, page_std)`.
pub fn log_newpage_buffer(buffer: Buffer, page_std: bool) -> PgResult<types_core::XLogRecPtr> {
    xloginsert_seams::log_newpage_buffer::call(buffer, page_std)
}
