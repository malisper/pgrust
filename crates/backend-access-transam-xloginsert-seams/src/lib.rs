//! Seam declarations for the `backend-access-transam-xloginsert` unit
//! (`access/transam/xloginsert.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::primitive::{BlockNumber, ForkNumber};
use types_core::{RmgrId, XLogRecPtr};
use types_error::PgResult;
use types_storage::{Buffer, RelFileLocator};

seam_core::seam!(
    /// One whole-record insertion: C's `XLogBeginInsert()`, one
    /// `XLogRegisterData(data, len)` per `fragments` entry (in order),
    /// `XLogSetRecordFlags(flags)` (skipped when `flags == 0`), then
    /// `XLogInsert(rmid, info)`. Returns the record's end LSN. The ambient
    /// rdata-chain registration protocol stays on the owner's side of the
    /// boundary.
    pub fn xlog_insert(
        rmid: RmgrId,
        info: u8,
        flags: u8,
        fragments: &[&[u8]],
    ) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    /// `XLogBeginInsert()` — start building a WAL record. `elog(ERROR)`s if
    /// already in insert mode or during recovery.
    pub fn xlog_begin_insert() -> PgResult<()>
);

seam_core::seam!(
    /// `XLogRegisterData(data, len)` — append one chunk of record data (the
    /// installed impl copies the bytes; C keeps the caller's pointer alive
    /// until `XLogInsert`). `elog(ERROR)` on too many data chunks.
    pub fn xlog_register_data(data: &[u8]) -> PgResult<()>
);

seam_core::seam!(
    /// `XLogRegisterBuffer(block_id, buffer, flags)` — register a buffer with
    /// the record under construction (`flags` is the `REGBUF_*` bitmask).
    /// `elog(ERROR)` on a bad block_id or double registration.
    pub fn xlog_register_buffer(block_id: u8, buffer: Buffer, flags: u8) -> PgResult<()>
);

seam_core::seam!(
    /// `XLogRegisterBlock(block_id, rlocator, forknum, blknum, page, flags)`
    /// (xloginsert.c) — register a block with the record by an explicit
    /// relation/fork/block identity and a caller-supplied `page` image (rather
    /// than a live `Buffer`). Used by `heap_inplace_update_and_unlock`, which
    /// builds the post-mutation block image on the stack so the FPI captures
    /// the page as it will look *after* the in-place memcpy (WAL-before-data
    /// ordering). `page` is `BLCKSZ` long. `elog(ERROR)` on a bad block_id or
    /// double registration. **Owned by `backend-access-transam-xloginsert`;
    /// uninstalled — and panics — until that unit lands.**
    pub fn xlog_register_block(
        block_id: u8,
        rlocator: RelFileLocator,
        forknum: ForkNumber,
        blknum: BlockNumber,
        page: &[u8],
        flags: u8,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `XLogRegisterBufData(block_id, data, len)` — append per-block data to a
    /// previously registered buffer (the installed impl copies the bytes; C
    /// keeps the caller's pointer alive until `XLogInsert`). `elog(ERROR)` on
    /// an unregistered block or too much data.
    pub fn xlog_register_buf_data(block_id: u8, data: &[u8]) -> PgResult<()>
);

seam_core::seam!(
    /// `XLogInsert(rmid, info)` — assemble and insert the WAL record built up
    /// by the preceding begin/register calls, returning its end LSN.
    /// `elog(ERROR)`/`PANIC` on insertion failure.
    pub fn xlog_insert_record(rmid: RmgrId, info: u8) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    /// `XLogSetRecordFlags(flags)` — e.g. `XLOG_INCLUDE_ORIGIN`.
    pub fn xlog_set_record_flags(flags: u8)
);

seam_core::seam!(
    /// `XLogResetInsertion()` — forget a partially-constructed record (abort
    /// path).
    pub fn xlog_reset_insertion()
);

// --- backend-utils-init-postinit consumer (xloginsert.c) ---

seam_core::seam!(
    /// `InitXLogInsert()` (xloginsert.c): allocate the backend-local WAL record
    /// construction buffers. `Err` carries its OOM surface.
    pub fn init_xlog_insert() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `log_newpage_buffer(buffer, page_std)` (xloginsert.c) — emit an
    /// `XLOG_FPI` full-page-image WAL record for `buffer` and return the
    /// record's end LSN. `page_std` is whether the page follows the standard
    /// layout (the FSM truncate caller passes `false`). `Err` carries the WAL
    /// insertion `ereport(ERROR)`s.
    pub fn log_newpage_buffer(
        buffer: Buffer,
        page_std: bool,
    ) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    /// `log_newpages(rlocator, forknum, num_pages, blknos, pages, page_std)`
    /// (xloginsert.c) — WAL-log a batch of full-page images in as few records
    /// as possible (up to `XLR_MAX_BLOCK_ID` per record). `bulk_write.c`'s
    /// `smgr_bulk_flush` uses it to log a flushed batch. `blknos[i]` is the
    /// block number of `pages[i]` (each a `BLCKSZ` page image). `Err` carries
    /// the WAL insertion `ereport(ERROR)`s.
    pub fn log_newpages(
        rlocator: RelFileLocator,
        forknum: ForkNumber,
        blknos: &[BlockNumber],
        pages: &[&[u8]],
        page_std: bool,
    ) -> PgResult<()>
);
