// Direct XLogInsert path: no seam on the per-record path (AGENTS.md perf
// addendum). Block references are resolved from state the caller already
// holds (rd_locator + the tuple TID), where C's XLogRegisterBuffer re-derives
// them via BufferGetTag.
use ::types_core::{BlockNumber, Buffer, ForkNumber, BLCKSZ};
#[cfg(not(test))]
use ::types_core::XLogRecPtr;
#[cfg(not(test))]
use ::types_error::PgResult;
use ::types_storage::RelFileLocator;

pub(crate) use ::xloginsert::RegBlock;

#[inline(always)]
pub(crate) fn reg_block<'a>(
    block_id: u8,
    rlocator: RelFileLocator,
    block: BlockNumber,
    buffer: Buffer,
    flags: u8,
    bufdata: &'a [&'a [u8]],
) -> RegBlock<'a> {
    let page = ::bufmgr_seams::buffer_page_ptr(buffer).as_ptr() as *const u8;
    RegBlock {
        block_id,
        rlocator,
        forknum: ForkNumber::MAIN_FORKNUM,
        block,
        // SAFETY: caller holds the pin + exclusive content lock for the
        // record (XLogRegisterBuffer contract); page is a BLCKSZ image.
        page: unsafe { core::slice::from_raw_parts(page, BLCKSZ) },
        flags,
        bufdata,
    }
}

// upstream f581fa729d8e (18.5): Fix VM clear WAL logging by registering VM blocks
// XLogRegisterBuffer(block_id, vmbuffer, 0): the visibility-map page whose
// bits the record cleared. No REGBUF_STANDARD (a VM page has no hole to
// skip), so an FPI, when one is due, covers the whole page.
#[inline(always)]
pub(crate) fn reg_vm_block<'a>(
    block_id: u8,
    rlocator: RelFileLocator,
    vmb: &visibilitymap::VmBuffer,
) -> RegBlock<'a> {
    let page = ::bufmgr_seams::buffer_page_ptr(vmb.buffer()).as_ptr() as *const u8;
    RegBlock {
        block_id,
        rlocator,
        forknum: ForkNumber::VISIBILITYMAP_FORKNUM,
        block: vmb.block_number(),
        // SAFETY: caller holds the pin + exclusive content lock on the VM
        // buffer for the record (XLogRegisterBuffer contract); page is a
        // BLCKSZ image.
        page: unsafe { core::slice::from_raw_parts(page, BLCKSZ) },
        flags: 0,
        bufdata: &[],
    }
}

#[cfg(not(test))]
#[inline(always)]
pub(crate) fn insert_record(
    rmid: u8,
    info: u8,
    record_flags: u8,
    main_data: &[&[u8]],
    blocks: &[RegBlock<'_>],
) -> PgResult<XLogRecPtr> {
    ::xloginsert::insert_record(rmid, info, record_flags, main_data, blocks)
}

#[cfg(test)]
pub(crate) use crate::tests::wal_insert_record_hook as insert_record;
