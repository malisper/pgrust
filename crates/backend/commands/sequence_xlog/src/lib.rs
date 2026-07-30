//! sequence.c rmgr half: XLOG_SEQ_LOG redo + on-page constants.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use types_core::{OffsetNumber, BLCKSZ};
use types_error::{PgError, PgResult};
use types_storage::bufpage::{PageMut, PageTemp};
use xlogreader_seams::XLogReaderState;
use xlogutils::{XLogFlushBufferForRedoIfInit, XLogInitBufferForRedo};

pub const SEQ_MAGIC: u32 = 0x1717;
pub const XLOG_SEQ_LOG: u8 = 0x00;
// sizeof(xl_seq_rec): one RelFileLocator.
pub const SizeOfXlSeqRec: usize = 12;
const SizeOfSequenceMagic: usize = 4;
const XLR_INFO_MASK: u8 = 0x0F;
const FIRST_OFFSET_NUMBER: OffsetNumber = 1;

pub fn seq_page_init(page: &mut PageMut<'_>) {
    page.init(SizeOfSequenceMagic);
    let off = page.as_ref().pd_special() as usize;
    // SAFETY: pd_special in bounds after init; magic fits.
    unsafe {
        page.as_ref()
            .as_ptr()
            .cast_mut()
            .add(off)
            .cast::<u32>()
            .write_unaligned(SEQ_MAGIC)
    };
}

pub fn seq_page_magic(page: &types_storage::bufpage::PageRef<'_>) -> u32 {
    let off = page.pd_special() as usize;
    assert!(off <= BLCKSZ - SizeOfSequenceMagic);
    // SAFETY: bounds asserted above.
    unsafe { page.as_ptr().add(off).cast::<u32>().read_unaligned() }
}

#[track_caller]
#[cold]
fn panic_err(msg: String) -> Box<PgError> {
    Box::new(PgError::new(types_error::PANIC, msg))
}

pub fn seq_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let rec = record.record.as_ref().expect("seq_redo with no decoded record");
    let info = rec.xl_info & !XLR_INFO_MASK;
    if info != XLOG_SEQ_LOG {
        return Err(panic_err(format!("seq_redo: unknown op code {info}")));
    }
    // SAFETY: decode-buffer bytes live for this callback.
    let main = unsafe { rec.main_data_bytes() };
    let item = &main[SizeOfXlSeqRec..];

    let buffer = XLogInitBufferForRedo(record, 0)?;

    // Whole-page rebuild in local workspace, memcpy'd in (hot-standby rule).
    let mut local = PageTemp::new(BLCKSZ).map_err(Box::new)?;
    {
        // SAFETY: local is an owned BLCKSZ buffer.
        let ptr = core::ptr::NonNull::new(local.as_mut_bytes().as_mut_ptr()).expect("PageTemp ptr");
        let mut page = unsafe { PageMut::from_raw(ptr) };
        seq_page_init(&mut page);
        if page.add_item(item, FIRST_OFFSET_NUMBER, 0).is_none() {
            return Err(panic_err("seq_redo: failed to add item to page".into()));
        }
        page.set_lsn(record.EndRecPtr);
    }

    let raw = bufmgr_seams::buffer_get_page::call(buffer);
    // SAFETY: buffer pinned + exclusively locked by XLogInitBufferForRedo.
    unsafe { core::ptr::copy_nonoverlapping(local.as_bytes().as_ptr(), raw.as_ptr(), BLCKSZ) };
    bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    XLogFlushBufferForRedoIfInit(record, 0, buffer)?;
    bufmgr_seams::lock_buffer::call(buffer, bufmgr_seams::BUFFER_LOCK_UNLOCK)?;
    bufmgr_seams::release_buffer::call(buffer)
}
