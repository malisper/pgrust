// bulk_write.c. Page buffers are boxed (bulk-build lifetime, owned by the
// state — C's memcxt palloc), not arena-resident.
#![allow(non_snake_case)]

use types_core::{BlockNumber, ForkNumber, InvalidSubTransactionId, XLogRecPtr, BLCKSZ, INVALID_PROC_NUMBER};
use types_error::PgResult;
use types_rel::Relation;
use types_storage::{RelFileLocatorBackend, DELAY_CHKPT_START};

pub const MAX_PENDING_WRITES: usize = xloginsert::XLR_MAX_BLOCK_ID;

// PGIOAlignedBlock (c.h): I/O-aligned page image.
#[repr(align(4096))]
pub struct AlignedPage(pub [u8; BLCKSZ]);

pub struct BulkWriteBuffer(Box<AlignedPage>);

impl BulkWriteBuffer {
    #[inline]
    pub fn page_mut(&mut self) -> &mut [u8; BLCKSZ] {
        &mut self.0 .0
    }
}

struct PendingWrite {
    buf: Box<AlignedPage>,
    blkno: BlockNumber,
    page_std: bool,
}

pub struct BulkWriteState {
    smgr: RelFileLocatorBackend,
    forknum: ForkNumber,
    use_wal: bool,
    pending_writes: Vec<PendingWrite>,
    relsize: BlockNumber,
    start_RedoRecPtr: XLogRecPtr,
}

fn relation_needs_wal(rel: &Relation<'_>) -> bool {
    rel.is_permanent()
        && (transam_xlog_seams::xlog_standby_info_active::call()
            || (rel.rd_createSubid.get() == InvalidSubTransactionId
                && rel.rd_firstRelfilelocatorSubid.get() == InvalidSubTransactionId))
}

pub fn smgr_bulk_start_rel(rel: &Relation<'_>, forknum: ForkNumber) -> PgResult<BulkWriteState> {
    smgr::RelationGetSmgr(rel)?;
    smgr_bulk_start_smgr(
        RelFileLocatorBackend { locator: rel.rd_locator.get(), backend: rel.rd_backend },
        forknum,
        relation_needs_wal(rel) || forknum == ForkNumber::INIT_FORKNUM,
    )
}

pub fn smgr_bulk_start_smgr(
    smgr_key: RelFileLocatorBackend,
    forknum: ForkNumber,
    use_wal: bool,
) -> PgResult<BulkWriteState> {
    Ok(BulkWriteState {
        smgr: smgr_key,
        forknum,
        use_wal,
        pending_writes: Vec::with_capacity(MAX_PENDING_WRITES),
        relsize: smgr::smgrnblocks(smgr_key, forknum)?,
        start_RedoRecPtr: transam_xlog::GetRedoRecPtr(),
    })
}

pub fn smgr_bulk_get_buf(_state: &BulkWriteState) -> BulkWriteBuffer {
    BulkWriteBuffer(Box::new(AlignedPage([0u8; BLCKSZ])))
}

pub fn smgr_bulk_write(
    state: &mut BulkWriteState,
    blocknum: BlockNumber,
    buf: BulkWriteBuffer,
    page_std: bool,
) -> PgResult<()> {
    state.pending_writes.push(PendingWrite { buf: buf.0, blkno: blocknum, page_std });
    if state.pending_writes.len() >= MAX_PENDING_WRITES {
        smgr_bulk_flush(state)?;
    }
    Ok(())
}

pub fn smgr_bulk_finish(mut state: BulkWriteState) -> PgResult<()> {
    smgr_bulk_flush(&mut state)?;

    if state.smgr.backend != INVALID_PROC_NUMBER {
        // Temporary relations don't need to be fsync'd, ever.
    } else if !state.use_wal {
        smgr::smgrregistersync(state.smgr, state.forknum)?;
    } else {
        let proc = lmgr_proc::GetPGProcByNumber(lmgr_proc::MyProc().expect("MyProc is not set"));
        use core::sync::atomic::Ordering::Relaxed;
        debug_assert_eq!(proc.delayChkptFlags.load(Relaxed) & DELAY_CHKPT_START, 0);
        proc.delayChkptFlags.fetch_or(DELAY_CHKPT_START, Relaxed);
        if state.start_RedoRecPtr != transam_xlog::GetRedoRecPtr() {
            proc.delayChkptFlags.fetch_and(!DELAY_CHKPT_START, Relaxed);
            smgr::smgrimmedsync(state.smgr, state.forknum)?;
        } else {
            smgr::smgrregistersync(state.smgr, state.forknum)?;
            proc.delayChkptFlags.fetch_and(!DELAY_CHKPT_START, Relaxed);
        }
    }
    Ok(())
}

fn smgr_bulk_flush(state: &mut BulkWriteState) -> PgResult<()> {
    if state.pending_writes.is_empty() {
        return Ok(());
    }
    state.pending_writes.sort_by_key(|w| w.blkno);

    if state.use_wal {
        let blknos: Vec<BlockNumber> = state.pending_writes.iter().map(|w| w.blkno).collect();
        let page_std = state.pending_writes.iter().all(|w| w.page_std);
        let mut pages: Vec<&mut [u8]> = state
            .pending_writes
            .iter_mut()
            .map(|w| &mut w.buf.0[..])
            .collect();
        xloginsert::log_newpages(
            &state.smgr.locator,
            state.forknum,
            &blknos,
            &mut pages,
            page_std,
        )?;
    }

    for w in state.pending_writes.drain(..) {
        let mut page = w.buf;
        bufmgr::PageSetChecksumInplace(&mut page.0, w.blkno);
        if w.blkno >= state.relsize {
            static ZERO_BUFFER: AlignedPage = AlignedPage([0u8; BLCKSZ]);
            while w.blkno > state.relsize {
                smgr::smgrextend(state.smgr, state.forknum, state.relsize, &ZERO_BUFFER.0, true)?;
                state.relsize += 1;
            }
            smgr::smgrextend(state.smgr, state.forknum, w.blkno, &page.0[..], true)?;
            state.relsize += 1;
        } else {
            smgr::smgrwrite(state.smgr, state.forknum, w.blkno, &page.0[..], true)?;
        }
    }
    Ok(())
}
