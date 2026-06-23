#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! Bulk-write facility — an idiomatic Rust port of
//! `src/backend/storage/smgr/bulk_write.c`.
//!
//! `bulk_write.c` efficiently populates a NEW relation, bypassing the buffer
//! manager and calling `smgrextend` directly. It queues up to
//! `MAX_PENDING_WRITES` (== `XLR_MAX_BLOCK_ID` == 32) pages, WAL-logs them in
//! batches (`log_newpages`), fills nonsequential holes with zeroes, and at
//! finish registers / forces the appropriate fsync depending on WAL level and
//! whether a checkpoint raced ahead.
//!
//! bulk_write's own logic — the pending-write queue, the sort-by-blkno, the
//! batch flush that fills holes and dispatches `smgrextend` vs `smgrwrite`, the
//! `MAX_PENDING_WRITES` batching, and the checkpoint-race fsync decision in
//! `smgr_bulk_finish` — is ported in-crate 1:1. The storage manager
//! (`backend-storage-smgr-smgr`) and page checksum (`backend-storage-page`) are
//! DIRECT deps (both below bulkwrite); WAL (`log_newpages`/`GetRedoRecPtr`) and
//! the proc checkpoint-delay flag cross seams.
//!
//! ## Owned model (vs the C-ABI port)
//! C's `BulkWriteBuffer` (a pointer into a writer-owned `BLCKSZ` page) is the
//! safe owned page workspace [`::mcx::PgVec<u8>`]: `smgr_bulk_get_buf` hands back
//! a zeroed page, the caller fills it, and `smgr_bulk_write` takes ownership
//! back. The opaque `BulkWriteState *` is the type-erased
//! [`::bulkwrite_seams::BulkWriteState`] carrier; the real
//! engine state is [`BulkWriteEngine`], which this crate boxes into it.

use ::page::{PageMut, PageSetChecksumInplace};
use smgr as smgr;
use ::utils_error::{ereport, PgError, PgResult};
use ::mcx::{Mcx, PgVec};
use ::types_core::primitive::{BlockNumber, ForkNumber, XLogRecPtr};
use ::types_core::BLCKSZ;
use ::types_error::ERRCODE_OUT_OF_MEMORY;
use ::rel::Relation;
use ::types_storage::RelFileLocatorBackend;

use ::transam_xlog_seams::get_redo_rec_ptr as get_redo_rec_ptr_seam;
use ::xloginsert_seams::log_newpages as log_newpages_seam;
use ::lmgr_proc_seams::set_delay_chkpt_start as set_delay_chkpt_start_seam;
use ::bulkwrite_seams::{self as seam, BulkWriteState};

/// `MAX_PENDING_WRITES` (bulk_write.c:47) == `XLR_MAX_BLOCK_ID` (32).
const MAX_PENDING_WRITES: usize = 32;

/// `RELPERSISTENCE_PERMANENT` — `RelationNeedsWAL(rel)` is
/// `relpersistence == RELPERSISTENCE_PERMANENT`.
const RELPERSISTENCE_PERMANENT: u8 = types_tuple::access::RELPERSISTENCE_PERMANENT;

/// One queued page write (`PendingWrite` in bulk_write.c).
///
/// The page workspace handed out by `smgr_bulk_get_buf` is an `'mcx`-bound
/// [`::mcx::PgVec`]; the engine, however, is type-erased through `dyn Any`
/// (which is `'static`), so the engine cannot retain `'mcx` borrows. The page
/// bytes are therefore copied into an owned `Vec<u8>` on the way into the queue
/// (the same `BLCKSZ` page image, no behavioral difference — the original C
/// reuses one writer-owned buffer; we own a per-page copy).
struct PendingWrite {
    buf: Option<Vec<u8>>,
    blkno: BlockNumber,
    page_std: bool,
}

/// The real bulk-write engine state (C's `BulkWriteState`), type-erased into the
/// seam crate's [`BulkWriteState`] carrier (`dyn Any`, hence `'static`).
pub struct BulkWriteEngine {
    /// `smgr` — the relation's `RelFileLocatorBackend` (C: `SMgrRelation`).
    smgr: RelFileLocatorBackend,
    /// `forknum`.
    forknum: ForkNumber,
    /// `use_wal`.
    use_wal: bool,
    /// `npending` — number of queued writes (`<= MAX_PENDING_WRITES`).
    npending: usize,
    /// `relsize` — current known size of the fork (extends as we write).
    relsize: BlockNumber,
    /// `start_RedoRecPtr` — redo pointer sampled at start, for the
    /// checkpoint-race check in `smgr_bulk_finish`.
    start_RedoRecPtr: XLogRecPtr,
    /// `pending_writes[MAX_PENDING_WRITES]`.
    pending_writes: Vec<PendingWrite>,
}

// ===========================================================================
// bulk_write.c public API (bulk_write.h)
// ===========================================================================

/// `smgr_bulk_start_rel()` (bulk_write.c:86-92) — start a bulk write on a
/// relation fork, given a (relcache-resolved) relation.
///
/// ```c
/// return smgr_bulk_start_smgr(RelationGetSmgr(rel), forknum,
///                             RelationNeedsWAL(rel) || forknum == INIT_FORKNUM);
/// ```
pub fn smgr_bulk_start_rel<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    forknum: ForkNumber,
) -> PgResult<BulkWriteState<'mcx>> {
    // RelationGetSmgr(rel) == rel.rd_locator / rel.rd_backend.
    let smgr_rlocator = RelFileLocatorBackend {
        locator: rel.rd_locator,
        backend: rel.rd_backend,
    };
    // RelationNeedsWAL(rel) || forknum == INIT_FORKNUM
    let needs_wal = rel.rd_rel.relpersistence == RELPERSISTENCE_PERMANENT;
    let use_wal = needs_wal || forknum == ForkNumber::INIT_FORKNUM;
    smgr_bulk_start_smgr(mcx, smgr_rlocator, forknum, use_wal)
}

/// `smgr_bulk_start_smgr()` (bulk_write.c:99-121) — start a bulk write on a
/// relation fork without a relcache entry.
pub fn smgr_bulk_start_smgr<'mcx>(
    mcx: Mcx<'mcx>,
    smgr_rlocator: RelFileLocatorBackend,
    forknum: ForkNumber,
    use_wal: bool,
) -> PgResult<BulkWriteState<'mcx>> {
    // state->relsize = smgrnblocks(smgr, forknum);
    let relsize = smgr::smgrnblocks(smgr_rlocator, forknum)?;
    // state->start_RedoRecPtr = GetRedoRecPtr();
    let start_RedoRecPtr = get_redo_rec_ptr_seam::call();

    let engine = BulkWriteEngine {
        smgr: smgr_rlocator,
        forknum,
        use_wal,
        npending: 0,
        relsize,
        start_RedoRecPtr,
        pending_writes: Vec::new(),
    };
    BulkWriteState::new(mcx, engine)
}

/// `smgr_bulk_get_buf()` (bulk_write.c:346-350) — allocate a fresh, zeroed
/// `BLCKSZ` page workspace. Ownership transfers to [`smgr_bulk_write`].
pub fn smgr_bulk_get_buf<'mcx>(
    mcx: Mcx<'mcx>,
    _bulkstate: &mut BulkWriteState<'mcx>,
) -> PgResult<PgVec<'mcx, u8>> {
    // MemoryContextAllocAligned(bulkstate->memcxt, BLCKSZ, PG_IO_ALIGN_SIZE, 0);
    // The owned model returns a zeroed page; alignment is not a correctness
    // requirement for the buffered write path.
    let mut buf: PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, BLCKSZ)?;
    buf.resize(BLCKSZ, 0u8);
    Ok(buf)
}

/// `smgr_bulk_write()` (bulk_write.c:322-334) — queue a page write.
///
/// NB: takes ownership of `buf`. You are only allowed to write a given block
/// once as part of one bulk write operation.
pub fn smgr_bulk_write<'mcx>(
    bulkstate: &mut BulkWriteState<'mcx>,
    blocknum: BlockNumber,
    buf: PgVec<'mcx, u8>,
    page_std: bool,
) -> PgResult<()> {
    // Copy the page workspace into an owned buffer (the engine is `'static`,
    // see `PendingWrite`); the `'mcx` `buf` is dropped here (≡ the writer
    // releasing its slot back to the page allocator).
    let owned: Vec<u8> = buf.as_slice().to_vec();
    drop(buf);

    let flush = {
        let engine = engine_mut(bulkstate)?;
        // w = &bulkstate->pending_writes[bulkstate->npending++];
        debug_assert!(engine.npending < MAX_PENDING_WRITES);
        engine.pending_writes.push(PendingWrite {
            buf: Some(owned),
            blkno: blocknum,
            page_std,
        });
        engine.npending += 1;
        engine.npending == MAX_PENDING_WRITES
    };

    // if (bulkstate->npending == MAX_PENDING_WRITES) smgr_bulk_flush(bulkstate);
    if flush {
        smgr_bulk_flush(bulkstate)?;
    }
    Ok(())
}

/// `smgr_bulk_finish()` (bulk_write.c:129-222) — finish the bulk write: flush
/// remaining pages, then fsync / register-sync the relation per WAL level,
/// guarding against a concurrent checkpoint.
pub fn smgr_bulk_finish<'mcx>(mut bulkstate: BulkWriteState<'mcx>) -> PgResult<()> {
    // WAL-log and flush any remaining pages.
    smgr_bulk_flush(&mut bulkstate)?;

    let (is_temp, use_wal, smgr_rlocator, forknum, start_redo) = {
        let engine = engine_ref(&bulkstate)?;
        (
            engine.smgr.backend != ::types_core::primitive::INVALID_PROC_NUMBER,
            engine.use_wal,
            engine.smgr,
            engine.forknum,
            engine.start_RedoRecPtr,
        )
    };

    if is_temp {
        // Temporary relations don't need to be fsync'd, ever.
    } else if !use_wal {
        // Unlogged relation (conservatively assumed): needs smgrregistersync so
        // the checkpointer flushes it at the shutdown checkpoint.
        smgr::smgrregistersync(smgr_rlocator, forknum)?;
    } else {
        // Permanent relation, WAL-logged normally. We passed skipFsync=true, so
        // register the whole relation now; but if a checkpoint started after the
        // bulk write it missed our pages, so fsync the files now in that case.

        // Prevent a checkpoint from starting between GetRedoRecPtr() and
        // smgrregistersync().  MyProc->delayChkptFlags |= DELAY_CHKPT_START;
        set_delay_chkpt_start_seam::call(true);

        if start_redo != get_redo_rec_ptr_seam::call() {
            // A checkpoint occurred and didn't know about our writes; fsync now.
            set_delay_chkpt_start_seam::call(false);
            smgr::smgrimmedsync(smgr_rlocator, forknum)?;
            // elog(DEBUG1, "flushed relation because a checkpoint occurred
            //              concurrently"); — DEBUG1 diagnostic, no observable
            // effect on the bulk-write result.
        } else {
            smgr::smgrregistersync(smgr_rlocator, forknum)?;
            set_delay_chkpt_start_seam::call(false);
        }
    }
    Ok(())
}

// ===========================================================================
// bulk_write.c static (file-local) helpers
// ===========================================================================

/// `smgr_bulk_flush()` (bulk_write.c:241-312) — finish all pending writes: sort
/// by blkno, optionally `log_newpages` the batch (one `page_std` for the whole
/// batch), then per page set the checksum and dispatch `smgrextend` (filling
/// holes with zeroes) or `smgrwrite`, dropping each page (≡ `pfree`).
fn smgr_bulk_flush<'mcx>(bulkstate: &mut BulkWriteState<'mcx>) -> PgResult<()> {
    let engine = engine_mut(bulkstate)?;

    let npending = engine.npending;
    if npending == 0 {
        return Ok(());
    }

    // if (npending > 1) qsort(pending_writes, npending, ..., buffer_cmp);
    if npending > 1 {
        engine.pending_writes[..npending].sort_by(buffer_cmp);
    }

    // if (bulkstate->use_wal) log_newpages(...).
    if engine.use_wal {
        log_pending_writes(engine, npending)?;
    }

    let smgr_rlocator = engine.smgr;
    let forknum = engine.forknum;

    // for (int i = 0; i < npending; i++)
    for i in 0..npending {
        let blkno = engine.pending_writes[i].blkno;
        // Take ownership of the page so it is dropped (≡ pfree) at iteration end.
        let mut page = engine.pending_writes[i]
            .buf
            .take()
            .ok_or_else(|| internal("pending write missing its buffer"))?;

        // PageSetChecksumInplace(page, blkno);
        {
            let mut pm = PageMut::new(page.as_mut_slice())?;
            PageSetChecksumInplace(&mut pm, blkno);
        }

        // if (blkno >= bulkstate->relsize)
        if blkno >= engine.relsize {
            // Fill nonsequential holes with zeroes until we reach blkno (the
            // dummy pages aren't WAL-logged).
            while blkno > engine.relsize {
                // don't set checksum for all-zero page.
                let zero = vec![0u8; BLCKSZ];
                smgr::smgrextend(smgr_rlocator, forknum, engine.relsize, &zero, true)?;
                engine.relsize += 1;
            }
            // smgrextend(bulkstate->smgr, bulkstate->forknum, blkno, page, true);
            smgr::smgrextend(smgr_rlocator, forknum, blkno, &page, true)?;
            engine.relsize += 1;
        } else {
            // smgrwrite(bulkstate->smgr, bulkstate->forknum, blkno, page, true);
            smgr::smgrwrite(smgr_rlocator, forknum, blkno, &page, true)?;
        }
        // pfree(page) — drop the owned page.
        drop(page);
    }

    // bulkstate->npending = 0;
    engine.npending = 0;
    engine.pending_writes.clear();
    Ok(())
}

/// `buffer_cmp()` (bulk_write.c:224-236) — order pending writes by ascending
/// block number. PURE. We never see duplicate writes for the same block.
fn buffer_cmp(a: &PendingWrite, b: &PendingWrite) -> core::cmp::Ordering {
    debug_assert!(a.blkno != b.blkno);
    if a.blkno > b.blkno {
        core::cmp::Ordering::Greater
    } else {
        core::cmp::Ordering::Less
    }
}

/// The WAL-batch leg of `smgr_bulk_flush()` (bulk_write.c:264-290):
/// `log_newpages(&smgr_rlocator.locator, forknum, npending, blknos, pages,
/// page_std)`.
fn log_pending_writes(engine: &BulkWriteEngine, npending: usize) -> PgResult<()> {
    let mut blknos: Vec<BlockNumber> = Vec::new();
    blknos.try_reserve(npending).map_err(|_| oom("smgr_bulk_flush blknos"))?;
    let mut pages: Vec<&[u8]> = Vec::new();
    pages.try_reserve(npending).map_err(|_| oom("smgr_bulk_flush pages"))?;

    let mut page_std = true;

    for w in &engine.pending_writes[..npending] {
        blknos.push(w.blkno);
        let page = w
            .buf
            .as_deref()
            .ok_or_else(|| internal("pending write missing its buffer"))?;
        pages.push(page);
        // If any page uses !page_std, log them all as such.
        if !w.page_std {
            page_std = false;
        }
    }

    log_newpages_seam::call(engine.smgr.locator, engine.forknum, &blknos, &pages, page_std)
}

// ===========================================================================
// carrier downcast helpers + error builders
// ===========================================================================

fn engine_ref<'a>(state: &'a BulkWriteState<'_>) -> PgResult<&'a BulkWriteEngine> {
    state
        .downcast_ref::<BulkWriteEngine>()
        .ok_or_else(|| internal("BulkWriteState carries no BulkWriteEngine"))
}

fn engine_mut<'a>(state: &'a mut BulkWriteState<'_>) -> PgResult<&'a mut BulkWriteEngine> {
    state
        .downcast_mut::<BulkWriteEngine>()
        .ok_or_else(|| internal("BulkWriteState carries no BulkWriteEngine"))
}

fn oom(what: &str) -> PgError {
    ereport(::types_error::ERROR)
        .errcode(ERRCODE_OUT_OF_MEMORY)
        .errmsg_internal(format!("out of memory allocating {what}"))
        .into_error()
}

fn internal(what: &str) -> PgError {
    PgError::error(format!("bulk_write internal error: {what}"))
}

// ===========================================================================
// init_seams() — install every seam in backend-storage-smgr-bulkwrite-seams.
// ===========================================================================

/// Install every seam this unit OWNS (`backend-storage-smgr-bulkwrite-seams`).
pub fn init_seams() {
    seam::smgr_bulk_start_rel::set(smgr_bulk_start_rel);
    seam::smgr_bulk_start_smgr::set(smgr_bulk_start_smgr);
    seam::smgr_bulk_get_buf::set(smgr_bulk_get_buf);
    seam::smgr_bulk_write::set(smgr_bulk_write);
    seam::smgr_bulk_finish::set(smgr_bulk_finish);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pw(blkno: BlockNumber) -> PendingWrite {
        PendingWrite { buf: None, blkno, page_std: true }
    }

    #[test]
    fn max_pending_writes_is_xlr_max_block_id() {
        assert_eq!(MAX_PENDING_WRITES, 32);
    }

    #[test]
    fn buffer_cmp_orders_ascending_by_blkno() {
        let a = pw(3);
        let b = pw(7);
        assert_eq!(buffer_cmp(&a, &b), core::cmp::Ordering::Less);
        assert_eq!(buffer_cmp(&b, &a), core::cmp::Ordering::Greater);
    }
}
