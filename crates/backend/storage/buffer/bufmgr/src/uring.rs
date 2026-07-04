//! Prefetch reads landing directly in the pool (C 18 AsyncReadBuffers narrowed
//! to one block): the issuer pins a victim, claims the IO, and hands the pin
//! to its ring slot; completion (any thread) verifies + terminates; only the
//! issuer unpins, via collect/drain.

use types_core::{BlockNumber, Buffer, ForkNumber, BLCKSZ};
use types_error::PgResult;
use types_storage::buf::{PgAioWaitRef, BM_IO_ERROR, BM_VALID};
use types_storage::RelFileLocatorBackend;

use crate::buf_hdr::{BufferGetBlockPtr, GetBufferDescriptor, LockBufHdr, UnlockBufHdr};
use crate::pin::{ForgetBufferPin, UnpinBuffer, UnpinBufferNoOwner};
use crate::read::{page_is_verified, BufferAlloc, StartBufferIO, TerminateBufferIO};
use crate::PrefetchOutcome;

const SLOTS: usize = 128;

pub fn start_read(
    smgr: RelFileLocatorBackend,
    relpersistence: u8,
    forknum: ForkNumber,
    blkno: BlockNumber,
) -> PgResult<Option<PrefetchOutcome>> {
    collect_done();
    let (buffer, found) = BufferAlloc(smgr, relpersistence, forknum, blkno, &None)?;
    let desc = GetBufferDescriptor(buffer - 1);
    if found {
        UnpinBuffer(desc);
        return Ok(Some(PrefetchOutcome::Cached));
    }
    if !StartBufferIO(desc, true, true, false)? {
        UnpinBuffer(desc);
        return Ok(Some(PrefetchOutcome::Cached));
    }
    match smgr_seams::smgr_start_buffer_read::call(smgr, forknum, blkno, buffer) {
        Ok(true) => {
            // Pin ownership moves to the ring slot (C: AIO holds its own pin);
            // collect/drain on this thread is the only unpinner.
            ForgetBufferPin(buffer);
            crate::counters::read();
            Ok(Some(PrefetchOutcome::Issued))
        }
        Ok(false) => {
            TerminateBufferIO(desc, false, BM_IO_ERROR, false);
            UnpinBuffer(desc);
            Ok(None)
        }
        Err(e) => {
            TerminateBufferIO(desc, false, BM_IO_ERROR, false);
            UnpinBuffer(desc);
            Err(e)
        }
    }
}

pub fn collect_done() {
    if !aio_seams::uring_collect_done::is_installed() {
        return;
    }
    let mut out = [0i32; SLOTS];
    loop {
        let n = aio_seams::uring_collect_done::call(&mut out);
        for &b in &out[..n] {
            UnpinBufferNoOwner(GetBufferDescriptor(b - 1));
        }
        if n < out.len() {
            break;
        }
    }
}

pub(crate) fn drain_own() {
    if !aio_seams::uring_drain_own::is_installed() {
        return;
    }
    let mut out = [0i32; SLOTS];
    loop {
        let n = aio_seams::uring_drain_own::call(&mut out);
        for &b in &out[..n] {
            UnpinBufferNoOwner(GetBufferDescriptor(b - 1));
        }
        if n < out.len() {
            break;
        }
    }
}

pub fn uring_set_io_wref(buffer: Buffer, aio_index: u32, generation: u64) {
    let desc = GetBufferDescriptor(buffer - 1);
    let st = LockBufHdr(desc);
    // SAFETY: header lock held.
    unsafe {
        desc.set_io_wref(PgAioWaitRef {
            aio_index,
            generation_upper: (generation >> 32) as u32,
            generation_lower: generation as u32,
        })
    };
    UnlockBufHdr(desc, st);
}

pub fn uring_clear_io_wref(buffer: Buffer) {
    let desc = GetBufferDescriptor(buffer - 1);
    let st = LockBufHdr(desc);
    // SAFETY: header lock held.
    unsafe { desc.set_io_wref(PgAioWaitRef::default()) };
    UnlockBufHdr(desc, st);
}

/// Completion body — may run on any thread (foreign drain): shared state only.
/// Verification failures degrade to BM_IO_ERROR; the arriving backend's sync
/// re-read raises the user-facing error with its own context.
pub fn uring_read_complete(buffer: Buffer, res: i32) {
    uring_clear_io_wref(buffer);
    let desc = GetBufferDescriptor(buffer - 1);
    let ok = res == BLCKSZ as i32 && page_is_verified(BufferGetBlockPtr(buffer));
    TerminateBufferIO(desc, false, if ok { BM_VALID } else { BM_IO_ERROR }, false);
}
