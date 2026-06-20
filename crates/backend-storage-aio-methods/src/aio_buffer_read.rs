//! The AIO-engine half of the buffer manager's explicit multi-block read
//! pipeline (`StartReadBuffers`/`WaitReadBuffers`/`AsyncReadBuffers`, bufmgr.c).
//!
//! The buffer manager drives a cold multi-block read by acquiring an AIO handle
//! (`pgaio_io_acquire`), binding the buffer-readv completion vtable +
//! the run of buf_ids to it (`pgaio_io_register_callbacks` +
//! `pgaio_io_set_handle_data_32`), then submitting the vectored read
//! (`smgrstartreadv`) and finally waiting (`pgaio_wref_wait`). Under
//! `io_method = sync` the read is performed synchronously inline at stage time
//! (`pgaio_io_perform_synchronously`), so the shared completion callback
//! (`buffer_readv_complete`, owned by bufmgr) runs before the submit returns.
//!
//! These functions install the five `backend-storage-buffer-bufmgr-seams`
//! buffer-read handle seams over the real engine. They mirror the C call
//! sequence inside `AsyncReadBuffers` (bufmgr.c:1764) and `smgrstartreadv`
//! (smgr.c:753) / `mdstartreadv` (md.c:986): set the smgr target on the handle,
//! stage the `PGAIO_OP_READV`, and let the engine's synchronous-execution path
//! perform the read + drive the completion callbacks.
//!
//! The actual `preadv` (`pgaio_io_perform_synchronously` -> the
//! `pgaio_perform_io_syscall` seam) and the per-buffer page verification +
//! `TerminateBufferIO` (`buffer_readv_complete`, the `pgaio_cb_*` seams) are
//! installed by the buffer manager, which owns those data structures — exactly
//! as in C, where those bodies live in `bufmgr.c`/`md.c`.

extern crate alloc;

use types_core::primitive::{BlockNumber, ForkNumber};
use types_error::PgResult;
use types_storage::buf::PgAioWaitRef as BufAioWaitRef;
use types_storage::RelFileLocatorBackend;

use crate::aio::{
    ioh, pgaio_io_acquire, pgaio_io_get_wref, pgaio_io_stage, pgaio_wref_check_done,
    pgaio_wref_wait,
};
use crate::aio_callback::{
    pgaio_io_register_callbacks, pgaio_io_set_handle_data_32, PGAIO_HCB_LOCAL_BUFFER_READV,
    PGAIO_HCB_SHARED_BUFFER_READV,
};
use crate::aio_target::pgaio_io_set_target;
use crate::{
    take_pgaio_last_return, PgAioReturn, PgAioTargetData, PGAIO_HF_SYNCHRONOUS, PGAIO_OP_READV,
    PGAIO_TID_SMGR,
};

/// Convert the buffer-manager-facing [`BufAioWaitRef`] (carried in the
/// `BufferDesc` for layout parity, `types_storage`) to the engine's own
/// `PgAioWaitRef`. The two are layout-identical three-`u32` references.
fn to_engine_wref(w: BufAioWaitRef) -> crate::PgAioWaitRef {
    crate::PgAioWaitRef {
        aio_index: w.aio_index,
        generation_upper: w.generation_upper,
        generation_lower: w.generation_lower,
    }
}

/// Convert the engine's `PgAioWaitRef` back to the buffer-manager-facing one.
fn to_buf_wref(w: crate::PgAioWaitRef) -> BufAioWaitRef {
    BufAioWaitRef {
        aio_index: w.aio_index,
        generation_upper: w.generation_upper,
        generation_lower: w.generation_lower,
    }
}

/// `pgaio_io_acquire(CurrentResourceOwner, &operation->io_return)` +
/// `pgaio_io_set_target(ioh, PGAIO_TID_SMGR)` (bufmgr.c `AsyncReadBuffers`).
///
/// Acquire an AIO handle for one in-flight buffer read and return its wait
/// reference. The issuer-owned `PgAioReturn` (C's `&operation->io_return`) is
/// recorded on the handle so the completion path writes the distilled result
/// back to the issuer (the value is published into the backend-local return
/// slot at reclaim time; see `wait_read_buffers`). The smgr target is marked
/// here so the staging assertions (`pgaio_io_has_target`) hold; the target's
/// per-block data is filled by `start_read_buffers` (smgrstartreadv).
///
/// The resource owner is left unset: under `io_method = sync` the read
/// completes and the handle is reclaimed inline within `start_read_buffers`,
/// so there is never an in-flight handle for resowner cleanup to chase (the
/// reclaim path already releases any owner). This matches the single-block
/// synchronous read path, which likewise performs the read without a resowner
/// round-trip.
pub fn pgaio_io_acquire_for_buffer_read() -> PgResult<BufAioWaitRef> {
    let ret = PgAioReturn::default();
    let ioh_index = pgaio_io_acquire(None, Some(ret))?;
    pgaio_io_set_target(ioh_index, PGAIO_TID_SMGR);
    Ok(to_buf_wref(pgaio_io_get_wref(ioh_index)))
}

/// `pgaio_io_register_callbacks(ioh, PGAIO_HCB_{SHARED,LOCAL}_BUFFER_READV,
/// cb_data)` + `pgaio_io_set_handle_data_32(ioh, io_buffers, len)`
/// (bufmgr.c `AsyncReadBuffers`).
///
/// Bind the buffer-readv completion vtable to the handle (`cb_data` is the
/// `READ_BUFFERS_*` flag bitmask the per-buffer completion consults) and record
/// the run of 0-based buf_ids the IO covers. `is_temp` selects the LOCAL vs
/// SHARED completion callback. When the issuer requested a synchronous read
/// (`READ_BUFFERS_SYNCHRONOUSLY`), set `PGAIO_HF_SYNCHRONOUS` so the handle is
/// executed inline regardless of the configured IO method (mirroring C's
/// `pgaio_io_set_flag(ioh, PGAIO_HF_SYNCHRONOUS)` in `AsyncReadBuffers`).
pub fn pgaio_register_callbacks_for_buffer_read(
    wref: BufAioWaitRef,
    io_buffers: &[i32],
    flags: u8,
    synchronous: bool,
    is_temp: bool,
) -> PgResult<()> {
    let ioh_index = wref.aio_index as usize;

    if synchronous {
        let h = ioh(ioh_index);
        let mut d = h.data();
        d.flags |= PGAIO_HF_SYNCHRONOUS;
    }

    let cb_id = if is_temp {
        PGAIO_HCB_LOCAL_BUFFER_READV
    } else {
        PGAIO_HCB_SHARED_BUFFER_READV
    };
    pgaio_io_register_callbacks(ioh_index, cb_id, flags)?;

    // `pgaio_io_set_handle_data_32(ioh, io_buffers, len)` — the run of Buffer
    // ids (positive shared `buf_id + 1`, or negative local handles) the
    // completion callback iterates.
    let data: alloc::vec::Vec<u32> = io_buffers.iter().map(|&b| b as u32).collect();
    pgaio_io_set_handle_data_32(ioh_index, &data);
    Ok(())
}

/// `smgrstartreadv(ioh, operation->smgr, forknum, blocknum, BufferGetBlock(..),
/// io_buffers_len)` (bufmgr.c `AsyncReadBuffers` -> smgr.c `smgrstartreadv` ->
/// md.c `mdstartreadv`).
///
/// Submit the vectored read of `io_buffers_len` consecutive blocks starting at
/// `blocknum` into the run previously registered on the handle. This fills the
/// handle's smgr target data (`pgaio_io_set_target_smgr`) and stages the
/// `PGAIO_OP_READV`; under `io_method = sync` (`PGAIO_HF_SYNCHRONOUS` set by
/// `pgaio_register_callbacks`) the engine executes the read synchronously at
/// stage time (`pgaio_io_perform_synchronously` -> the `pgaio_perform_io_syscall`
/// seam, which performs the `smgrreadv` into the run's blocks) and drives the
/// shared completion callback (`buffer_readv_complete`, the `pgaio_cb_*` seams)
/// before returning. On return the issuer's return slot carries the actual
/// blocks-read count + status.
pub fn start_read_buffers_aio(
    wref: BufAioWaitRef,
    rlocator: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    io_buffers_len: i32,
) -> PgResult<()> {
    let ioh_index = wref.aio_index as usize;

    // `pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks, is_temp)`
    // (md.c `mdstartreadv`) — record which relation/fork/block range this IO
    // covers so the synchronous read syscall + the completion callback can find
    // the blocks. `is_temp`/`skip_fsync` are preserved from any prior target
    // marking (acquire set only the target id, not the data).
    {
        let h = ioh(ioh_index);
        let mut d = h.data();
        let prev = d.target_data;
        d.target_data = PgAioTargetData {
            rlocator_spc_oid: rlocator.locator.spcOid,
            rlocator_db_oid: rlocator.locator.dbOid,
            rlocator_rel_number: rlocator.locator.relNumber,
            block: blocknum,
            nblocks: io_buffers_len as u16,
            fork: forknum as u8,
            is_temp: prev.is_temp,
            skip_fsync: prev.skip_fsync,
        };
    }

    // `pgaio_io_start_readv(ioh, fd, iovcnt, offset)` reduces, under the
    // synchronous IO method, to staging the readv op and performing it inline.
    // The op_data fd/offset/iov_length the async methods set are not consulted
    // by the synchronous syscall (it reads the smgr target data instead), so we
    // stage directly with the run length as the iovec count.
    {
        let h = ioh(ioh_index);
        let mut d = h.data();
        d.op_data.iov_length = io_buffers_len as u16;
    }

    // Drive the stage path. With PGAIO_HF_SYNCHRONOUS set this runs the stage
    // callbacks (buffer_stage_common), then performs the read synchronously and
    // runs the completion callbacks, reclaiming the handle inline.
    pgaio_io_stage(ioh_index, PGAIO_OP_READV)?;
    Ok(())
}

/// `pgaio_wref_wait(&operation->io_wref)` (bufmgr.c `WaitReadBuffers`).
///
/// Block until the in-flight read completes, then return its distilled
/// `(result, status)`: `result` is the number of blocks SMGR successfully read
/// (or the failure indicator), `status` is the `PgAioResultStatus` discriminant
/// (0=UNKNOWN, 1=OK, 2=PARTIAL, 3=WARNING, 4=ERROR). Under the synchronous
/// method the IO already completed within `start_read_buffers`, so this returns
/// the published result immediately.
pub fn wait_read_buffers_aio(wref: BufAioWaitRef) -> PgResult<(i32, u32)> {
    let ew = to_engine_wref(wref);
    pgaio_wref_wait(&ew)?;

    // The completion path published the issuer-owned `PgAioReturn` into the
    // backend-local slot at reclaim time (C writes through `report_return`).
    let ret = take_pgaio_last_return(wref.aio_index).unwrap_or_default();
    Ok((ret.result.result, ret.result.status as u32))
}

/// `pgaio_wref_check_done(&operation->io_wref)` (bufmgr.c `WaitReadBuffers`).
pub fn wref_check_done_aio(wref: BufAioWaitRef) -> PgResult<bool> {
    let ew = to_engine_wref(wref);
    pgaio_wref_check_done(&ew)
}
