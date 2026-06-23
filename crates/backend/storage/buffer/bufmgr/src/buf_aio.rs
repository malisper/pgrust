//! The buffer manager's AIO readv completion callbacks + the synchronous read
//! syscall (`bufmgr.c` `buffer_stage_common` / `buffer_readv_complete` /
//! `buffer_readv_complete_one` / `buffer_readv_report`, and the `smgrreadv` the
//! `mdstartreadv` -> `pgaio_io_perform_synchronously` path issues under
//! `io_method = sync`).
//!
//! These are the bodies the AIO engine (`backend-storage-aio-methods`) reaches
//! through the `backend-storage-aio-completion-seams` vtable for the
//! `PGAIO_HCB_SHARED_BUFFER_READV` / `PGAIO_HCB_LOCAL_BUFFER_READV` callbacks
//! and the `pgaio_perform_io_syscall` leaf — the parts of the AIO read path
//! whose data structures live in the buffer manager, exactly as in C (where
//! `buffer_readv_complete` and friends live in `bufmgr.c`, not in
//! `storage/aio/`).
//!
//! Engine handshake: the engine seeds the handle's `distilled_result` to
//! `{status: OK, result: ioh->result}` before dispatching `complete_shared`,
//! then dispatches each registered callback (last-registered first), reading
//! the running result back out of `distilled_result` afterwards. So each
//! callback reads `pgaio_io_get_distilled_result` (= `prior_result`) and writes
//! `pgaio_io_set_distilled_result` (= its returned `PgAioResult`). The handle's
//! `target_data` (which relation/fork/block range) and `handle_data` (the run
//! of `Buffer` ids) are read through the aio-methods accessors.
//!
//! Under `io_method = sync` (the only IO method this port supports) the read is
//! performed inline at stage time, so `pgaio_perform_io_syscall` runs the
//! `smgrreadv` and `complete_shared` runs the verification before the buffer
//! manager's `WaitReadBuffers` ever blocks. The read is byte-identical to the
//! single-block synchronous path the core `ReadBuffer*` already uses.

extern crate alloc;

use alloc::format;

use ::types_core::primitive::{BlockNumber, ForkNumber};
use ::types_error::{PgError, PgResult};
use ::types_storage::buf::{
    BM_IO_ERROR, BM_VALID, BUF_REFCOUNT_ONE, PgAioWaitRef, BM_IO_IN_PROGRESS,
};
use ::types_storage::storage::Buffer;
use ::types_storage::{RelFileLocator, RelFileLocatorBackend};

use ::methods::aio::{
    pgaio_io_get_distilled_result, pgaio_io_owner, pgaio_io_set_distilled_result,
};
use ::methods::aio_callback::pgaio_io_get_handle_data;
use ::methods::aio_target::pgaio_io_get_target_data;
use ::methods::{PgAioResult, PgAioResultStatus, PgAioTargetData};
use bufmgr_seams as sb;
use page as page;
use smgr as smgr;

use crate::mgr::BufferManager;

// The AIO handle-callback ids (storage/aio.h), mirrored from the engine.
const PGAIO_HCB_SHARED_BUFFER_READV: u8 = 2;
const PGAIO_HCB_LOCAL_BUFFER_READV: u8 = 3;

// READ_BUFFERS_* completion flag bits (bufmgr.h), the `cb_data` of the readv
// callbacks.
const READ_BUFFERS_ZERO_ON_ERROR: u8 = 1 << 0;
const READ_BUFFERS_IGNORE_CHECKSUM_FAILURES: u8 = 1 << 2;

// READV error-encoding field widths (bufmgr.c buffer_readv_encode_error).
const READV_COUNT_BITS: u32 = 7;
const READV_COUNT_MASK: u32 = (1 << READV_COUNT_BITS) - 1;

/// `BufferDescriptorGetBuffer(buf)` — 1-based [`Buffer`] for a 0-based id.
#[inline]
fn buf_id_to_buffer(buf_id: i32) -> Buffer {
    buf_id + 1
}

/// Reconstruct the `(RelFileLocatorBackend, ForkNumber, BlockNumber)` the IO
/// covers from the handle's smgr target data.
fn target_smgr(td: &PgAioTargetData) -> (RelFileLocatorBackend, ForkNumber, BlockNumber) {
    let locator = RelFileLocator {
        spcOid: td.rlocator_spc_oid,
        dbOid: td.rlocator_db_oid,
        relNumber: td.rlocator_rel_number,
    };
    let backend = if td.is_temp {
        lmgr_proc_seams::my_proc_number::call()
    } else {
        ::types_core::primitive::INVALID_PROC_NUMBER
    };
    let rlocator = RelFileLocatorBackend { locator, backend };
    let fork = ForkNumber::from_i32(td.fork as i32).unwrap_or(ForkNumber::MAIN_FORKNUM);
    (rlocator, fork, td.block)
}

/// `relpath`-style identifying string for the readv error messages.
fn relpath_str(rlocator: RelFileLocatorBackend, fork: ForkNumber) -> alloc::string::String {
    let loc = rlocator.locator;
    format!("{}/{}/{} (fork {:?})", loc.spcOid, loc.dbOid, loc.relNumber, fork)
}

// ===========================================================================
// pgaio_perform_io_syscall — the synchronous read leaf (io_method = sync)
// ===========================================================================

/// The `pg_preadv` leg of `pgaio_io_perform_synchronously` (aio_io.c), for the
/// buffer-readv path: read the run of consecutive blocks the handle covers off
/// disk into the run's buffer pages, returning the raw `ssize_t` (the total
/// bytes read, or `-errno` on failure) the engine stores in `ioh->result` and
/// then threads through the completion callbacks.
///
/// This is the faithful `io_method = sync` inline transfer: in C the synchronous
/// method performs the same `preadv` the worker/io_uring methods would issue
/// asynchronously, against the smgr file. The buffer manager already performs
/// byte-identical reads via [`smgr::smgrreadv`] in its single-block path; the
/// only difference here is the vectored run. The buffer pages are read one block
/// at a time (each `smgrreadv` of one consecutive block at the right offset is
/// byte-identical to a single vectored `preadv` across the run — `md_readv`
/// loops per segment internally anyway), because the shared buffer pool's blocks
/// for a run of buf_ids are not contiguous in memory.
pub fn pgaio_perform_io_syscall(ioh_index: u32) -> PgResult<i64> {
    let ioh = ioh_index as usize;
    let td = pgaio_io_get_target_data(ioh);
    let (rlocator, fork, first_block) = target_smgr(&td);
    let io_data = pgaio_io_get_handle_data(ioh);

    let mgr = BufferManager::global_expect();
    let mut total_bytes: i64 = 0;

    for (i, &buf_word) in io_data.iter().enumerate() {
        let buffer = buf_word as i32;
        let blocknum = first_block.wrapping_add(i as u32);

        if buffer < 0 {
            // Local (temp) buffer: read into the backend-local page.
            buffer_support_seams::local_buffer_with_page::call(
                buffer,
                &mut |dst: &mut [u8]| {
                    let mut bufs: [&mut [u8]; 1] = [dst];
                    smgr::smgrreadv(rlocator, fork, blocknum, &mut bufs, 1)
                },
            )?;
        } else {
            let buf_id = (buffer - 1) as usize;
            mgr.with_block_mut(buf_id, |dst| {
                let mut bufs: [&mut [u8]; 1] = [dst];
                smgr::smgrreadv(rlocator, fork, blocknum, &mut bufs, 1)
            })?;
        }

        total_bytes += ::types_core::primitive::BLCKSZ as i64;
    }

    Ok(total_bytes)
}

// ===========================================================================
// Stage callback — buffer_stage_common (bufmgr.c:6784)
// ===========================================================================

/// `buffer_stage_common(ioh, is_write = false, is_temp)` (bufmgr.c) — reflect
/// that each buffer in the run is now owned by the AIO subsystem: add the AIO
/// pin (`BUF_REFCOUNT_ONE`), stamp the handle's wait reference, and (for shared
/// buffers) hand the buffer I/O off from the resource owner to the AIO system.
/// The pin is released again in `TerminateBufferIO` at completion.
fn buffer_stage_common(ioh: usize, is_temp: bool, io_wref: PgAioWaitRef) -> PgResult<()> {
    let io_data = pgaio_io_get_handle_data(ioh);
    let mgr = BufferManager::global_expect();

    for &buf_word in io_data.iter() {
        let buffer = buf_word as i32;
        if is_temp {
            // Temp buffers don't use BM_IO_IN_PROGRESS and aren't tracked by the
            // resource owner; the local-buffer subsystem owns their lifecycle.
            continue;
        }
        let buf_id = (buffer - 1) as usize;

        let mut buf_state = mgr.lock_buf_hdr(buf_id);
        // A run member that is already valid with no I/O in progress is a "hit"
        // that needs neither the AIO pin/forget at stage nor termination at
        // completion. In C `ReadBuffersCanStartIO` takes `BM_IO_IN_PROGRESS`
        // atomically under the header lock, so such a buffer is never placed in
        // the run; the value-typed model's synchronous inline completion +
        // read_stream buffer forwarding can leave an already-completed buffer in
        // the run, so skip it here (and symmetrically in the completion).
        if buf_state & BM_IO_IN_PROGRESS == 0 {
            mgr.unlock_buf_hdr(buf_id, buf_state);
            continue;
        }
        // Reflect that the buffer is now owned by the AIO subsystem (the pin
        // released in TerminateBufferIO).
        buf_state += BUF_REFCOUNT_ONE;
        mgr.set_io_wref(buf_id, io_wref);
        mgr.unlock_buf_hdr(buf_id, buf_state);

        // Stop tracking this buffer via the resowner — the AIO system now keeps
        // track (ResourceOwnerForgetBufferIO).
        sb::forget_buffer_io::call(buf_id_to_buffer(buf_id as i32));
    }
    Ok(())
}

// ===========================================================================
// Completion — buffer_readv_complete (bufmgr.c:7173)
// ===========================================================================

/// Per-buffer completion (`buffer_readv_complete_one`, bufmgr.c:7029): verify
/// the page, honor `READ_BUFFERS_ZERO_ON_ERROR`, terminate the buffer I/O with
/// `BM_VALID`/`BM_IO_ERROR`, releasing the AIO pin. Returns the
/// `(buffer_invalid, failed_checksum, ignored_checksum, zeroed_buffer)` flags.
#[allow(clippy::too_many_arguments)]
fn buffer_readv_complete_one(
    rlocator: RelFileLocatorBackend,
    fork: ForkNumber,
    blocknum: BlockNumber,
    buffer: Buffer,
    flags: u8,
    mut failed: bool,
    is_temp: bool,
) -> PgResult<(bool, bool, bool, bool)> {
    let mgr = BufferManager::global_expect();

    let mut buffer_invalid = false;
    let mut failed_checksum = false;
    let mut ignored_checksum = false;
    let mut zeroed_buffer = false;

    // PIV_LOG_LOG: only log (to the server log) on checksum errors here; the
    // definer reports the user-facing message in buffer_readv_report.
    let mut piv_flags = ::types_storage::bufpage::PIV_LOG_LOG;
    if flags & READ_BUFFERS_IGNORE_CHECKSUM_FAILURES != 0 {
        piv_flags |= ::types_storage::bufpage::PIV_IGNORE_CHECKSUM_FAILURE;
    }

    if !failed {
        let verified = if is_temp {
            let mut v = (true, false);
            buffer_support_seams::local_buffer_with_page::call(
                buffer,
                &mut |bytes: &mut [u8]| {
                    let p = page::PageRef::new(bytes)?;
                    v = page::PageIsVerified(&p, blocknum, piv_flags)?;
                    Ok(())
                },
            )?;
            v
        } else {
            let buf_id = (buffer - 1) as usize;
            mgr.with_block(buf_id, |bytes| {
                let p = page::PageRef::new(bytes)?;
                page::PageIsVerified(&p, blocknum, piv_flags)
            })?
        };
        let (page_ok, chk_failed) = verified;
        failed_checksum = chk_failed;

        if !page_ok {
            if flags & READ_BUFFERS_ZERO_ON_ERROR != 0 {
                if is_temp {
                    buffer_support_seams::local_buffer_with_page::call(
                        buffer,
                        &mut |bytes: &mut [u8]| {
                            bytes.fill(0);
                            Ok(())
                        },
                    )?;
                } else {
                    mgr.zero_block((buffer - 1) as usize);
                }
                zeroed_buffer = true;
            } else {
                buffer_invalid = true;
                failed = true;
            }
        } else if failed_checksum {
            ignored_checksum = true;
        }

        // Immediately log a server-only message about an invalid page (the
        // completion may run in a different backend than the definer). We reuse
        // the report path; here, since we run inline in the definer under
        // io_method=sync, emit only the server-log line.
        if buffer_invalid || failed_checksum || zeroed_buffer {
            let path = relpath_str(rlocator, fork);
            let msg = if zeroed_buffer {
                format!("invalid page in block {blocknum} of relation \"{path}\"; zeroing out page")
            } else if buffer_invalid {
                format!("invalid page in block {blocknum} of relation \"{path}\"")
            } else {
                format!("ignoring checksum failure in block {blocknum} of relation \"{path}\"")
            };
            utils_error::emit_error_report_for(
                &utils_error::ereport(::types_error::error::LOG_SERVER_ONLY)
                    .errcode(::types_error::error::ERRCODE_DATA_CORRUPTED)
                    .errmsg_internal(msg)
                    .into_error(),
            );
        }
    }

    // Terminate I/O and set BM_VALID / BM_IO_ERROR, releasing the AIO pin
    // (release_aio = true). forget_owner = false: the resowner already forgot
    // the buffer I/O in buffer_stage_common.
    let set_flag_bits = if failed { BM_IO_ERROR } else { BM_VALID };
    if is_temp {
        buffer_support_seams::terminate_local_buffer_io::call(
            buffer,
            false,
            set_flag_bits,
        )?;
    } else {
        let buf_id = (buffer - 1) as usize;
        mgr.terminate_buffer_io(buf_id, false, set_flag_bits, false, true)?;
    }

    Ok((buffer_invalid, failed_checksum, ignored_checksum, zeroed_buffer))
}

/// `buffer_readv_complete(ioh, prior_result, cb_data, is_temp)` (bufmgr.c:7173)
/// — iterate the run, completing each buffer, and distill the run's result.
fn buffer_readv_complete(ioh: usize, cb_data: u8, is_temp: bool) -> PgResult<()> {
    let td = pgaio_io_get_target_data(ioh);
    let (rlocator, fork, first_block) = target_smgr(&td);

    // md_readv_complete (md.c:1978) — the INNERMOST readv callback, which in C is
    // registered by mdstartreadv (PGAIO_HCB_MD_READV) and runs before this
    // (outer) buffer-readv callback. The smgr API operates on blocks, but the
    // synchronous read syscall returns the raw byte count in `ioh->result`
    // (which the engine seeds into `distilled_result.result`). Convert it to
    // blocks here and apply md_readv's hard-error / zero-blocks / partial-read
    // classification, exactly as md_readv_complete does, so the (outer) buffer
    // completion below sees a blocks-shaped `prior_result`.
    let prior_result = {
        let mut r = pgaio_io_get_distilled_result(ioh);
        let raw = r.result;
        if raw < 0 {
            // Hard error (`-errno`): status ERROR, blocks 0. (The bufmgr's
            // WaitReadBuffers raises the user-facing error from the status.)
            r.status = PgAioResultStatus::Error;
            r.error_data = (-raw) as u32;
            r.result = 0;
        } else {
            let blocks = raw / (::types_core::primitive::BLCKSZ as i32);
            debug_assert!(blocks <= td.nblocks as i32);
            r.result = blocks;
            if blocks == 0 {
                // 0 blocks read is a failure.
                r.status = PgAioResultStatus::Error;
                r.error_data = 0;
            } else if r.status != PgAioResultStatus::Error && blocks < td.nblocks as i32 {
                // Partial read — retried at the bufmgr level.
                r.status = PgAioResultStatus::Partial;
            }
        }
        r
    };

    if is_temp {
        debug_assert!(td.is_temp);
        debug_assert_eq!(
            pgaio_io_owner(ioh),
            lmgr_proc_seams::my_proc_number::call()
        );
    } else {
        debug_assert!(!td.is_temp);
    }

    let io_data = pgaio_io_get_handle_data(ioh);
    let mgr = BufferManager::global_expect();

    let mut error_count: u8 = 0;
    let mut zeroed_count: u8 = 0;
    let mut ignored_count: u8 = 0;
    let mut checkfail_count: u8 = 0;
    let mut first_error_off: u8 = 0;
    let mut first_zeroed_off: u8 = 0;
    let mut first_ignored_off: u8 = 0;

    let prior_status = prior_result.status;
    let prior_blocks = prior_result.result;

    for (buf_off, &buf_word) in io_data.iter().enumerate() {
        let buffer = buf_word as i32;
        let blocknum = first_block.wrapping_add(buf_off as u32);

        // Skip a run member that is already a "hit" (valid, no I/O in progress);
        // it was skipped at stage time too and needs no completion. See the note
        // in `buffer_stage_common`.
        if !is_temp {
            let buf_id = (buffer - 1) as usize;
            if mgr.read_state(buf_id) & BM_IO_IN_PROGRESS == 0 {
                continue;
            }
        }

        // If the entire I/O failed at a lower level, or this block is past the
        // number of blocks the smgr read (partial read), this buffer failed.
        let failed = prior_status == PgAioResultStatus::Error
            || prior_blocks <= buf_off as i32;

        let (failed_verification, failed_checksum, ignored_checksum, zeroed_buffer) =
            buffer_readv_complete_one(
                rlocator,
                fork,
                blocknum,
                buffer,
                cb_data,
                failed,
                is_temp,
            )?;

        if failed_verification && !zeroed_buffer {
            if error_count == 0 {
                first_error_off = buf_off as u8;
            }
            error_count += 1;
        }
        if zeroed_buffer {
            if zeroed_count == 0 {
                first_zeroed_off = buf_off as u8;
            }
            zeroed_count += 1;
        }
        if ignored_checksum {
            if ignored_count == 0 {
                first_ignored_off = buf_off as u8;
            }
            ignored_count += 1;
        }
        if failed_checksum {
            checkfail_count += 1;
        }
    }

    // Distill the run's result. If the smgr read [partially] succeeded but page
    // verification failed for some pages, encode that into the result.
    let mut result = prior_result;
    if prior_status != PgAioResultStatus::Error
        && (error_count > 0 || ignored_count > 0 || zeroed_count > 0)
    {
        buffer_readv_encode_error(
            &mut result,
            is_temp,
            zeroed_count > 0,
            ignored_count > 0,
            error_count,
            zeroed_count,
            checkfail_count,
            first_error_off,
            first_zeroed_off,
            first_ignored_off,
        );
    }

    pgaio_io_set_distilled_result(ioh, result);
    Ok(())
}

/// `buffer_readv_encode_error` (bufmgr.c:6934) — pack the per-run error summary
/// into `result.error_data` + set `result.id`/`result.status`.
#[allow(clippy::too_many_arguments)]
fn buffer_readv_encode_error(
    result: &mut PgAioResult,
    is_temp: bool,
    zeroed_any: bool,
    ignored_any: bool,
    error_count: u8,
    zeroed_count: u8,
    checkfail_count: u8,
    first_error_off: u8,
    first_zeroed_off: u8,
    first_ignored_off: u8,
) {
    let zeroed_or_error_count = if error_count > 0 { error_count } else { zeroed_count };
    let first_off = if error_count > 0 {
        first_error_off
    } else if zeroed_count > 0 {
        first_zeroed_off
    } else {
        first_ignored_off
    };

    debug_assert!(!zeroed_any || error_count == 0);

    let mut shift: u32 = 0;
    let mut error_data: u32 = 0;
    error_data |= (zeroed_any as u32) << shift;
    shift += 1;
    error_data |= (ignored_any as u32) << shift;
    shift += 1;
    error_data |= (zeroed_or_error_count as u32) << shift;
    shift += READV_COUNT_BITS;
    error_data |= (checkfail_count as u32) << shift;
    shift += READV_COUNT_BITS;
    error_data |= (first_off as u32) << shift;

    result.error_data = error_data & ((1 << (2 + 3 * READV_COUNT_BITS)) - 1);
    result.id = if is_temp {
        PGAIO_HCB_LOCAL_BUFFER_READV as u32
    } else {
        PGAIO_HCB_SHARED_BUFFER_READV as u32
    };
    result.status = if error_count > 0 {
        PgAioResultStatus::Error
    } else {
        PgAioResultStatus::Warning
    };
}

/// Decode the readv error summary (bufmgr.c:6892) — the inverse of
/// [`buffer_readv_encode_error`], for the report path.
fn buffer_readv_decode_error(result: &PgAioResult) -> (bool, bool, u8, u8, u8) {
    let mut rem = result.error_data;
    let zeroed_any = (rem & 1) != 0;
    rem >>= 1;
    let ignored_any = (rem & 1) != 0;
    rem >>= 1;
    let zeroed_or_error_count = (rem & READV_COUNT_MASK) as u8;
    rem >>= READV_COUNT_BITS;
    let checkfail_count = (rem & READV_COUNT_MASK) as u8;
    rem >>= READV_COUNT_BITS;
    let first_off = (rem & READV_COUNT_MASK) as u8;
    (zeroed_any, ignored_any, zeroed_or_error_count, checkfail_count, first_off)
}

// ===========================================================================
// Per-callback vtable dispatch (the pgaio_cb_* seams)
// ===========================================================================

/// `aio_handle_cbs[cb_id].cb->stage(ioh, cb_data)` for the buffer-readv vtable.
pub fn pgaio_cb_stage(cb_id: u8, ioh_index: u32) -> PgResult<()> {
    let ioh = ioh_index as usize;
    let io_wref = aio_wref_of(ioh);
    match cb_id {
        PGAIO_HCB_SHARED_BUFFER_READV => buffer_stage_common(ioh, false, io_wref),
        PGAIO_HCB_LOCAL_BUFFER_READV => buffer_stage_common(ioh, true, io_wref),
        other => Err(PgError::error(format!(
            "pgaio_cb_stage: unexpected callback id {other} for the buffer manager"
        ))),
    }
}

/// `aio_handle_cbs[cb_id].cb->complete_shared(ioh, prior_result, cb_data)`.
pub fn pgaio_cb_complete_shared(cb_id: u8, ioh_index: u32) -> PgResult<()> {
    let ioh = ioh_index as usize;
    let cb_data = readv_cb_data(ioh, cb_id);
    match cb_id {
        // Shared buffers: the verification + termination runs in complete_shared.
        PGAIO_HCB_SHARED_BUFFER_READV => buffer_readv_complete(ioh, cb_data, false),
        // Local buffers: only the issuing backend can complete them; this runs
        // in complete_local (below). complete_shared is a no-op (the engine
        // threads prior_result through unchanged).
        PGAIO_HCB_LOCAL_BUFFER_READV => Ok(()),
        other => Err(PgError::error(format!(
            "pgaio_cb_complete_shared: unexpected callback id {other}"
        ))),
    }
}

/// `aio_handle_cbs[cb_id].cb->complete_local(ioh, prior_result, cb_data)`.
pub fn pgaio_cb_complete_local(cb_id: u8, ioh_index: u32) -> PgResult<()> {
    let ioh = ioh_index as usize;
    let cb_data = readv_cb_data(ioh, cb_id);
    match cb_id {
        // Shared buffers report checksum failures in the issuing backend in
        // complete_local; under this synchronous port the completion already ran
        // in the definer, so there is nothing extra to do (prior_result stands).
        PGAIO_HCB_SHARED_BUFFER_READV => Ok(()),
        // Local buffers: do the verification + termination here.
        PGAIO_HCB_LOCAL_BUFFER_READV => buffer_readv_complete(ioh, cb_data, true),
        other => Err(PgError::error(format!(
            "pgaio_cb_complete_local: unexpected callback id {other}"
        ))),
    }
}

/// `aio_handle_cbs[cb_id].cb->report(result, target_data, elevel)`
/// (`buffer_readv_report`, bufmgr.c:7276).
pub fn pgaio_cb_report(cb_id: u8, ioh_index: u32, elevel: i32) -> PgResult<()> {
    let ioh = ioh_index as usize;
    match cb_id {
        PGAIO_HCB_SHARED_BUFFER_READV | PGAIO_HCB_LOCAL_BUFFER_READV => {
            buffer_readv_report(ioh, elevel)
        }
        other => Err(PgError::error(format!(
            "pgaio_cb_report: unexpected callback id {other}"
        ))),
    }
}

/// `buffer_readv_report(result, td, elevel)` (bufmgr.c:7276) — emit the
/// user-facing ereport for a readv that zeroed pages / ignored or hit checksum
/// failures. The `result` is the handle's distilled result.
fn buffer_readv_report(ioh: usize, elevel: i32) -> PgResult<()> {
    let result = pgaio_io_get_distilled_result(ioh);
    let td = pgaio_io_get_target_data(ioh);
    let (rlocator, fork, first) = target_smgr(&td);
    let nblocks = td.nblocks as u32;
    let last = first + nblocks - 1;
    let path = relpath_str(rlocator, fork);

    let (zeroed_any, ignored_any, zeroed_or_error_count, checkfail_count, first_off) =
        buffer_readv_decode_error(&result);

    let (affected_count, msg) = if zeroed_any && ignored_any {
        let affected = zeroed_or_error_count;
        (
            affected,
            format!(
                "zeroing {affected} page(s) and ignoring {checkfail_count} checksum failure(s) among blocks {first}..{last} of relation \"{path}\""
            ),
        )
    } else if result.status == PgAioResultStatus::Error {
        let affected = zeroed_or_error_count;
        if affected == 1 {
            (
                affected,
                format!("invalid page in block {} of relation \"{path}\"", first + first_off as u32),
            )
        } else {
            (
                affected,
                format!("{affected} invalid pages among blocks {first}..{last} of relation \"{path}\""),
            )
        }
    } else if zeroed_any && !ignored_any {
        let affected = zeroed_or_error_count;
        if affected == 1 {
            (
                affected,
                format!(
                    "invalid page in block {} of relation \"{path}\"; zeroing out page",
                    first + first_off as u32
                ),
            )
        } else {
            (
                affected,
                format!("zeroing out {affected} invalid pages among blocks {first}..{last} of relation \"{path}\""),
            )
        }
    } else {
        let affected = checkfail_count;
        if affected == 1 {
            (
                affected,
                format!(
                    "ignoring checksum failure in block {} of relation \"{path}\"",
                    first + first_off as u32
                ),
            )
        } else {
            (
                affected,
                format!("ignoring {affected} checksum failures among blocks {first}..{last} of relation \"{path}\""),
            )
        }
    };
    let _ = affected_count;

    utils_error::emit_error_report_for(
        &utils_error::ereport(::types_error::error::ErrorLevel(elevel))
            .errcode(::types_error::error::ERRCODE_DATA_CORRUPTED)
            .errmsg_internal(msg)
            .into_error(),
    );
    Ok(())
}

/// Read the `READ_BUFFERS_*` flag bitmask (`cb_data`) the handle's readv
/// callback `cb_id` was registered with.
fn readv_cb_data(ioh: usize, cb_id: u8) -> u8 {
    ::methods::aio_callback::pgaio_io_get_callback_data_for(ioh, cb_id)
}

/// The handle's wait reference (for the stage callback's io_wref stamp).
fn aio_wref_of(ioh: usize) -> PgAioWaitRef {
    let w = ::methods::aio::pgaio_io_get_wref(ioh);
    PgAioWaitRef {
        aio_index: w.aio_index,
        generation_upper: w.generation_upper,
        generation_lower: w.generation_lower,
    }
}

/// Install the buffer-readv completion callbacks + the synchronous read syscall
/// into the AIO completion seams.
pub fn init_seams() {
    completion_seams::pgaio_cb_stage::set(pgaio_cb_stage);
    completion_seams::pgaio_cb_complete_shared::set(pgaio_cb_complete_shared);
    completion_seams::pgaio_cb_complete_local::set(pgaio_cb_complete_local);
    completion_seams::pgaio_cb_report::set(pgaio_cb_report);
    completion_seams::pgaio_perform_io_syscall::set(pgaio_perform_io_syscall);
}
