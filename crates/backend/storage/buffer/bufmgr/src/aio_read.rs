//! bufmgr.c's AIO readv callbacks: pin handover at stage, per-buffer
//! verification + termination at completion, deferred error reporting.
//! Shared buffers only — temp relations keep the pre-AIO direct path (see
//! read.rs ReadBuffer_common).

use std::sync::atomic::Ordering;

use elog::ereport;
use types_core::{Buffer, BLCKSZ};
use types_error::{ErrorLevel, PgResult, ERRCODE_DATA_CORRUPTED, LOG_SERVER_ONLY};
use types_storage::aio::{
    PgAioResult, PgAioResultStatus, PgAioTargetData, PGAIO_HCB_SHARED_BUFFER_READV,
    PGAIO_RESULT_ERROR_BITS, PGAIO_WREF_TAG,
};
use types_storage::buf::{
    PgAioWaitRef, BM_DIRTY, BM_IO_ERROR, BM_IO_IN_PROGRESS, BM_TAG_VALID, BM_VALID,
    BUF_REFCOUNT_ONE,
};

use crate::buf_hdr::{BufferGetBlockPtr, GetBufferDescriptor, LockBufHdr, UnlockBufHdr};
use crate::pin::buffer_refcount;
use crate::read::{
    forget_buffer_io_resowner, page_is_verified, relpath_desc, TerminateBufferIO,
    PIV_IGNORE_CHECKSUM_FAILURE, PIV_LOG_LOG, READ_BUFFERS_IGNORE_CHECKSUM_FAILURES,
    READ_BUFFERS_ZERO_ON_ERROR,
};

const READV_COUNT_BITS: u32 = 7;
const READV_COUNT_MASK: u32 = (1 << READV_COUNT_BITS) - 1;

/// Pin handover: the AIO subsystem takes its own pin and arms a TAGGED
/// io_wref (WaitIO routes tagged wrefs to pgaio, untagged to the uring lane).
fn buffer_readv_stage(ioh: u32, _cb_data: u8, is_temp: bool) {
    assert!(!is_temp, "local-buffer AIO stage: temp relations keep the pre-AIO path");

    let (io_data, len) = aio_core::pgaio_io_get_handle_data(ioh);
    let wref = aio_core::pgaio_io_get_wref(ioh);
    let tagged = PgAioWaitRef {
        aio_index: wref.aio_index | PGAIO_WREF_TAG,
        generation_upper: wref.generation_upper,
        generation_lower: wref.generation_lower,
    };

    for i in 0..len {
        let buffer = io_data[i] as Buffer;
        let desc = GetBufferDescriptor(buffer - 1);

        let mut buf_state = LockBufHdr(desc);
        debug_assert!(buf_state & BM_TAG_VALID != 0);
        debug_assert!(buf_state & BM_VALID == 0);
        debug_assert!(buf_state & BM_DIRTY == 0);
        debug_assert!(buf_state & BM_IO_IN_PROGRESS != 0);
        debug_assert!(buffer_refcount(buf_state) >= 1);

        // AIO's own pin: issuer-side error cleanup releasing its pins must
        // not let the buffer be replaced while IO is ongoing.
        buf_state += BUF_REFCOUNT_ONE;
        // SAFETY: header lock held.
        unsafe { desc.set_io_wref(tagged) };
        UnlockBufHdr(desc, buf_state);

        forget_buffer_io_resowner(buffer);
    }
}

fn buffer_readv_decode_error(
    result: PgAioResult,
) -> (bool, bool, u8, u8, u8) {
    let mut rem = result.error_data;
    let zeroed_any = rem & 1 != 0;
    rem >>= 1;
    let ignored_any = rem & 1 != 0;
    rem >>= 1;
    let zeroed_or_error_count = (rem & READV_COUNT_MASK) as u8;
    rem >>= READV_COUNT_BITS;
    let checkfail_count = (rem & READV_COUNT_MASK) as u8;
    rem >>= READV_COUNT_BITS;
    let first_off = (rem & READV_COUNT_MASK) as u8;
    (zeroed_any, ignored_any, zeroed_or_error_count, checkfail_count, first_off)
}

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
    const {
        assert!(1 + 1 + 3 * READV_COUNT_BITS <= PGAIO_RESULT_ERROR_BITS);
    }
    debug_assert!(!is_temp);

    let zeroed_or_error_count = if error_count > 0 { error_count } else { zeroed_count };
    let first_off = if error_count > 0 {
        first_error_off
    } else if zeroed_count > 0 {
        first_zeroed_off
    } else {
        first_ignored_off
    };
    debug_assert!(!zeroed_any || error_count == 0);

    let mut shift = 0;
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
    let _ = shift;

    result.error_data = error_data;
    result.id = PGAIO_HCB_SHARED_BUFFER_READV;
    result.status = if error_count > 0 {
        PgAioResultStatus::Error
    } else {
        PgAioResultStatus::Warning
    };
}

fn buffer_readv_complete_one(
    td: &PgAioTargetData,
    buf_off: u8,
    buffer: Buffer,
    cb_data: u8,
    failed: bool,
) -> (bool, bool, bool, bool) {
    let desc = GetBufferDescriptor(buffer - 1);
    let tag_blocknum = desc.tag().blockNum;
    #[cfg(debug_assertions)]
    {
        let buf_state = desc.state.load(Ordering::Relaxed);
        debug_assert!(buf_state & BM_TAG_VALID != 0);
        debug_assert!(buf_state & BM_VALID == 0);
        debug_assert!(buf_state & BM_IO_IN_PROGRESS != 0);
        debug_assert!(buf_state & BM_DIRTY == 0);
    }

    let mut buffer_invalid = false;
    let mut failed_checksum = false;
    let mut ignored_checksum = false;
    let mut zeroed_buffer = false;
    let mut failed = failed;

    // Only log the checksum detail here (the completion may run in any
    // backend or IO worker); the failure itself is reported at the definer
    // via buffer_readv_report.
    let mut piv_flags = PIV_LOG_LOG;
    // The completing side's GUCs may differ from the definer's: use the
    // definer's baked-in flags.
    if cb_data as u32 & READ_BUFFERS_IGNORE_CHECKSUM_FAILURES != 0 {
        piv_flags |= PIV_IGNORE_CHECKSUM_FAILURE;
    }

    if !failed {
        let blk = BufferGetBlockPtr(buffer);
        if !page_is_verified(blk, tag_blocknum, piv_flags, Some(&mut failed_checksum)) {
            if cb_data as u32 & READ_BUFFERS_ZERO_ON_ERROR != 0 {
                // SAFETY: BM_IO_IN_PROGRESS + the AIO pin: we own the image.
                unsafe { core::ptr::write_bytes(blk, 0, BLCKSZ) };
                zeroed_buffer = true;
            } else {
                buffer_invalid = true;
                failed = true;
            }
        } else if failed_checksum {
            ignored_checksum = true;
        }
        // Log immediately, server-only: the definer may never process the
        // result (cancelled, or another IO failed first).
        if buffer_invalid || failed_checksum || zeroed_buffer {
            let mut result_one = PgAioResult::default();
            buffer_readv_encode_error(
                &mut result_one,
                false,
                zeroed_buffer,
                ignored_checksum,
                buffer_invalid as u8,
                zeroed_buffer as u8,
                failed_checksum as u8,
                buf_off,
                buf_off,
                buf_off,
            );
            let _ = buffer_readv_report(result_one, *td, LOG_SERVER_ONLY);
        }
    }

    let set_flag_bits = if failed { BM_IO_ERROR } else { BM_VALID };
    TerminateBufferIO(desc, false, set_flag_bits, false, true);

    (buffer_invalid, failed_checksum, ignored_checksum, zeroed_buffer)
}

fn shared_buffer_readv_complete(ioh: u32, prior_result: PgAioResult, cb_data: u8) -> PgAioResult {
    let mut result = prior_result;
    let td = aio_core::pgaio_io_get_target_data(ioh);
    debug_assert!(!td.smgr.is_temp);

    let mut first_error_off: u8 = 0;
    let mut first_zeroed_off: u8 = 0;
    let mut first_ignored_off: u8 = 0;
    let mut error_count: u8 = 0;
    let mut zeroed_count: u8 = 0;
    let mut ignored_count: u8 = 0;
    let mut checkfail_count: u8 = 0;

    let (io_data, len) = aio_core::pgaio_io_get_handle_data(ioh);
    for buf_off in 0..len {
        let buf = io_data[buf_off] as Buffer;

        let failed = prior_result.status == PgAioResultStatus::Error
            || prior_result.result <= buf_off as i32;

        let (failed_verification, failed_checksum, ignored_checksum, zeroed_buffer) =
            buffer_readv_complete_one(&td, buf_off as u8, buf, cb_data, failed);

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

    if prior_result.status != PgAioResultStatus::Error
        && (error_count > 0 || ignored_count > 0 || zeroed_count > 0)
    {
        buffer_readv_encode_error(
            &mut result,
            false,
            zeroed_count > 0,
            ignored_count > 0,
            error_count,
            zeroed_count,
            checkfail_count,
            first_error_off,
            first_zeroed_off,
            first_ignored_off,
        );
        let _ = buffer_readv_report(result, td, types_error::DEBUG1);
    }

    // Checksum-failure stats are reported by the definer in
    // shared_buffer_readv_complete_local (only it is known to have prepared
    // its database entry ref).

    result
}

/// The definer-side completion: report checksum failures against the
/// database's stats (safe here because the definer ran
/// pgstat_prepare_report_checksum_failure before issuing).
fn shared_buffer_readv_complete_local(
    ioh: u32,
    prior_result: PgAioResult,
    _cb_data: u8,
) -> PgAioResult {
    if prior_result.status == PgAioResultStatus::Ok {
        return prior_result;
    }
    let (_, _, _, checkfail_count, _) = buffer_readv_decode_error(prior_result);
    if checkfail_count > 0 {
        let td = aio_core::pgaio_io_get_target_data(ioh);
        pgstat::pgstat_report_checksum_failures_in_db(
            td.smgr.rlocator.dbOid,
            checkfail_count as i64,
        );
    }
    prior_result
}

fn local_buffer_readv_complete(_ioh: u32, _prior: PgAioResult, _cb_data: u8) -> PgAioResult {
    panic!("local-buffer AIO completion: temp relations keep the pre-AIO path");
}

/// buffer_readv_report (bufmgr.c): the C 18 message shapes for
/// invalid/zeroed/ignored pages, single- and multi-block.
fn buffer_readv_report(
    result: PgAioResult,
    td: PgAioTargetData,
    elevel: ErrorLevel,
) -> PgResult<()> {
    let nblocks = td.smgr.nblocks;
    let first = td.smgr.blockNum;
    let last = first + nblocks - 1;
    let rpath = relpath_desc(td.smgr.rlocator, td.smgr.forkNum);
    let (zeroed_any, ignored_any, zeroed_or_error_count, checkfail_count, first_off) =
        buffer_readv_decode_error(result);

    // A read with both zeroed buffers *and* ignored checksums is too
    // irregular for the common shapes (C's special case).
    if zeroed_any && ignored_any {
        debug_assert!(nblocks > 1); // same block can't be both zeroed and ignored
        debug_assert!(result.status != PgAioResultStatus::Error);
        let affected_count = zeroed_or_error_count;
        let mut b = ereport(elevel).errcode(ERRCODE_DATA_CORRUPTED).errmsg(format!(
            "zeroing {} page(s) and ignoring {} checksum failure(s) among blocks {}..{} of relation \"{}\"",
            affected_count, checkfail_count, first, last, rpath
        ));
        if affected_count > 1 {
            b = b.errdetail(format!(
                "Block {} held the first zeroed page.",
                first + first_off as u32
            ));
        }
        let others = affected_count as u32 + checkfail_count as u32 - 1;
        b = b.errhint(if others == 1 {
            format!("See server log for details about the other {others} invalid block.")
        } else {
            format!("See server log for details about the other {others} invalid blocks.")
        });
        return b.finish(crate::read::loc("WaitReadBuffers"));
    }

    let affected_count;
    let (msg, detail, hint) = if result.status == PgAioResultStatus::Error {
        debug_assert!(!zeroed_any); // can't have invalid pages when zeroing them
        affected_count = zeroed_or_error_count;
        if affected_count <= 1 {
            (
                format!(
                    "invalid page in block {} of relation \"{}\"",
                    first + first_off as u32,
                    rpath
                ),
                None,
                None,
            )
        } else {
            (
                format!(
                    "{} invalid pages among blocks {}..{} of relation \"{}\"",
                    affected_count, first, last, rpath
                ),
                Some(format!("Block {} held the first invalid page.", first + first_off as u32)),
                Some(format!(
                    "See server log for the other {} invalid block(s).",
                    affected_count - 1
                )),
            )
        }
    } else if zeroed_any {
        affected_count = zeroed_or_error_count;
        if affected_count <= 1 {
            (
                format!(
                    "invalid page in block {} of relation \"{}\"; zeroing out page",
                    first + first_off as u32,
                    rpath
                ),
                None,
                None,
            )
        } else {
            (
                format!(
                    "zeroing out {} invalid pages among blocks {}..{} of relation \"{}\"",
                    affected_count, first, last, rpath
                ),
                Some(format!("Block {} held the first zeroed page.", first + first_off as u32)),
                Some(format!(
                    "See server log for the other {} zeroed block(s).",
                    affected_count - 1
                )),
            )
        }
    } else {
        debug_assert!(ignored_any);
        affected_count = checkfail_count;
        if affected_count <= 1 {
            (
                format!(
                    "ignoring checksum failure in block {} of relation \"{}\"",
                    first + first_off as u32,
                    rpath
                ),
                None,
                None,
            )
        } else {
            (
                format!(
                    "ignoring {} checksum failures among blocks {}..{} of relation \"{}\"",
                    affected_count, first, last, rpath
                ),
                Some(format!("Block {} held the first ignored page.", first + first_off as u32)),
                Some(format!(
                    "See server log for the other {} ignored block(s).",
                    affected_count - 1
                )),
            )
        }
    };

    let mut b = ereport(elevel).errcode(ERRCODE_DATA_CORRUPTED).errmsg(msg);
    if let Some(d) = detail {
        b = b.errdetail(d);
    }
    if let Some(h) = hint {
        b = b.errhint(h);
    }
    b.finish(crate::read::loc("WaitReadBuffers"))
}

pub(crate) fn init_seams() {
    bufmgr_seams::aio_buffer_readv_stage::set(buffer_readv_stage);
    bufmgr_seams::aio_shared_buffer_readv_complete::set(shared_buffer_readv_complete);
    bufmgr_seams::aio_shared_buffer_readv_complete_local::set(shared_buffer_readv_complete_local);
    bufmgr_seams::aio_local_buffer_readv_complete::set(local_buffer_readv_complete);
    bufmgr_seams::aio_buffer_readv_report::set(buffer_readv_report);
}
