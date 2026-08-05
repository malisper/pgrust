//! aio_callback.c: per-IO callback registration + lifecycle dispatch. The set
//! is closed (aio.h PgAioHandleCallbackID); dispatch is a match over IDs and
//! each callee lives in its owning crate, reached via that crate's seams
//! (bufmgr/md depend on aio_core, so the reverse edges must be seams).

use elog::ereport;
use types_error::PgResult;
use types_storage::aio::{
    PgAioResult, PgAioResultStatus, PgAioTargetData, PGAIO_HANDLE_MAX_CALLBACKS, PGAIO_HCB_INVALID,
    PGAIO_HCB_LOCAL_BUFFER_READV, PGAIO_HCB_MD_READV, PGAIO_HCB_SHARED_BUFFER_READV,
};

use crate::handle::loc;
use crate::{ioh, PGAIO_HS_HANDED_OUT};

pub fn pgaio_io_register_callbacks(index: u32, cb_id: u8, cb_data: u8) {
    debug_assert!(matches!(
        cb_id,
        PGAIO_HCB_MD_READV | PGAIO_HCB_SHARED_BUFFER_READV | PGAIO_HCB_LOCAL_BUFFER_READV
    ));
    let h = ioh(index);
    debug_assert!(h.state() == PGAIO_HS_HANDED_OUT);
    // SAFETY: HANDED_OUT, owner thread.
    let d = unsafe { h.data() };
    if d.num_callbacks as usize >= PGAIO_HANDLE_MAX_CALLBACKS {
        panic!("too many callbacks, the max is {}", PGAIO_HANDLE_MAX_CALLBACKS);
    }
    d.callbacks[d.num_callbacks as usize] = cb_id;
    d.callbacks_data[d.num_callbacks as usize] = cb_data;
    d.num_callbacks += 1;
}

pub fn pgaio_io_set_handle_data_32(index: u32, data: &[u32]) {
    let h = ioh(index);
    debug_assert!(h.state() == PGAIO_HS_HANDED_OUT);
    // SAFETY: HANDED_OUT, owner thread.
    let d = unsafe { h.data() };
    debug_assert!(d.handle_data_len == 0);
    debug_assert!(data.len() <= guc_tables::vars::io_max_combine_limit.read() as usize);
    // SAFETY: owner fills its own handle's data region pre-submission.
    unsafe {
        let hd = crate::handle_data_region(h.iovec_off);
        for (i, &v) in data.iter().enumerate() {
            *hd.add(i) = v as u64;
        }
    }
    d.handle_data_len = data.len() as u8;
}

pub fn pgaio_io_get_handle_data(index: u32) -> ([u64; 128], usize) {
    let h = ioh(index);
    // SAFETY: completion edges; the definer is done writing.
    let len = unsafe { h.data() }.handle_data_len as usize;
    debug_assert!(len > 0 && len <= 128);
    let mut out = [0u64; 128];
    // SAFETY: as above.
    unsafe {
        let hd = crate::handle_data_region(h.iovec_off);
        for (i, slot) in out.iter_mut().enumerate().take(len) {
            *slot = *hd.add(i);
        }
    }
    (out, len)
}

pub(crate) fn pgaio_io_call_stage(index: u32) {
    // SAFETY: DEFINED edge, owner thread.
    let (num, callbacks, callbacks_data) = unsafe {
        let d = ioh(index).data();
        (d.num_callbacks as usize, d.callbacks, d.callbacks_data)
    };
    for i in (0..num).rev() {
        match callbacks[i] {
            PGAIO_HCB_SHARED_BUFFER_READV => {
                bufmgr_seams::aio_buffer_readv_stage::call(index, callbacks_data[i], false);
            }
            PGAIO_HCB_LOCAL_BUFFER_READV => {
                bufmgr_seams::aio_buffer_readv_stage::call(index, callbacks_data[i], true);
            }
            // md readv has no stage callback.
            _ => {}
        }
    }
}

pub(crate) fn pgaio_io_call_complete_shared(index: u32) {
    let g = init_small::globals::CritSectionCount();
    debug_assert!(g > 0);

    let h = ioh(index);
    // SAFETY: completing side owns d on the COMPLETED_IO edge.
    let (num, callbacks, callbacks_data) = unsafe {
        let d = h.data();
        (d.num_callbacks as usize, d.callbacks, d.callbacks_data)
    };

    let mut result = PgAioResult {
        status: PgAioResultStatus::Ok, // low-level IO is always considered OK
        result: h.result.load(std::sync::atomic::Ordering::Relaxed),
        id: PGAIO_HCB_INVALID,
        error_data: 0,
    };

    for i in (0..num).rev() {
        result = match callbacks[i] {
            PGAIO_HCB_MD_READV => {
                smgr_seams::aio_md_readv_complete::call(index, result, callbacks_data[i])
            }
            PGAIO_HCB_SHARED_BUFFER_READV => {
                bufmgr_seams::aio_shared_buffer_readv_complete::call(
                    index,
                    result,
                    callbacks_data[i],
                )
            }
            PGAIO_HCB_LOCAL_BUFFER_READV => result,
            id => panic!("aio callback {id} has no complete_shared"),
        };
        debug_assert!(result.status != PgAioResultStatus::Unknown);
    }

    // SAFETY: completer owns d until the COMPLETED_SHARED Release store that
    unsafe { h.data() }.distilled_result = result;
}

pub(crate) fn pgaio_io_call_complete_local(index: u32) -> PgAioResult {
    let h = ioh(index);
    // SAFETY: owner thread past COMPLETED_SHARED.
    let (num, callbacks, callbacks_data, mut result) = unsafe {
        let d = h.data();
        (d.num_callbacks as usize, d.callbacks, d.callbacks_data, d.distilled_result)
    };
    debug_assert!(result.status != PgAioResultStatus::Unknown);

    for i in (0..num).rev() {
        result = match callbacks[i] {
            PGAIO_HCB_SHARED_BUFFER_READV => {
                bufmgr_seams::aio_shared_buffer_readv_complete_local::call(
                    index,
                    result,
                    callbacks_data[i],
                )
            }
            PGAIO_HCB_LOCAL_BUFFER_READV => {
                bufmgr_seams::aio_local_buffer_readv_complete::call(
                    index,
                    result,
                    callbacks_data[i],
                )
            }
            _ => result,
        };
        debug_assert!(result.status != PgAioResultStatus::Unknown);
    }

    result
}

pub fn pgaio_result_report(
    result: PgAioResult,
    target_data: &PgAioTargetData,
    elevel: types_error::ErrorLevel,
) -> PgResult<()> {
    debug_assert!(result.status != PgAioResultStatus::Unknown);
    debug_assert!(result.status != PgAioResultStatus::Ok);
    match result.id {
        PGAIO_HCB_MD_READV => smgr_seams::aio_md_readv_report::call(result, *target_data, elevel),
        PGAIO_HCB_SHARED_BUFFER_READV | PGAIO_HCB_LOCAL_BUFFER_READV => {
            bufmgr_seams::aio_buffer_readv_report::call(result, *target_data, elevel)
        }
        id => {
            ereport(types_error::ERROR)
                .errmsg_internal(format!("callback {id} does not have report callback"))
                .finish(loc("pgaio_result_report"))?;
            unreachable!("ERROR reported");
        }
    }
}
