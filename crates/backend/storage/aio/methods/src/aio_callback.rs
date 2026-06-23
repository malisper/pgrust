//! `storage/aio/aio_callback.c` — callback registration + the
//! stage/complete-shared/complete-local/report dispatch loops.
//!
//! The registered callbacks themselves (`aio_md_readv_cb`,
//! `aio_shared_buffer_readv_cb`, `aio_local_buffer_readv_cb`) live in
//! `md.c`/`bufmgr.c`/`localbuf.c` and are not yet ported for AIO. The dispatch
//! loops below are real (registration, last-registered-first ordering, the
//! running [`PgAioResult`] threaded between callbacks); the per-callback
//! invocation is seamed (`pgaio_cb_*`) and threads the running result through
//! the handle's `distilled_result` field — the same datum C passes by value
//! between callbacks. These are reached only on the async / buffered-IO
//! completion path.

extern crate alloc;

use types_error::{PgError, PgResult};

use miscinit_seams as misc;

use crate::aio::ioh;
use crate::{
    PgAioResult, PgAioResultStatus, PgAioTargetData, PGAIO_HANDLE_MAX_CALLBACKS, PGAIO_OP_INVALID,
    PGAIO_TID_INVALID,
};

/// `PGAIO_HCB_INVALID` (`storage/aio.h`) — the zero callback id.
pub const PGAIO_HCB_INVALID: u8 = 0;
/// `PGAIO_HCB_MD_READV` (`storage/aio.h`).
pub const PGAIO_HCB_MD_READV: u8 = 1;
/// `PGAIO_HCB_SHARED_BUFFER_READV` (`storage/aio.h`).
pub const PGAIO_HCB_SHARED_BUFFER_READV: u8 = 2;
/// `PGAIO_HCB_LOCAL_BUFFER_READV` (`storage/aio.h`).
pub const PGAIO_HCB_LOCAL_BUFFER_READV: u8 = 3;
/// `PGAIO_HCB_MAX` (`storage/aio.h`) — highest valid callback id.
pub const PGAIO_HCB_MAX: u8 = PGAIO_HCB_LOCAL_BUFFER_READV;

/// `lengthof(aio_handle_cbs)` — the size of the callback registry.
const AIO_HANDLE_CBS_LEN: u8 = PGAIO_HCB_MAX + 1;

/// `void pgaio_io_register_callbacks(PgAioHandle *ioh, cb_id, cb_data)`
/// (aio_callback.c).
pub fn pgaio_io_register_callbacks(ioh_index: usize, cb_id: u8, cb_data: u8) -> PgResult<()> {
    if cb_id > PGAIO_HCB_MAX {
        // Assert(cb_id <= PGAIO_HCB_MAX) + range check.
        return Err(PgError::error(alloc::format!(
            "callback {cb_id} is out of range"
        )));
    }
    if cb_id >= AIO_HANDLE_CBS_LEN {
        return Err(PgError::error(alloc::format!(
            "callback {cb_id} is out of range"
        )));
    }
    // All non-invalid callbacks have a completion callback; the invalid entry
    // (id 0) has neither, matching the C "does not have a completion callback".
    if cb_id == PGAIO_HCB_INVALID {
        return Err(PgError::error(alloc::format!(
            "callback {cb_id} does not have a completion callback"
        )));
    }

    let h = ioh(ioh_index);
    let mut d = h.data();
    if d.num_callbacks as usize >= PGAIO_HANDLE_MAX_CALLBACKS {
        // elog(PANIC, ...) — faithfully a hard error.
        panic!(
            "too many callbacks, the max is {}",
            PGAIO_HANDLE_MAX_CALLBACKS
        );
    }
    let n = d.num_callbacks as usize;
    let mut callbacks = d.callbacks;
    let mut callbacks_data = d.callbacks_data;
    callbacks[n] = cb_id;
    callbacks_data[n] = cb_data;
    d.callbacks = callbacks;
    d.callbacks_data = callbacks_data;
    d.num_callbacks += 1;
    Ok(())
}

/// `void pgaio_io_set_handle_data_64(PgAioHandle *ioh, uint64 *data, uint8 len)`
/// (aio_callback.c).
pub fn pgaio_io_set_handle_data_64(ioh_index: usize, data: &[u64]) {
    let h = ioh(ioh_index);
    debug_assert!(h.state() == crate::PgAioHandleState::HandedOut);
    debug_assert!(h.data().handle_data_len == 0);
    let len = data.len();
    // Assert(len <= PG_IOV_MAX) + Assert(len <= io_max_combine_limit): the
    // caller-provided length must fit the handle's reserved sub-range.
    let iovec_off = h.iovec_off as usize;
    {
        let mut hd = crate::pgaio_ctl().handle_data.lock().unwrap();
        for (i, &v) in data.iter().enumerate() {
            hd[iovec_off + i] = v;
        }
    }
    h.data().handle_data_len = len as u8;
}

/// `void pgaio_io_set_handle_data_32(PgAioHandle *ioh, uint32 *data, uint8 len)`
/// (aio_callback.c) — convenience 32->64 widening version.
pub fn pgaio_io_set_handle_data_32(ioh_index: usize, data: &[u32]) {
    let widened: alloc::vec::Vec<u64> = data.iter().map(|&v| v as u64).collect();
    pgaio_io_set_handle_data_64(ioh_index, &widened);
}

/// The `cb_data` (`callbacks_data[i]`) a callback was registered with. The C
/// completion dispatch passes `ce->cb_data` to each callback; the value-typed
/// seam dispatch carries only `(cb_id, ioh_index)`, so the callback owner reads
/// its own registered `cb_data` back here. Returns the `cb_data` of the first
/// registered callback matching `cb_id` (each handle registers a given readv
/// callback at most once), or 0 if not found.
pub fn pgaio_io_get_callback_data_for(ioh_index: usize, cb_id: u8) -> u8 {
    let h = ioh(ioh_index);
    let d = h.data();
    for i in 0..d.num_callbacks as usize {
        if d.callbacks[i] == cb_id {
            return d.callbacks_data[i];
        }
    }
    0
}

/// `uint64 *pgaio_io_get_handle_data(PgAioHandle *ioh, uint8 *len)`
/// (aio_callback.c) — copy out the handle's data array.
pub fn pgaio_io_get_handle_data(ioh_index: usize) -> alloc::vec::Vec<u64> {
    let h = ioh(ioh_index);
    let len = h.data().handle_data_len;
    debug_assert!(len > 0);
    let iovec_off = h.iovec_off as usize;
    let hd = crate::pgaio_ctl().handle_data.lock().unwrap();
    hd[iovec_off..iovec_off + len as usize].to_vec()
}

/// `void pgaio_result_report(PgAioResult result, target_data, int elevel)`
/// (aio_callback.c). The callback-specific `report` body lives in the unported
/// owner; the dispatch + the result-state asserts are real.
pub fn pgaio_result_report(
    ioh_index: usize,
    result: PgAioResult,
    _target_data: &PgAioTargetData,
    elevel: i32,
) -> PgResult<()> {
    let cb_id = result.id as u8;
    debug_assert!(result.status != PgAioResultStatus::Unknown);
    debug_assert!(result.status != PgAioResultStatus::Ok);

    completion_seams::pgaio_cb_report::call(cb_id, ioh_index as u32, elevel)
}

/// `void pgaio_io_call_stage(PgAioHandle *ioh)` (aio_callback.c) — invoke `stage`
/// for all registered callbacks, last-registered first.
pub fn pgaio_io_call_stage(ioh_index: usize) -> PgResult<()> {
    let h = ioh(ioh_index);
    let (num_callbacks, callbacks, callbacks_data) = {
        let d = h.data();
        debug_assert!(d.target > PGAIO_TID_INVALID);
        debug_assert!(d.op > PGAIO_OP_INVALID);
        (d.num_callbacks, d.callbacks, d.callbacks_data)
    };

    let mut i = num_callbacks as usize;
    while i > 0 {
        let cb_id = callbacks[i - 1];
        let _cb_data = callbacks_data[i - 1];
        // if (!ce->cb->stage) continue; — the seam owner skips no-stage entries.
        completion_seams::pgaio_cb_stage::call(cb_id, ioh_index as u32)?;
        i -= 1;
    }
    Ok(())
}

/// `void pgaio_io_call_complete_shared(PgAioHandle *ioh)` (aio_callback.c) —
/// invoke `complete_shared` for all registered callbacks (last first), threading
/// the running result and storing the final distilled result on the handle.
pub fn pgaio_io_call_complete_shared(ioh_index: usize) -> PgResult<()> {
    misc::start_crit_section::call();

    let h = ioh(ioh_index);
    let (num_callbacks, callbacks) = {
        let d = h.data();
        debug_assert!(d.target > PGAIO_TID_INVALID);
        debug_assert!(d.op > PGAIO_OP_INVALID);
        (d.num_callbacks, d.callbacks)
    };

    // result.status = PGAIO_RS_OK; result.result = ioh->result; id = INVALID.
    {
        let mut d = h.data();
        d.distilled_result = PgAioResult {
            id: PGAIO_HCB_INVALID as u32,
            status: PgAioResultStatus::Ok,
            error_data: 0,
            result: h.result.load(core::sync::atomic::Ordering::Relaxed),
        };
    }

    // Last registered (innermost) callback first; each may modify the running
    // result, threaded through `distilled_result`.
    let mut i = num_callbacks as usize;
    while i > 0 {
        let cb_id = callbacks[i - 1];
        completion_seams::pgaio_cb_complete_shared::call(
            cb_id,
            ioh_index as u32,
        )?;
        // the callback should never transition to unknown
        debug_assert!(h.data().distilled_result.status != PgAioResultStatus::Unknown);
        i -= 1;
    }

    misc::end_crit_section::call();
    Ok(())
}

/// `PgAioResult pgaio_io_call_complete_local(PgAioHandle *ioh)`
/// (aio_callback.c) — invoke `complete_local` for all registered callbacks,
/// returning the result as modified by the local callbacks (NOT persisted to
/// `distilled_result`).
pub fn pgaio_io_call_complete_local(ioh_index: usize) -> PgResult<PgAioResult> {
    misc::start_crit_section::call();

    let h = ioh(ioh_index);
    let (num_callbacks, callbacks) = {
        let d = h.data();
        debug_assert!(d.target > PGAIO_TID_INVALID);
        debug_assert!(d.op > PGAIO_OP_INVALID);
        (d.num_callbacks, d.callbacks)
    };

    // start with distilled result from shared callback
    let saved = h.data().distilled_result;
    debug_assert!(saved.status != PgAioResultStatus::Unknown);

    // Thread the running result through `distilled_result` while dispatching,
    // then restore the shared distilled result (the local result must not be
    // visible to other waiters).
    let mut i = num_callbacks as usize;
    while i > 0 {
        let cb_id = callbacks[i - 1];
        completion_seams::pgaio_cb_complete_local::call(
            cb_id,
            ioh_index as u32,
        )?;
        debug_assert!(h.data().distilled_result.status != PgAioResultStatus::Unknown);
        i -= 1;
    }

    let local_result = h.data().distilled_result;
    // Note: don't save the local result in ioh->distilled_result.
    h.data().distilled_result = saved;

    misc::end_crit_section::call();
    Ok(local_result)
}
