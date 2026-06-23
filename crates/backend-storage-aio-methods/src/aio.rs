//! `storage/aio/aio.c` — the `PgAioHandle` state-machine engine.
//!
//! This is a faithful port of aio.c's control flow. The shared-memory model
//! (the process-global `PgAioCtl`, the per-handle atomics + per-handle `Mutex`,
//! the per-backend `Mutex`, the `thread_local` `pgaio_my_backend`) lives in the
//! crate root [`crate`]; this module is the engine that drives it.
//!
//! Divergences from C, all per the crate-root model notes:
//!  * the intrusive `dlist_node`/`resowner_node` links are expressed by the
//!    [`crate::DclistHead`] index-vector lists, so every `dclist_*` op is a Vec
//!    op (see `crate::dclist_*`);
//!  * `pg_read_barrier`/`pg_write_barrier` map to the `Acquire`/`Release`
//!    orderings on the handle's atomic `state`/`generation`/`result` fields;
//!  * `pgaio_my_backend` is a process-local index into `pgaio_ctl->backend_state`;
//!  * `HOLD_INTERRUPTS`/`RESUME_INTERRUPTS`/`START_CRIT_SECTION`/
//!    `END_CRIT_SECTION` are the miscadmin seams;
//!  * the completion-callback leaves (`pgaio_io_call_*`), the synchronous-perform
//!    leaf and the resowner-AIO registry are seamed into their genuinely-unported
//!    owners (bufmgr/md/localbuf, smgr/fd, resowner) — see
//!    `backend-storage-aio-completion-seams`. They are reached only on the async /
//!    buffered-IO path, never on the `io_method = sync` boot path.

extern crate alloc;

use alloc::string::ToString;
use core::sync::atomic::Ordering;

use types_error::{PgError, PgResult};

use backend_storage_lmgr_condition_variable as cv;
use backend_utils_init_miscinit_seams as misc;

use crate::{
    dclist_count, dclist_delete_from, dclist_is_empty, dclist_pop_head, dclist_push_head,
    dclist_push_tail,
    pgaio_ctl, pgaio_method_ops, pgaio_my_backend, PgAioHandle, PgAioHandleState,
    PgAioResultStatus, PgAioReturn, PgAioWaitRef, ResourceOwnerId, PGAIO_HF_SYNCHRONOUS,
    PGAIO_OP_INVALID, PGAIO_SUBMIT_BATCH_SIZE, PGAIO_TID_INVALID, PG_UINT32_MAX,
    WAIT_EVENT_AIO_IO_COMPLETION,
};

use crate::aio_callback::{
    pgaio_io_call_complete_local, pgaio_io_call_complete_shared, pgaio_io_call_stage,
};
use crate::aio_io::{pgaio_io_perform_synchronously, pgaio_io_uses_fd};
use crate::aio_target::pgaio_io_has_target;

// ===========================================================================
// Small accessors over the process-global control struct + per-backend slot.
// ===========================================================================

/// `&pgaio_ctl->io_handles[i]`.
pub(crate) fn ioh(index: usize) -> &'static PgAioHandle {
    &pgaio_ctl().io_handles[index]
}

/// `pgaio_my_backend` deref, panicking when AIO is not initialized for this
/// process (the C code only ever uses `pgaio_my_backend` after asserting it).
fn mb_index() -> usize {
    pgaio_my_backend().expect("pgaio_my_backend is NULL (pgaio_init_backend not called)")
}

/// Lock and access this backend's `PgAioBackend` slot.
fn mb() -> std::sync::MutexGuard<'static, crate::PgAioBackend> {
    pgaio_ctl().backend_state[mb_index()].lock().unwrap()
}

// ===========================================================================
// Acquire / release
// ===========================================================================

/// `PgAioHandle *pgaio_io_acquire(ResourceOwner resowner, PgAioReturn *ret)`
/// (aio.c) — acquire a handle, blocking until one frees up.
pub fn pgaio_io_acquire(
    resowner: Option<ResourceOwnerId>,
    ret: Option<PgAioReturn>,
) -> PgResult<usize> {
    loop {
        if let Some(h) = pgaio_io_acquire_nb(resowner, ret)? {
            return Ok(h);
        }
        // Evidently all handles by this backend are in use. Just wait for some
        // to complete.
        pgaio_io_wait_for_free()?;
    }
}

/// `PgAioHandle *pgaio_io_acquire_nb(ResourceOwner resowner, PgAioReturn *ret)`
/// (aio.c) — acquire a handle, returning `None` if none are idle.
pub fn pgaio_io_acquire_nb(
    resowner: Option<ResourceOwnerId>,
    ret: Option<PgAioReturn>,
) -> PgResult<Option<usize>> {
    {
        let mb = mb();
        if mb.num_staged_ios as usize >= PGAIO_SUBMIT_BATCH_SIZE {
            debug_assert!(mb.num_staged_ios as usize == PGAIO_SUBMIT_BATCH_SIZE);
            drop(mb);
            pgaio_submit_staged()?;
        }
    }

    if mb().handed_out_io.is_some() {
        return Err(PgError::error("API violation: Only one IO can be handed out"));
    }

    // Probably not needed today, as interrupts should not process this IO, but...
    misc::hold_interrupts::call();

    let result: Option<usize>;
    {
        let mut mb = mb();
        if !dclist_is_empty(&mb.idle_ios) {
            let ion = dclist_pop_head(&mut mb.idle_ios);
            let h = ioh(ion);

            debug_assert!(h.state() == PgAioHandleState::Idle);
            debug_assert!(h.owner_procno == my_proc_number());

            // pgaio_io_update_state(ioh, PGAIO_HS_HANDED_OUT).
            pgaio_io_update_state(h, PgAioHandleState::HandedOut);
            mb.handed_out_io = Some(ion);
            drop(mb);

            if let Some(owner) = resowner {
                pgaio_io_resowner_register(ion, owner)?;
            }

            if let Some(mut r) = ret {
                let mut d = h.data();
                r.result.status = PgAioResultStatus::Unknown;
                d.report_return = Some(r);
            }

            result = Some(ion);
        } else {
            result = None;
        }
    }

    misc::resume_interrupts::call();

    Ok(result)
}

/// `void pgaio_io_release(PgAioHandle *ioh)` (aio.c).
pub fn pgaio_io_release(ioh_index: usize) -> PgResult<()> {
    let is_handed_out = mb().handed_out_io == Some(ioh_index);
    if is_handed_out {
        let h = ioh(ioh_index);
        debug_assert!(h.state() == PgAioHandleState::HandedOut);
        debug_assert!(h.data().resowner.is_some());

        mb().handed_out_io = None;

        // Note that no interrupts are processed between the handed_out_io check
        // and the call to reclaim.
        pgaio_io_reclaim(ioh_index)?;
        Ok(())
    } else {
        Err(PgError::error("release in unexpected state"))
    }
}

/// `void pgaio_io_release_resowner(dlist_node *ioh_node, bool on_error)`
/// (aio.c) — release a handle still on a resource owner during resowner cleanup.
/// `ioh_node` is the io-handle index (the AIO subsystem's own node identity).
pub fn pgaio_io_release_resowner(ioh_node: u64, on_error: bool) {
    let ioh_index = ioh_node as usize;
    let h = ioh(ioh_index);

    debug_assert!(h.data().resowner.is_some());

    // Otherwise an interrupt, in the middle of releasing the IO, could end up
    // trying to wait for the IO, leading to state confusion.
    misc::hold_interrupts::call();

    let owner = h.data().resowner.expect("resowner set");
    backend_storage_aio_completion_seams::resource_owner_forget_aio_handle::call(
        owner,
        ioh_index as u32,
    )
    .expect("ResourceOwnerForgetAioHandle");
    h.data().resowner = None;

    match h.state() {
        PgAioHandleState::Idle => {
            // elog(ERROR, "unexpected") — faithfully a hard error.
            panic!("pgaio_io_release_resowner: unexpected IDLE state");
        }
        PgAioHandleState::HandedOut => {
            let is_mine = {
                let mb = mb();
                debug_assert!(
                    mb.handed_out_io == Some(ioh_index) || mb.handed_out_io.is_none()
                );
                mb.handed_out_io == Some(ioh_index)
            };
            if is_mine {
                mb().handed_out_io = None;
                if !on_error {
                    // elog(WARNING, "leaked AIO handle")
                    log_warning("leaked AIO handle");
                }
            }
            pgaio_io_reclaim(ioh_index).expect("reclaim");
        }
        PgAioHandleState::Defined | PgAioHandleState::Staged => {
            if !on_error {
                log_warning("AIO handle was not submitted");
            }
            pgaio_submit_staged().expect("submit staged");
        }
        PgAioHandleState::Submitted
        | PgAioHandleState::CompletedIo
        | PgAioHandleState::CompletedShared
        | PgAioHandleState::CompletedLocal => {
            // this is expected to happen
        }
    }

    // Need to unregister the reporting of the IO's result, the memory it's
    // referencing likely has gone away.
    if h.data().report_return.is_some() {
        h.data().report_return = None;
    }

    misc::resume_interrupts::call();
}

/// `void pgaio_io_set_flag(PgAioHandle *ioh, PgAioHandleFlags flag)` (aio.c).
pub fn pgaio_io_set_flag(ioh_index: usize, flag: u8) {
    let h = ioh(ioh_index);
    debug_assert!(h.state() == PgAioHandleState::HandedOut);
    let mut d = h.data();
    d.flags |= flag;
}

/// `int pgaio_io_get_id(PgAioHandle *ioh)` (aio.c) — the handle's array index.
pub fn pgaio_io_get_id(ioh_index: usize) -> i32 {
    debug_assert!(ioh_index < pgaio_ctl().io_handle_count as usize);
    ioh_index as i32
}

/// `ProcNumber pgaio_io_get_owner(PgAioHandle *ioh)` (aio.c).
pub fn pgaio_io_get_owner(ioh_index: usize) -> i32 {
    ioh(ioh_index).owner_procno
}

/// `void pgaio_io_get_wref(PgAioHandle *ioh, PgAioWaitRef *iow)` (aio.c).
pub fn pgaio_io_get_wref(ioh_index: usize) -> PgAioWaitRef {
    let h = ioh(ioh_index);
    debug_assert!(matches!(
        h.state(),
        PgAioHandleState::HandedOut | PgAioHandleState::Defined | PgAioHandleState::Staged
    ));
    let generation = h.generation.load(Ordering::Relaxed);
    debug_assert!(generation != 0);
    PgAioWaitRef {
        aio_index: ioh_index as u32,
        generation_upper: (generation >> 32) as u32,
        generation_lower: generation as u32,
    }
}

/// Read the handle's running distilled result (`ioh->distilled_result`), the
/// `prior_result` a completion callback threads from the previously-run inner
/// callback. The engine seeds it to `{status: OK, result: ioh->result}` before
/// dispatching `complete_shared` (`pgaio_io_call_complete_shared`).
pub fn pgaio_io_get_distilled_result(ioh_index: usize) -> crate::PgAioResult {
    ioh(ioh_index).data().distilled_result
}

/// Store the handle's distilled result (`ioh->distilled_result`), the value a
/// completion callback returns and the engine threads to the next callback /
/// the issuer's return slot.
pub fn pgaio_io_set_distilled_result(ioh_index: usize, result: crate::PgAioResult) {
    ioh(ioh_index).data().distilled_result = result;
}

/// `ProcNumber pgaio_io_get_owner(PgAioHandle *ioh)` exposed by index for the
/// buffer-readv completion's `is_temp` owner assertion.
pub fn pgaio_io_owner(ioh_index: usize) -> i32 {
    ioh(ioh_index).owner_procno
}

// ===========================================================================
// Internal handle helpers
// ===========================================================================

/// `static inline void pgaio_io_update_state(PgAioHandle *ioh, new_state)`
/// (aio.c). The `pg_write_barrier(); ioh->state = new_state` becomes a `Release`
/// store (see [`PgAioHandle::set_state`]).
fn pgaio_io_update_state(h: &PgAioHandle, new_state: PgAioHandleState) {
    // Assert(!INTERRUPTS_CAN_BE_PROCESSED()): all callers hold interrupts.
    h.set_state(new_state);
}

/// `static void pgaio_io_resowner_register(PgAioHandle *ioh, ResourceOwner)`
/// (aio.c).
fn pgaio_io_resowner_register(ioh_index: usize, resowner: ResourceOwnerId) -> PgResult<()> {
    let h = ioh(ioh_index);
    debug_assert!(h.data().resowner.is_none());

    backend_storage_aio_completion_seams::resource_owner_remember_aio_handle::call(
        resowner,
        ioh_index as u32,
    )?;
    h.data().resowner = Some(resowner);
    Ok(())
}

/// `void pgaio_io_stage(PgAioHandle *ioh, PgAioOp op)` (aio.c).
pub fn pgaio_io_stage(ioh_index: usize, op: u8) -> PgResult<()> {
    let h = ioh(ioh_index);

    debug_assert!(h.state() == PgAioHandleState::HandedOut);
    debug_assert!(mb().handed_out_io == Some(ioh_index));
    debug_assert!(pgaio_io_has_target(ioh_index));

    // Otherwise an interrupt could end up trying to wait for the IO.
    misc::hold_interrupts::call();

    {
        let mut d = h.data();
        d.op = op;
    }
    h.result.store(0, Ordering::Relaxed);

    pgaio_io_update_state(h, PgAioHandleState::Defined);

    // allow a new IO to be staged
    mb().handed_out_io = None;

    pgaio_io_call_stage(ioh_index)?;

    pgaio_io_update_state(h, PgAioHandleState::Staged);

    // Synchronous execution has to be executed synchronously, so check first.
    let needs_synchronous = pgaio_io_needs_synchronous_execution(ioh_index);

    if !needs_synchronous {
        let in_batchmode = {
            let mut mb = mb();
            let n = mb.num_staged_ios as usize;
            mb.staged_ios[n] = Some(ioh_index);
            mb.num_staged_ios += 1;
            debug_assert!(mb.num_staged_ios as usize <= PGAIO_SUBMIT_BATCH_SIZE);
            mb.in_batchmode
        };

        // Unless code explicitly opted into batching, submit immediately.
        if !in_batchmode {
            pgaio_submit_staged()?;
        }
    } else {
        pgaio_io_prepare_submit(ioh_index)?;
        pgaio_io_perform_synchronously(ioh_index)?;
    }

    misc::resume_interrupts::call();
    Ok(())
}

/// `bool pgaio_io_needs_synchronous_execution(PgAioHandle *ioh)` (aio.c).
pub fn pgaio_io_needs_synchronous_execution(ioh_index: usize) -> bool {
    let h = ioh(ioh_index);

    // If the caller said to execute the IO synchronously, do so.
    if h.data().flags & PGAIO_HF_SYNCHRONOUS != 0 {
        return true;
    }

    // Check if the IO method requires synchronous execution of IO.
    let ops = pgaio_method_ops();
    if let Some(needs) = ops.needs_synchronous_execution {
        return needs(h);
    }

    false
}

/// `void pgaio_io_prepare_submit(PgAioHandle *ioh)` (aio.c).
pub fn pgaio_io_prepare_submit(ioh_index: usize) -> PgResult<()> {
    let h = ioh(ioh_index);
    pgaio_io_update_state(h, PgAioHandleState::Submitted);
    dclist_push_tail(&mut mb().in_flight_ios, ioh_index)?;
    Ok(())
}

/// `void pgaio_io_process_completion(PgAioHandle *ioh, int result)` (aio.c).
pub fn pgaio_io_process_completion(ioh_index: usize, result: i32) -> PgResult<()> {
    let h = ioh(ioh_index);
    debug_assert!(h.state() == PgAioHandleState::Submitted);
    // Assert(CritSectionCount > 0): completion runs in a critical section.

    h.result.store(result, Ordering::Relaxed);

    pgaio_io_update_state(h, PgAioHandleState::CompletedIo);

    // INJECTION_POINT("aio-process-completion-before-shared", ioh) — no-op here.

    pgaio_io_call_complete_shared(ioh_index)?;

    pgaio_io_update_state(h, PgAioHandleState::CompletedShared);

    // condition variable broadcast ensures state is visible before wakeup
    cv::ConditionVariableBroadcast(&h.cv);

    // contains call to pgaio_io_call_complete_local()
    if h.owner_procno == my_proc_number() {
        pgaio_io_reclaim(ioh_index)?;
    }
    Ok(())
}

/// `bool pgaio_io_was_recycled(PgAioHandle *ioh, uint64 ref_generation, state*)`
/// (aio.c). The `pg_read_barrier()` between the `state` and `generation` loads
/// maps to the `Acquire` load of `state` followed by an `Acquire` load of
/// `generation`.
fn pgaio_io_was_recycled(
    h: &PgAioHandle,
    ref_generation: u64,
) -> (bool, PgAioHandleState) {
    let state = h.state();
    let generation = h.generation.load(Ordering::Acquire);
    (generation != ref_generation, state)
}

/// `static void pgaio_io_wait(PgAioHandle *ioh, uint64 ref_generation)` (aio.c).
fn pgaio_io_wait(ioh_index: usize, ref_generation: u64) -> PgResult<()> {
    let h = ioh(ioh_index);
    let am_owner = h.owner_procno == my_proc_number();

    let (recycled, mut state) = pgaio_io_was_recycled(h, ref_generation);
    if recycled {
        return Ok(());
    }

    if am_owner
        && !matches!(
            state,
            PgAioHandleState::Submitted
                | PgAioHandleState::CompletedIo
                | PgAioHandleState::CompletedShared
                | PgAioHandleState::CompletedLocal
        )
    {
        panic!(
            "waiting for own IO {} in wrong state: {:?}",
            pgaio_io_get_id(ioh_index),
            state
        );
    }

    loop {
        let (recycled, s) = pgaio_io_was_recycled(h, ref_generation);
        if recycled {
            return Ok(());
        }
        state = s;

        match state {
            PgAioHandleState::Idle | PgAioHandleState::HandedOut => {
                return Err(PgError::error(alloc::format!(
                    "IO in wrong state: {}",
                    state as u8
                )));
            }
            PgAioHandleState::Submitted => {
                // If we need to wait via the IO method, do so now. Don't check
                // via the IO method if the issuing backend is executing the IO
                // synchronously.
                let ops = pgaio_method_ops();
                let is_sync = h.data().flags & PGAIO_HF_SYNCHRONOUS != 0;
                if ops.wait_one.is_some() && !is_sync {
                    (ops.wait_one.unwrap())(h, ref_generation);
                    continue;
                }
                // fallthrough into the CV-sleep arm.
                wait_via_cv(h, ref_generation)?;
            }
            PgAioHandleState::Defined
            | PgAioHandleState::Staged
            | PgAioHandleState::CompletedIo => {
                // waiting for owner to submit / reaper to complete.
                // Assert(IsUnderPostmaster).
                wait_via_cv(h, ref_generation)?;
            }
            PgAioHandleState::CompletedShared | PgAioHandleState::CompletedLocal => {
                // Note that no interrupts are processed between
                // pgaio_io_was_recycled() and this check.
                if am_owner {
                    pgaio_io_reclaim(ioh_index)?;
                }
                return Ok(());
            }
        }
    }
}

/// The shared CV-sleep arm of `pgaio_io_wait` (aio.c L659-671): sleep on the
/// handle's completion condition variable until it reaches a completed-shared/
/// local state or is recycled.
fn wait_via_cv(h: &PgAioHandle, ref_generation: u64) -> PgResult<()> {
    // ensure we're going to get woken up
    cv::ConditionVariablePrepareToSleep(&h.cv);

    loop {
        let (recycled, state) = pgaio_io_was_recycled(h, ref_generation);
        if recycled {
            break;
        }
        if state == PgAioHandleState::CompletedShared
            || state == PgAioHandleState::CompletedLocal
        {
            break;
        }
        cv::ConditionVariableSleep(&h.cv, WAIT_EVENT_AIO_IO_COMPLETION)?;
    }

    cv::ConditionVariableCancelSleep();
    Ok(())
}

/// `static void pgaio_io_reclaim(PgAioHandle *ioh)` (aio.c).
fn pgaio_io_reclaim(ioh_index: usize) -> PgResult<()> {
    let h = ioh(ioh_index);

    // This is only ok if it's our IO.
    debug_assert!(h.owner_procno == my_proc_number());
    debug_assert!(h.state() != PgAioHandleState::Idle);

    misc::hold_interrupts::call();

    // Execute local completion callbacks just before reclaiming.
    if h.state() == PgAioHandleState::CompletedShared {
        let local_result = pgaio_io_call_complete_local(ioh_index)?;
        pgaio_io_update_state(h, PgAioHandleState::CompletedLocal);

        // Submit-time generation (the read's identity in the issuer's wref); the
        // generation is only bumped after the field reset further below.
        let generation = h.generation.load(Ordering::Relaxed);
        let mut d = h.data();
        if let Some(mut rr) = d.report_return {
            rr.result = local_result;
            rr.target_data = d.target_data;
            d.report_return = Some(rr);
            // Publish the completed return into the issuer's backend-local slot
            // before the handle is recycled (C writes through the caller's
            // `report_return` pointer, whose storage outlives the handle). The
            // buffer-read `wait_read_buffers` seam reads it back after the wait.
            //
            // Key by the handle INSTANCE generation (this read's identity in the
            // issuer's wref), not the recycled `aio_index`: the read-ahead
            // pipeline can have several completed-but-unwaited reads queued on the
            // same recycled index, and an index-only key would let a later read's
            // result clobber an earlier, still-unwaited read's. The generation is
            // bumped further below (after the field reset), so this load is the
            // submit-time generation the issuer holds in its `PgAioWaitRef`.
            crate::set_pgaio_last_return(ioh_index as u32, generation, rr);
        }
    }

    // if the IO has been defined, it's on the in-flight list, remove.
    if h.state() != PgAioHandleState::HandedOut {
        dclist_delete_from(&mut mb().in_flight_ios, ioh_index);
    }

    {
        let owner = h.data().resowner;
        if let Some(owner) = owner {
            backend_storage_aio_completion_seams::resource_owner_forget_aio_handle::call(
                owner,
                ioh_index as u32,
            )?;
            h.data().resowner = None;
        }
    }
    debug_assert!(h.data().resowner.is_none());

    // Update generation & state first, before resetting the IO's fields.
    // Increment the generation first, so that we can assert elsewhere that we
    // never wait for an IDLE IO.
    h.generation.fetch_add(1, Ordering::Relaxed);
    pgaio_io_update_state(h, PgAioHandleState::Idle);

    // ensure the state update is visible before we reset fields (the Release
    // store in set_state above already orders this).
    {
        let mut d = h.data();
        d.op = PGAIO_OP_INVALID;
        d.target = PGAIO_TID_INVALID;
        d.flags = 0;
        d.num_callbacks = 0;
        d.handle_data_len = 0;
        d.report_return = None;
        d.distilled_result.status = PgAioResultStatus::Unknown;
    }
    h.result.store(0, Ordering::Relaxed);

    // Push to the head of the idle list (cache efficiency).
    dclist_push_head(&mut mb().idle_ios, ioh_index)?;

    misc::resume_interrupts::call();
    Ok(())
}

/// `static void pgaio_io_wait_for_free(void)` (aio.c).
fn pgaio_io_wait_for_free() -> PgResult<()> {
    let mut reclaimed = 0;

    // pgaio_debug(DEBUG2, "waiting for free IO with %d pending, %u in-flight,
    // %u idle IOs", ...) — elided (logging only).

    let (io_handle_off, imc) = {
        let mb = mb();
        (mb.io_handle_off as usize, crate::io_max_concurrency() as usize)
    };

    // First check if any of our IOs actually have completed - when using worker,
    // that'll often be the case. We could do so as part of the loop below, but
    // that'd potentially lead us to wait for some IO submitted before.
    for i in 0..imc {
        let idx = io_handle_off + i;
        let h = ioh(idx);
        if h.state() == PgAioHandleState::CompletedShared {
            // Note that no interrupts are processed between the state check and
            // the call to reclaim - that's important as otherwise an interrupt
            // could have already reclaimed the handle.
            //
            // Need to ensure that there's no reordering, in the more common
            // paths, where we wait for IO, that's done by
            // pgaio_io_was_recycled().
            // pg_read_barrier() — subsumed by the Acquire load in state().
            pgaio_io_reclaim(idx)?;
            reclaimed += 1;
        }
    }

    if reclaimed > 0 {
        return Ok(());
    }

    // If we have any unsubmitted IOs, submit them now. We'll start waiting in a
    // second, so it's better they're in flight. This also addresses the
    // edge-case that all IOs are unsubmitted.
    if mb().num_staged_ios > 0 {
        pgaio_submit_staged()?;
    }

    // possibly some IOs finished during submission
    if !dclist_is_empty(&mb().idle_ios) {
        return Ok(());
    }

    if dclist_count(&mb().in_flight_ios) == 0 {
        // ereport(ERROR, errmsg_internal("no free IOs despite no in-flight IOs"),
        //         errdetail_internal("%d pending, %u in-flight, %u idle IOs", ...))
        let mb = mb();
        return Err(PgError::error(alloc::format!(
            "no free IOs despite no in-flight IOs ({} pending, {} in-flight, {} idle IOs)",
            mb.num_staged_ios,
            dclist_count(&mb.in_flight_ios),
            dclist_count(&mb.idle_ios),
        )));
    }

    // Wait for the oldest in-flight IO to complete.
    //
    // XXX: Reusing the general IO wait is suboptimal, we don't need to wait for
    // that specific IO to complete, we just need *any* IO to complete.
    {
        // PgAioHandle *ioh = dclist_head_element(..., &in_flight_ios);
        let idx = {
            let mb = mb();
            mb.in_flight_ios.members[0]
        };
        let h = ioh(idx);
        // uint64 generation = ioh->generation;
        let generation = h.generation.load(Ordering::Relaxed);

        match h.state() {
            // should not be in in-flight list
            PgAioHandleState::Idle
            | PgAioHandleState::Defined
            | PgAioHandleState::HandedOut
            | PgAioHandleState::Staged
            | PgAioHandleState::CompletedLocal => {
                // elog(ERROR, "shouldn't get here with io:%d in state %d", ...)
                return Err(PgError::error(alloc::format!(
                    "shouldn't get here with io:{} in state {}",
                    pgaio_io_get_id(idx),
                    h.state() as u8,
                )));
            }
            PgAioHandleState::CompletedIo | PgAioHandleState::Submitted => {
                // pgaio_debug_io(DEBUG2, ioh, "waiting for free io with %u in
                // flight", ...) — elided (logging only).
                //
                // In a more general case this would be racy, because the
                // generation could increase after we read ioh->state above. But
                // we are only looking at IOs by the current backend and the IO
                // can only be recycled by this backend.
                pgaio_io_wait(idx, generation)?;
            }
            PgAioHandleState::CompletedShared => {
                // It's possible that another backend just finished this IO.
                //
                // Note that no interrupts are processed between the state check
                // and the call to reclaim - that's important as otherwise an
                // interrupt could have already reclaimed the handle.
                // pg_read_barrier() — subsumed by the Acquire load in state().
                pgaio_io_reclaim(idx)?;
            }
        }

        if dclist_count(&mb().idle_ios) == 0 {
            // elog(PANIC, "no idle IO after waiting for IO to terminate")
            panic!("no idle IO after waiting for IO to terminate");
        }
        Ok(())
    }
}

// ===========================================================================
// State-name + result-status string helpers (aio.c)
// ===========================================================================

/// `static const char *pgaio_io_state_get_name(PgAioHandleState s)` (aio.c).
pub fn pgaio_io_state_get_name(s: PgAioHandleState) -> &'static str {
    match s {
        PgAioHandleState::Idle => "IDLE",
        PgAioHandleState::HandedOut => "HANDED_OUT",
        PgAioHandleState::Defined => "DEFINED",
        PgAioHandleState::Staged => "STAGED",
        PgAioHandleState::Submitted => "SUBMITTED",
        PgAioHandleState::CompletedIo => "COMPLETED_IO",
        PgAioHandleState::CompletedShared => "COMPLETED_SHARED",
        PgAioHandleState::CompletedLocal => "COMPLETED_LOCAL",
    }
}

/// `const char *pgaio_io_get_state_name(PgAioHandle *ioh)` (aio.c).
pub fn pgaio_io_get_state_name(ioh_index: usize) -> &'static str {
    pgaio_io_state_get_name(ioh(ioh_index).state())
}

/// `const char *pgaio_result_status_string(PgAioResultStatus rs)` (aio.c).
pub fn pgaio_result_status_string(rs: PgAioResultStatus) -> &'static str {
    match rs {
        PgAioResultStatus::Unknown => "UNKNOWN",
        PgAioResultStatus::Ok => "OK",
        PgAioResultStatus::Warning => "WARNING",
        PgAioResultStatus::Partial => "PARTIAL",
        PgAioResultStatus::Error => "ERROR",
    }
}

// ===========================================================================
// Wait references
// ===========================================================================

/// `static PgAioHandle *pgaio_io_from_wref(PgAioWaitRef *iow, uint64 *ref_gen)`
/// (aio.c) — resolve a wait ref to a handle index + its referenced generation.
fn pgaio_io_from_wref(iow: &PgAioWaitRef) -> (usize, u64) {
    debug_assert!(iow.aio_index < pgaio_ctl().io_handle_count);
    let ref_generation =
        ((iow.generation_upper as u64) << 32) | iow.generation_lower as u64;
    debug_assert!(ref_generation != 0);
    (iow.aio_index as usize, ref_generation)
}

/// `void pgaio_wref_clear(PgAioWaitRef *iow)` (aio.c).
pub fn pgaio_wref_clear(iow: &mut PgAioWaitRef) {
    iow.aio_index = PG_UINT32_MAX;
}

/// `bool pgaio_wref_valid(PgAioWaitRef *iow)` (aio.c).
pub fn pgaio_wref_valid(iow: &PgAioWaitRef) -> bool {
    iow.aio_index != PG_UINT32_MAX
}

/// `int pgaio_wref_get_id(PgAioWaitRef *iow)` (aio.c).
pub fn pgaio_wref_get_id(iow: &PgAioWaitRef) -> i32 {
    debug_assert!(pgaio_wref_valid(iow));
    iow.aio_index as i32
}

/// `void pgaio_wref_wait(PgAioWaitRef *iow)` (aio.c).
pub fn pgaio_wref_wait(iow: &PgAioWaitRef) -> PgResult<()> {
    let (ioh_index, ref_generation) = pgaio_io_from_wref(iow);
    pgaio_io_wait(ioh_index, ref_generation)
}

/// `bool pgaio_wref_check_done(PgAioWaitRef *iow)` (aio.c).
pub fn pgaio_wref_check_done(iow: &PgAioWaitRef) -> PgResult<bool> {
    let (ioh_index, ref_generation) = pgaio_io_from_wref(iow);
    let h = ioh(ioh_index);

    let (recycled, state) = pgaio_io_was_recycled(h, ref_generation);
    if recycled {
        return Ok(true);
    }
    if state == PgAioHandleState::Idle {
        return Ok(true);
    }

    let am_owner = h.owner_procno == my_proc_number();

    if state == PgAioHandleState::CompletedShared || state == PgAioHandleState::CompletedLocal {
        if am_owner {
            pgaio_io_reclaim(ioh_index)?;
        }
        return Ok(true);
    }

    Ok(false)
}

// ===========================================================================
// Actions on multiple IOs (batch mode + submission)
// ===========================================================================

/// `void pgaio_enter_batchmode(void)` (aio.c).
pub fn pgaio_enter_batchmode() -> PgResult<()> {
    let mut mb = mb();
    if mb.in_batchmode {
        return Err(PgError::error("starting batch while batch already in progress"));
    }
    mb.in_batchmode = true;
    Ok(())
}

/// `void pgaio_exit_batchmode(void)` (aio.c).
pub fn pgaio_exit_batchmode() -> PgResult<()> {
    debug_assert!(mb().in_batchmode);
    pgaio_submit_staged()?;
    mb().in_batchmode = false;
    Ok(())
}

/// `bool pgaio_have_staged(void)` (aio.c).
pub fn pgaio_have_staged() -> bool {
    mb().num_staged_ios > 0
}

/// `void pgaio_submit_staged(void)` (aio.c).
pub fn pgaio_submit_staged() -> PgResult<()> {
    // Snapshot this backend's staged io-handle index list (C passes the engine
    // method `(num_staged_ios, pgaio_my_backend->staged_ios)`).
    let staged: alloc::vec::Vec<usize> = {
        let mb = mb();
        let num = mb.num_staged_ios as usize;
        if num == 0 {
            return Ok(());
        }
        mb.staged_ios[..num]
            .iter()
            .map(|s| s.expect("staged_ios slot is None within num_staged_ios"))
            .collect()
    };

    misc::start_crit_section::call();

    let ops = pgaio_method_ops();
    let submit = ops
        .submit
        .expect("io method has no submit callback (pgaio_submit_staged)");
    let did_submit = submit(&staged)?;

    misc::end_crit_section::call();

    let total_submitted = did_submit;
    debug_assert!(total_submitted == did_submit);

    mb().num_staged_ios = 0;
    let _ = total_submitted;
    Ok(())
}

// ===========================================================================
// Error / transaction-boundary cleanup
// ===========================================================================

/// `void pgaio_error_cleanup(void)` (aio.c).
pub fn pgaio_error_cleanup() {
    // It is possible code errored out after pgaio_enter_batchmode() but before
    // pgaio_exit_batchmode(). In that case submit the IO now.
    let in_batchmode = {
        let mut mb = mb();
        if mb.in_batchmode {
            mb.in_batchmode = false;
            true
        } else {
            false
        }
    };
    if in_batchmode {
        pgaio_submit_staged().expect("pgaio_error_cleanup: submit staged");
    }

    // As we aren't in batchmode, there shouldn't be any unsubmitted IOs.
    debug_assert!(mb().num_staged_ios == 0);
}

/// `void AtEOXact_Aio(bool isCommit)` (aio.c).
#[allow(non_snake_case)]
pub fn AtEOXact_Aio(_is_commit: bool) {
    // We should never be in batch mode at transactional boundaries.
    let in_batchmode = mb().in_batchmode;
    if in_batchmode {
        pgaio_error_cleanup();
        log_warning("open AIO batch at end of (sub-)transaction");
    }

    debug_assert!(mb().num_staged_ios == 0);
}

/// `void pgaio_closing_fd(int fd)` (aio.c) — submit staged IOs and (if the IO
/// method requires it) wait for any in-flight IO that uses `fd`, just before the
/// VFD layer closes that descriptor.
pub fn pgaio_closing_fd(fd: i32) {
    // Might be called before AIO is initialized or in a subprocess that doesn't
    // use AIO.
    if pgaio_my_backend().is_none() {
        return;
    }

    // For now just submit all staged IOs.
    if mb().num_staged_ios > 0 {
        pgaio_submit_staged().expect("pgaio_closing_fd: submit staged");
    }

    // If requested by the IO method, wait for all IOs that use the to-be-closed
    // FD.
    if pgaio_method_ops().wait_on_fd_before_close {
        // As waiting for one IO to complete may complete multiple IOs, we can't
        // just use a mutable list iterator. Restart the loop after each wait.
        loop {
            if dclist_is_empty(&mb().in_flight_ios) {
                break;
            }

            let mut found: Option<(usize, u64)> = None;
            {
                let mb = mb();
                for &idx in mb.in_flight_ios.members.iter() {
                    let generation = ioh(idx).generation.load(Ordering::Relaxed);
                    if pgaio_io_uses_fd(idx, fd) {
                        found = Some((idx, generation));
                        break;
                    }
                }
            }

            match found {
                None => break,
                Some((idx, generation)) => {
                    // see comment in pgaio_io_wait_for_free() about raciness
                    pgaio_io_wait(idx, generation).expect("pgaio_closing_fd: wait");
                }
            }
        }
    }
}

/// `void pgaio_shutdown(int code, Datum arg)` (aio.c) — the `before_shmem_exit`
/// callback registered in `pgaio_init_backend`.
pub fn pgaio_shutdown(code: i32, _arg: types_tuple::Datum<'static>) -> PgResult<()> {
    debug_assert!(pgaio_my_backend().is_some());
    debug_assert!(mb().handed_out_io.is_none());

    // first clean up resources as we would at a transaction boundary
    AtEOXact_Aio(code == 0);

    // Before exiting, make sure that all IOs are finished.
    loop {
        let candidate = {
            let mb = mb();
            if dclist_is_empty(&mb.in_flight_ios) {
                None
            } else {
                let idx = mb.in_flight_ios.members[0];
                Some((idx, ioh(idx).generation.load(Ordering::Relaxed)))
            }
        };
        match candidate {
            None => break,
            Some((idx, generation)) => {
                pgaio_io_wait(idx, generation)?;
            }
        }
    }

    clear_pgaio_my_backend_local();
    Ok(())
}

// ===========================================================================
// Small wrappers over crate-root helpers
// ===========================================================================

/// `MyProcNumber` (the issuing backend's proc number).
fn my_proc_number() -> i32 {
    backend_utils_init_small_seams::my_proc_number::call()
}

/// `pgaio_my_backend = NULL`.
fn clear_pgaio_my_backend_local() {
    crate::clear_pgaio_my_backend();
}

/// `elog(WARNING, ...)` — surfaced through the error crate's logging path.
fn log_warning(msg: &str) {
    let _ = backend_utils_error::elog(types_error::WARNING, msg.to_string());
}
