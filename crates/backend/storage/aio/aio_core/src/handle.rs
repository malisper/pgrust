//! aio.c: handle lifecycle, waits, batch submission, boundary cleanup.

use std::sync::atomic::Ordering;

use condition_variable::{
    ConditionVariableBroadcast, ConditionVariableCancelSleep, ConditionVariablePrepareToSleep,
    ConditionVariableSleep,
};
use elog::ereport;
use init_small::globals as g;
use types_error::{ErrorLocation, PgResult, ERROR, WARNING};
use types_storage::aio::{
    PgAioResult, PgAioResultStatus, PgAioReturn, PGAIO_HF_SYNCHRONOUS, PGAIO_OP_INVALID,
    PGAIO_SUBMIT_BATCH_SIZE, PGAIO_TID_INVALID,
};
use types_storage::buf::PgAioWaitRef;

use crate::{
    dclist_delete_from, dclist_pop_head, dclist_push_head, dclist_push_tail, handle_count, ioh,
    my_backend, my_backend_procno, IoMethodKind, PgAioHandle, MY_BACKEND, NO_HANDLE,
    PGAIO_HS_COMPLETED_IO, PGAIO_HS_COMPLETED_LOCAL, PGAIO_HS_COMPLETED_SHARED, PGAIO_HS_DEFINED,
    PGAIO_HS_HANDED_OUT, PGAIO_HS_IDLE, PGAIO_HS_STAGED, PGAIO_HS_SUBMITTED,
};

// wait_event_names.txt IO section, first entry.
const PG_WAIT_IO: u32 = 0x0A00_0000;
pub(crate) const WAIT_EVENT_AIO_IO_COMPLETION: u32 = PG_WAIT_IO;

#[cold]
#[track_caller]
pub(crate) fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

pub fn pgaio_io_acquire(
    resowner: Option<types_resowner::ResourceOwner>,
    ret: *mut PgAioReturn,
) -> PgResult<u32> {
    loop {
        if let Some(h) = pgaio_io_acquire_nb(resowner, ret)? {
            return Ok(h);
        }
        pgaio_io_wait_for_free()?;
    }
}

pub fn pgaio_io_acquire_nb(
    resowner: Option<types_resowner::ResourceOwner>,
    ret: *mut PgAioReturn,
) -> PgResult<Option<u32>> {
    // SAFETY: owner-thread slot access, no aio reentry in scope.
    unsafe {
        let mb = my_backend();
        if mb.num_staged_ios as usize >= PGAIO_SUBMIT_BATCH_SIZE {
            debug_assert!(mb.num_staged_ios as usize == PGAIO_SUBMIT_BATCH_SIZE);
            pgaio_submit_staged()?;
        }
    }

    // SAFETY: as above.
    if unsafe { my_backend() }.handed_out_io != NO_HANDLE {
        ereport(ERROR)
            .errmsg_internal("API violation: Only one IO can be handed out")
            .finish(loc("pgaio_io_acquire_nb"))?;
    }

    g::HoldInterrupts();

    // SAFETY: as above; dclist ops don't re-enter the backend slot.
    let result = unsafe {
        let mb = my_backend();
        if mb.idle_ios.count > 0 {
            let index = dclist_pop_head(&mut mb.idle_ios);
            let h = ioh(index);

            debug_assert!(h.state() == PGAIO_HS_IDLE);
            debug_assert!(h.owner_procno == my_backend_procno());

            h.set_state(PGAIO_HS_HANDED_OUT);
            mb.handed_out_io = index;

            if let Some(owner) = resowner {
                pgaio_io_resowner_register(index, owner)?;
            }

            if !ret.is_null() {
                (*ret).result.status = PgAioResultStatus::Unknown;
                h.data().report_return = ret;
            }

            Some(index)
        } else {
            None
        }
    };

    g::ResumeInterrupts();
    Ok(result)
}

pub fn pgaio_io_release(index: u32) -> PgResult<()> {
    // SAFETY: owner-thread slot access.
    let is_handed_out = unsafe { my_backend() }.handed_out_io == index;
    if !is_handed_out {
        ereport(ERROR)
            .errmsg_internal("release in unexpected state")
            .finish(loc("pgaio_io_release"))?;
    }
    let h = ioh(index);
    debug_assert!(h.state() == PGAIO_HS_HANDED_OUT);
    // SAFETY: HANDED_OUT => owner owns d.
    debug_assert!(unsafe { h.data() }.resowner.is_some());

    // SAFETY: owner-thread slot access.
    unsafe { my_backend() }.handed_out_io = NO_HANDLE;
    // No interrupt processing between the check and the reclaim (C contract).
    pgaio_io_reclaim(index);
    Ok(())
}

/// pgaio_io_release_resowner: resowner cleanup callback (installed into
pub fn pgaio_io_release_resowner(index: u32, on_error: bool) {
    let h = ioh(index);
    // SAFETY: resowner cleanup runs on the owner thread.
    let d = unsafe { h.data() };
    debug_assert!(d.resowner.is_some());

    g::HoldInterrupts();

    if let Some(owner) = d.resowner.take() {
        resowner::ResourceOwnerForgetAioHandle(owner, index as usize);
    }

    match h.state() {
        PGAIO_HS_IDLE => panic!("pgaio_io_release_resowner: unexpected IDLE state"),
        PGAIO_HS_HANDED_OUT => {
            // SAFETY: owner-thread slot access.
            let mb = unsafe { my_backend() };
            debug_assert!(mb.handed_out_io == index || mb.handed_out_io == NO_HANDLE);
            if mb.handed_out_io == index {
                mb.handed_out_io = NO_HANDLE;
                if !on_error {
                    let _ = elog::elog(WARNING, "leaked AIO handle".to_string());
                }
            }
            pgaio_io_reclaim(index);
        }
        PGAIO_HS_DEFINED | PGAIO_HS_STAGED => {
            if !on_error {
                let _ = elog::elog(WARNING, "AIO handle was not submitted".to_string());
            }
            pgaio_submit_staged().expect("pgaio_io_release_resowner: submit staged");
        }
        _ => {
        }
    }

    // SAFETY: owner thread; reclaim above may have re-entered d, re-borrow.
    unsafe { h.data() }.report_return = std::ptr::null_mut();

    g::ResumeInterrupts();
}

pub fn pgaio_io_set_flag(index: u32, flag: u8) {
    let h = ioh(index);
    debug_assert!(h.state() == PGAIO_HS_HANDED_OUT);
    h.flags.fetch_or(flag, Ordering::Relaxed);
}

pub fn pgaio_io_get_id(index: u32) -> i32 {
    debug_assert!((index as usize) < handle_count());
    index as i32
}

pub fn pgaio_io_get_owner(index: u32) -> i32 {
    ioh(index).owner_procno
}

pub fn pgaio_io_get_wref(index: u32) -> PgAioWaitRef {
    let h = ioh(index);
    debug_assert!(matches!(
        h.state(),
        PGAIO_HS_HANDED_OUT | PGAIO_HS_DEFINED | PGAIO_HS_STAGED
    ));
    let generation = h.generation.load(Ordering::Relaxed);
    debug_assert!(generation != 0);
    PgAioWaitRef {
        aio_index: index,
        generation_upper: (generation >> 32) as u32,
        generation_lower: generation as u32,
    }
}

fn pgaio_io_resowner_register(index: u32, owner: types_resowner::ResourceOwner) -> PgResult<()> {
    let h = ioh(index);
    // SAFETY: HANDED_OUT, owner thread.
    let d = unsafe { h.data() };
    debug_assert!(d.resowner.is_none());
    resowner::ResourceOwnerRememberAioHandle(owner, index as usize)?;
    d.resowner = Some(owner);
    Ok(())
}

pub(crate) fn pgaio_io_stage(index: u32, op: u8) -> PgResult<()> {
    let h = ioh(index);

    debug_assert!(h.state() == PGAIO_HS_HANDED_OUT);
    // SAFETY: owner-thread slot access.
    debug_assert!(unsafe { my_backend() }.handed_out_io == index);
    debug_assert!(crate::target::pgaio_io_has_target(index));

    g::HoldInterrupts();

    // SAFETY: HANDED_OUT, owner thread.
    unsafe { h.data() }.op = op;
    h.result.store(0, Ordering::Relaxed);

    h.set_state(PGAIO_HS_DEFINED);

    // SAFETY: owner-thread slot access.
    unsafe { my_backend() }.handed_out_io = NO_HANDLE;

    crate::callback::pgaio_io_call_stage(index);

    h.set_state(PGAIO_HS_STAGED);

    let needs_synchronous = pgaio_io_needs_synchronous_execution(index);

    if !needs_synchronous {
        // SAFETY: owner-thread slot access, released before submit.
        let in_batchmode = unsafe {
            let mb = my_backend();
            let n = mb.num_staged_ios as usize;
            mb.staged_ios[n] = index;
            mb.num_staged_ios += 1;
            debug_assert!(mb.num_staged_ios as usize <= PGAIO_SUBMIT_BATCH_SIZE);
            mb.in_batchmode
        };
        if !in_batchmode {
            pgaio_submit_staged()?;
        }
    } else {
        pgaio_io_prepare_submit(index);
        crate::io::pgaio_io_perform_synchronously(index);
    }

    g::ResumeInterrupts();
    Ok(())
}

pub(crate) fn pgaio_io_needs_synchronous_execution(index: u32) -> bool {
    let h = ioh(index);
    if h.flags.load(Ordering::Relaxed) & PGAIO_HF_SYNCHRONOUS != 0 {
        return true;
    }
    match crate::pgaio_method_kind() {
        IoMethodKind::Sync => true,
        IoMethodKind::Worker => crate::method_worker::pgaio_worker_needs_synchronous_execution(index),
    }
}

pub(crate) fn pgaio_io_prepare_submit(index: u32) {
    let h = ioh(index);
    h.set_state(PGAIO_HS_SUBMITTED);
    // SAFETY: owner-thread slot access.
    unsafe {
        let mb = my_backend();
        dclist_push_tail(&mut mb.in_flight_ios, index);
    }
}

/// synchronous execution) with the raw IO result, inside a critical section.
pub(crate) fn pgaio_io_process_completion(index: u32, result: i32) {
    let h = ioh(index);
    debug_assert!(h.state() == PGAIO_HS_SUBMITTED);
    debug_assert!(g::CritSectionCount() > 0);

    h.result.store(result, Ordering::Relaxed);
    h.set_state(PGAIO_HS_COMPLETED_IO);

    crate::callback::pgaio_io_call_complete_shared(index);

    h.set_state(PGAIO_HS_COMPLETED_SHARED);

    ConditionVariableBroadcast(&h.cv);

    if h.owner_procno == my_backend_procno_or_invalid() {
        pgaio_io_reclaim(index);
    }
}

// IO workers complete other backends' IOs; their thread has no aio backend
// must not panic there.
fn my_backend_procno_or_invalid() -> i32 {
    MY_BACKEND.get().unwrap_or(types_core::INVALID_PROC_NUMBER)
}

pub(crate) fn pgaio_io_was_recycled(h: &PgAioHandle, ref_generation: u64) -> (bool, u8) {
    let state = h.state();
    let generation = h.generation.load(Ordering::Relaxed);
    (generation != ref_generation, state)
}

fn pgaio_io_wait(index: u32, ref_generation: u64) -> PgResult<()> {
    let h = ioh(index);
    let am_owner = h.owner_procno == my_backend_procno_or_invalid();

    let (recycled, state) = pgaio_io_was_recycled(h, ref_generation);
    if recycled {
        return Ok(());
    }

    if am_owner
        && !matches!(
            state,
            PGAIO_HS_SUBMITTED
                | PGAIO_HS_COMPLETED_IO
                | PGAIO_HS_COMPLETED_SHARED
                | PGAIO_HS_COMPLETED_LOCAL
        )
    {
        // C: elog(PANIC, ...).
        panic!("waiting for own IO {} in wrong state: {}", index, state_name(state));
    }

    loop {
        let (recycled, state) = pgaio_io_was_recycled(h, ref_generation);
        if recycled {
            return Ok(());
        }

        match state {
            PGAIO_HS_IDLE | PGAIO_HS_HANDED_OUT => {
                ereport(ERROR)
                    .errmsg_internal(format!("IO in wrong state: {}", state))
                    .finish(loc("pgaio_io_wait"))?;
            }
            PGAIO_HS_SUBMITTED => {
                // Neither ported method has a wait_one callback (sync executes
                wait_via_cv(h, ref_generation)?;
            }
            PGAIO_HS_DEFINED | PGAIO_HS_STAGED | PGAIO_HS_COMPLETED_IO => {
                wait_via_cv(h, ref_generation)?;
            }
            _ => {
                if am_owner {
                    pgaio_io_reclaim(index);
                }
                return Ok(());
            }
        }
    }
}

fn wait_via_cv(h: &'static PgAioHandle, ref_generation: u64) -> PgResult<()> {
    ConditionVariablePrepareToSleep(&h.cv);
    loop {
        let (recycled, state) = pgaio_io_was_recycled(h, ref_generation);
        if recycled || state == PGAIO_HS_COMPLETED_SHARED || state == PGAIO_HS_COMPLETED_LOCAL {
            break;
        }
        if let Err(e) = ConditionVariableSleep(&h.cv, WAIT_EVENT_AIO_IO_COMPLETION) {
            ConditionVariableCancelSleep();
            return Err(e);
        }
    }
    ConditionVariableCancelSleep();
    Ok(())
}

fn pgaio_io_reclaim(index: u32) {
    let h = ioh(index);
    debug_assert!(h.owner_procno == my_backend_procno());
    debug_assert!(h.state() != PGAIO_HS_IDLE);

    g::HoldInterrupts();

    if h.state() == PGAIO_HS_COMPLETED_SHARED {
        let local_result = crate::callback::pgaio_io_call_complete_local(index);
        h.set_state(PGAIO_HS_COMPLETED_LOCAL);

        // SAFETY: owner thread past COMPLETED_SHARED; completer is done.
        let d = unsafe { h.data() };
        if !d.report_return.is_null() {
            // SAFETY: report_return points into the issuer's live operation
            unsafe {
                (*d.report_return).result = local_result;
                (*d.report_return).target_data = d.target_data;
            }
        }
    }

    if h.state() != PGAIO_HS_HANDED_OUT {
        // SAFETY: owner-thread slot access.
        unsafe {
            let mb = my_backend();
            dclist_delete_from(&mut mb.in_flight_ios, index);
        }
    }

    // SAFETY: owner thread; no concurrent access in these states.
    let d = unsafe { h.data() };
    if let Some(owner) = d.resowner.take() {
        resowner::ResourceOwnerForgetAioHandle(owner, index as usize);
    }

    // Generation bump first, then state (Release publishes both), then field
    // reset: concurrent viewers see either the old generation or IDLE.
    h.generation.fetch_add(1, Ordering::Relaxed);
    h.set_state(PGAIO_HS_IDLE);

    d.op = PGAIO_OP_INVALID;
    d.target = PGAIO_TID_INVALID;
    d.num_callbacks = 0;
    d.handle_data_len = 0;
    d.report_return = std::ptr::null_mut();
    d.distilled_result = PgAioResult { status: PgAioResultStatus::Unknown, ..Default::default() };
    h.flags.store(0, Ordering::Relaxed);
    h.result.store(0, Ordering::Relaxed);

    // SAFETY: owner-thread slot access.
    unsafe {
        let mb = my_backend();
        dclist_push_head(&mut mb.idle_ios, index);
    }

    g::ResumeInterrupts();
}

fn pgaio_io_wait_for_free() -> PgResult<()> {
    let mut reclaimed = 0;

    // SAFETY: owner-thread slot access (copied out, no reference held).
    let io_handle_off = {
        crate::backend_slot(my_backend_procno()).io_handle_off
    };
    let imc = crate::io_max_concurrency() as u32;

    for i in 0..imc {
        let index = io_handle_off + i;
        if ioh(index).state() == PGAIO_HS_COMPLETED_SHARED {
            pgaio_io_reclaim(index);
            reclaimed += 1;
        }
    }

    if reclaimed > 0 {
        return Ok(());
    }

    // SAFETY: owner-thread slot access.
    if unsafe { my_backend() }.num_staged_ios > 0 {
        pgaio_submit_staged()?;
    }

    // SAFETY: as above.
    if unsafe { my_backend() }.idle_ios.count > 0 {
        return Ok(());
    }

    // SAFETY: as above.
    let (in_flight_head, in_flight_count, num_staged, idle_count) = unsafe {
        let mb = my_backend();
        (mb.in_flight_ios.head, mb.in_flight_ios.count, mb.num_staged_ios, mb.idle_ios.count)
    };

    if in_flight_count == 0 {
        ereport(ERROR)
            .errmsg_internal("no free IOs despite no in-flight IOs")
            .errdetail_internal(format!(
                "{} pending, {} in-flight, {} idle IOs",
                num_staged, in_flight_count, idle_count
            ))
            .finish(loc("pgaio_io_wait_for_free"))?;
    }

    {
        let index = in_flight_head;
        let h = ioh(index);
        let generation = h.generation.load(Ordering::Relaxed);

        match h.state() {
            PGAIO_HS_IDLE | PGAIO_HS_DEFINED | PGAIO_HS_HANDED_OUT | PGAIO_HS_STAGED
            | PGAIO_HS_COMPLETED_LOCAL => {
                ereport(ERROR)
                    .errmsg_internal(format!(
                        "shouldn't get here with io:{} in state {}",
                        index,
                        h.state()
                    ))
                    .finish(loc("pgaio_io_wait_for_free"))?;
            }
            PGAIO_HS_COMPLETED_IO | PGAIO_HS_SUBMITTED => {
                // Racy in general, but we only look at our own backend's IOs
                pgaio_io_wait(index, generation)?;
            }
            _ => {
                pgaio_io_reclaim(index);
            }
        }

        // SAFETY: owner-thread slot access.
        if unsafe { my_backend() }.idle_ios.count == 0 {
            panic!("no idle IO after waiting for IO to terminate");
        }
        Ok(())
    }
}

fn state_name(s: u8) -> &'static str {
    match s {
        PGAIO_HS_IDLE => "IDLE",
        PGAIO_HS_HANDED_OUT => "HANDED_OUT",
        PGAIO_HS_DEFINED => "DEFINED",
        PGAIO_HS_STAGED => "STAGED",
        PGAIO_HS_SUBMITTED => "SUBMITTED",
        PGAIO_HS_COMPLETED_IO => "COMPLETED_IO",
        PGAIO_HS_COMPLETED_SHARED => "COMPLETED_SHARED",
        PGAIO_HS_COMPLETED_LOCAL => "COMPLETED_LOCAL",
        _ => "?",
    }
}


fn pgaio_io_from_wref(iow: &PgAioWaitRef) -> (u32, u64) {
    debug_assert!((iow.aio_index as usize) < handle_count());
    let ref_generation = ((iow.generation_upper as u64) << 32) | iow.generation_lower as u64;
    debug_assert!(ref_generation != 0);
    (iow.aio_index, ref_generation)
}

pub fn pgaio_wref_clear(iow: &mut PgAioWaitRef) {
    iow.aio_index = u32::MAX;
}

pub fn pgaio_wref_valid(iow: &PgAioWaitRef) -> bool {
    iow.aio_index != u32::MAX
}

pub fn pgaio_wref_wait(iow: &PgAioWaitRef) -> PgResult<()> {
    let (index, ref_generation) = pgaio_io_from_wref(iow);
    pgaio_io_wait(index, ref_generation)
}

pub fn pgaio_wref_check_done(iow: &PgAioWaitRef) -> bool {
    let (index, ref_generation) = pgaio_io_from_wref(iow);
    let h = ioh(index);

    let (recycled, state) = pgaio_io_was_recycled(h, ref_generation);
    if recycled || state == PGAIO_HS_IDLE {
        return true;
    }

    let am_owner = h.owner_procno == my_backend_procno_or_invalid();
    if state == PGAIO_HS_COMPLETED_SHARED || state == PGAIO_HS_COMPLETED_LOCAL {
        if am_owner {
            pgaio_io_reclaim(index);
        }
        return true;
    }

    false
}


pub fn pgaio_enter_batchmode() -> PgResult<()> {
    // SAFETY: owner-thread slot access.
    let mb = unsafe { my_backend() };
    if mb.in_batchmode {
        ereport(ERROR)
            .errmsg_internal("starting batch while batch already in progress")
            .finish(loc("pgaio_enter_batchmode"))?;
    }
    mb.in_batchmode = true;
    Ok(())
}

pub fn pgaio_exit_batchmode() -> PgResult<()> {
    // SAFETY: owner-thread slot access (not held across submit).
    debug_assert!(unsafe { my_backend() }.in_batchmode);
    pgaio_submit_staged()?;
    // SAFETY: as above.
    unsafe { my_backend() }.in_batchmode = false;
    Ok(())
}

pub fn pgaio_have_staged() -> bool {
    // SAFETY: owner-thread slot access.
    let mb = unsafe { my_backend() };
    debug_assert!(mb.in_batchmode || mb.num_staged_ios == 0);
    mb.num_staged_ios > 0
}

pub fn pgaio_submit_staged() -> PgResult<()> {
    let staged: ([u32; PGAIO_SUBMIT_BATCH_SIZE], usize) = {
        // SAFETY: owner-thread slot access.
        let mb = unsafe { my_backend() };
        let n = mb.num_staged_ios as usize;
        if n == 0 {
            return Ok(());
        }
        (mb.staged_ios, n)
    };

    g::StartCriticalSection();
    let submit_result = match crate::pgaio_method_kind() {
        IoMethodKind::Sync => {
            g::EndCriticalSection();
            ereport(ERROR)
                .errmsg_internal("IO should have been executed synchronously")
                .finish(loc("pgaio_submit_staged"))?;
            unreachable!("ERROR reported");
        }
        IoMethodKind::Worker => {
            crate::method_worker::pgaio_worker_submit(&staged.0[..staged.1])
        }
    };
    g::EndCriticalSection();
    submit_result?;

    // SAFETY: owner-thread slot access.
    unsafe { my_backend() }.num_staged_ios = 0;
    Ok(())
}


pub fn pgaio_error_cleanup() {
    if MY_BACKEND.get().is_none() {
        return;
    }
    // SAFETY: owner-thread slot access.
    let in_batchmode = unsafe {
        let mb = my_backend();
        let was = mb.in_batchmode;
        mb.in_batchmode = false;
        was
    };
    if in_batchmode {
        pgaio_submit_staged().expect("pgaio_error_cleanup: submit staged");
    }
    // SAFETY: as above.
    debug_assert!(unsafe { my_backend() }.num_staged_ios == 0);
}

pub fn AtEOXact_Aio(_is_commit: bool) {
    if MY_BACKEND.get().is_none() {
        return;
    }
    // SAFETY: owner-thread slot access.
    if unsafe { my_backend() }.in_batchmode {
        pgaio_error_cleanup();
        let _ = elog::elog(WARNING, "open AIO batch at end of (sub-)transaction".to_string());
    }
    // SAFETY: as above.
    debug_assert!(unsafe { my_backend() }.num_staged_ios == 0);
}

/// fd. Neither ported method sets wait_on_fd_before_close (that's io_uring's),
pub fn pgaio_closing_fd(_fd: i32) {
    if MY_BACKEND.get().is_none() {
        return;
    }
    // SAFETY: owner-thread slot access.
    if unsafe { my_backend() }.num_staged_ios > 0 {
        pgaio_submit_staged().expect("pgaio_closing_fd: submit staged");
    }
}

pub(crate) fn pgaio_shutdown(code: i32, _arg: datum::Datum) -> PgResult<()> {
    debug_assert!(MY_BACKEND.get().is_some());
    // SAFETY: owner-thread slot access.
    debug_assert!(unsafe { my_backend() }.handed_out_io == NO_HANDLE);

    AtEOXact_Aio(code == 0);

    loop {
        // SAFETY: owner-thread slot access (copied out).
        let candidate = unsafe {
            let mb = my_backend();
            if mb.in_flight_ios.count == 0 {
                None
            } else {
                let index = mb.in_flight_ios.head;
                Some((index, ioh(index).generation.load(Ordering::Relaxed)))
            }
        };
        match candidate {
            None => break,
            Some((index, generation)) => pgaio_io_wait(index, generation)?,
        }
    }

    MY_BACKEND.set(None);
    Ok(())
}
