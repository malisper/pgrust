//! Port of `src/backend/utils/misc/timeout.c` (PostgreSQL 18.3): multiplexing
//! one SIGALRM timer across many timeout reasons.
//!
//! All of the module's state is per-backend (each C backend is a process), so
//! it lives in `thread_local!`s here rather than shared statics. C marks the
//! state `volatile` because the SIGALRM handler mutates it asynchronously; the
//! mutual exclusion that makes that safe is the `alarm_enabled` flag plus the
//! discipline that the mainline always calls `disable_alarm()` before touching
//! the data structures and `schedule_alarm()` last (which re-enables the
//! handler). We mirror that discipline exactly: the handler only borrows the
//! state arrays when `alarm_enabled` was true, and it clears the flag before
//! doing so, so handler and mainline never hold overlapping borrows.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

// Sibling files of the `backend-utils-misc-more2` unit. `timeout.c` is this
// crate's lib body below; these are the remaining manifest c_sources.
pub mod conffiles;
pub mod injection_point;
pub mod pg_config;
pub mod tzparser;

use std::cell::{Cell, RefCell};

use types_core::TimestampTz;
use types_error::{PgError, PgResult, ERRCODE_CONFIGURATION_LIMIT_EXCEEDED, FATAL};
use types_signal::SigHandler;
use types_timeout::{
    DisableTimeoutParams, EnableTimeoutParams, TimeoutHandlerProc, TimeoutId, TimeoutType,
    MAX_TIMEOUTS,
};

/// `TimestampTzPlusMilliseconds(tz, ms)` (`utils/timestamp.h`): add a
/// millisecond delay to a timestamp, expressed in microseconds.
#[inline]
fn timestamptz_plus_milliseconds(tz: TimestampTz, ms: i32) -> TimestampTz {
    tz + ms as TimestampTz * 1000
}

/// `struct timeout_params` — data about any one timeout reason.
#[derive(Clone, Copy)]
struct TimeoutParams {
    /// Identifier of the timeout reason (this slot's index).
    index: usize,
    /// True if timeout is in the active list.
    active: bool,
    /// True if timeout has occurred.
    indicator: bool,
    /// Callback for timeout, or `None` if not registered.
    timeout_handler: Option<TimeoutHandlerProc>,
    /// Time the timeout was last activated.
    start_time: TimestampTz,
    /// Time it is, or was last, due to fire.
    fin_time: TimestampTz,
    /// Time between firings, or 0 if just once.
    interval_in_ms: i32,
}

impl TimeoutParams {
    const fn new(index: usize) -> Self {
        TimeoutParams {
            index,
            active: false,
            indicator: false,
            timeout_handler: None,
            start_time: 0,
            fin_time: 0,
            interval_in_ms: 0,
        }
    }
}

/// The arrays the signal handler and mainline share: every timeout reason and
/// the active list ordered by `fin_time` then priority. `active_timeouts`
/// stores indices into `all_timeouts` (C stores pointers; an index is the
/// faithful safe equivalent for a fixed-size array).
struct TimeoutData {
    all_timeouts: [TimeoutParams; MAX_TIMEOUTS],
    num_active_timeouts: usize,
    active_timeouts: [usize; MAX_TIMEOUTS],
}

impl TimeoutData {
    const fn new() -> Self {
        // `all_timeouts[i].index` is set in InitializeTimeouts; start at 0.
        TimeoutData {
            all_timeouts: [TimeoutParams::new(0); MAX_TIMEOUTS],
            num_active_timeouts: 0,
            active_timeouts: [0; MAX_TIMEOUTS],
        }
    }
}

thread_local! {
    static TIMEOUT_DATA: RefCell<TimeoutData> = const { RefCell::new(TimeoutData::new()) };

    /// `static bool all_timeouts_initialized`.
    static ALL_TIMEOUTS_INITIALIZED: Cell<bool> = const { Cell::new(false) };

    /// `volatile sig_atomic_t alarm_enabled`. Whether the signal handler may
    /// do anything; lets us mutate the data structures without disabling the
    /// kernel timer.
    static ALARM_ENABLED: Cell<bool> = const { Cell::new(false) };

    /// `volatile sig_atomic_t signal_pending`. The handler unconditionally
    /// resets this to false, so it can change asynchronously even when
    /// `alarm_enabled` is false.
    static SIGNAL_PENDING: Cell<bool> = const { Cell::new(false) };

    /// `volatile TimestampTz signal_due_at`. Valid only when `signal_pending`.
    static SIGNAL_DUE_AT: Cell<TimestampTz> = const { Cell::new(0) };
}

#[inline]
fn disable_alarm() {
    ALARM_ENABLED.with(|c| c.set(false));
}

#[inline]
fn enable_alarm() {
    ALARM_ENABLED.with(|c| c.set(true));
}

/*****************************************************************************
 * Internal helper functions
 *
 * For all of these, it is caller's responsibility to protect them from
 * interruption by the signal handler.  Generally, call disable_alarm()
 * first to prevent interruption, then update state, and last call
 * schedule_alarm(), which will re-enable the signal handler if needed.
 *****************************************************************************/

/// Find the index of a given timeout reason in the active array, or `None`.
fn find_active_timeout(data: &TimeoutData, id: usize) -> Option<usize> {
    for i in 0..data.num_active_timeouts {
        if data.all_timeouts[data.active_timeouts[i]].index == id {
            return Some(i);
        }
    }
    None
}

/// Insert specified timeout reason into the active list at the given index.
fn insert_timeout(data: &mut TimeoutData, id: usize, index: usize) -> PgResult<()> {
    if index > data.num_active_timeouts {
        return Err(elog_fatal(format!(
            "timeout index {} out of range 0..{}",
            index, data.num_active_timeouts
        )));
    }

    debug_assert!(!data.all_timeouts[id].active);
    data.all_timeouts[id].active = true;

    let mut i = data.num_active_timeouts;
    while i > index {
        data.active_timeouts[i] = data.active_timeouts[i - 1];
        i -= 1;
    }

    data.active_timeouts[index] = id;
    data.num_active_timeouts += 1;
    Ok(())
}

/// Remove the index'th element from the timeout list.
fn remove_timeout_index(data: &mut TimeoutData, index: usize) -> PgResult<()> {
    if index >= data.num_active_timeouts {
        return Err(elog_fatal(format!(
            "timeout index {} out of range 0..{}",
            index,
            data.num_active_timeouts as isize - 1
        )));
    }

    debug_assert!(data.all_timeouts[data.active_timeouts[index]].active);
    data.all_timeouts[data.active_timeouts[index]].active = false;

    for i in (index + 1)..data.num_active_timeouts {
        data.active_timeouts[i - 1] = data.active_timeouts[i];
    }
    data.num_active_timeouts -= 1;
    Ok(())
}

/// Enable the specified timeout reason.
fn enable_timeout(
    data: &mut TimeoutData,
    id: usize,
    now: TimestampTz,
    fin_time: TimestampTz,
    interval_in_ms: i32,
) -> PgResult<()> {
    debug_assert!(ALL_TIMEOUTS_INITIALIZED.with(|c| c.get()));
    debug_assert!(data.all_timeouts[id].timeout_handler.is_some());

    // If already active, momentarily disable it: reschedule the timeout.
    if data.all_timeouts[id].active {
        let pos = find_active_timeout(data, id).expect("active timeout must be in active list");
        remove_timeout_index(data, pos)?;
    }

    // Find where to insert: sort by fin_time, and for equal fin_time by
    // priority (lower id first).
    let mut i = 0;
    while i < data.num_active_timeouts {
        let old_timeout = &data.all_timeouts[data.active_timeouts[i]];
        if fin_time < old_timeout.fin_time {
            break;
        }
        if fin_time == old_timeout.fin_time && id < old_timeout.index {
            break;
        }
        i += 1;
    }

    data.all_timeouts[id].indicator = false;
    data.all_timeouts[id].start_time = now;
    data.all_timeouts[id].fin_time = fin_time;
    data.all_timeouts[id].interval_in_ms = interval_in_ms;

    insert_timeout(data, id, i)
}

/// Schedule alarm for the next active timeout, if any.
///
/// The caller has obtained `now`, or a close-enough approximation. (A tick or
/// two of slop is fine; passing a "now" in the future would be bad.)
fn schedule_alarm(data: &TimeoutData, now: TimestampTz) -> PgResult<()> {
    if data.num_active_timeouts > 0 {
        let mut timeval: libc::itimerval = unsafe { std::mem::zeroed() };
        let secs;
        let usecs;

        // If we think there's a signal pending, but current time is more than
        // 10ms past when the signal was due, assume the timeout request got
        // lost; clear signal_pending so we reset the interrupt below.
        if SIGNAL_PENDING.with(|c| c.get()) && now > SIGNAL_DUE_AT.with(|c| c.get()) + 10 * 1000 {
            SIGNAL_PENDING.with(|c| c.set(false));
        }

        // Time remaining till the nearest pending timeout. If negative, assume
        // we missed an interrupt and clear signal_pending.
        let nearest_timeout = data.all_timeouts[data.active_timeouts[0]].fin_time;
        if now > nearest_timeout {
            SIGNAL_PENDING.with(|c| c.set(false));
            // force an interrupt as soon as possible
            secs = 0;
            usecs = 1;
        } else {
            let (s, mut u) = backend_utils_adt_timestamp_seams::timestamp_difference::call(
                now,
                nearest_timeout,
            );
            // It's possible the difference is less than a microsecond; ensure
            // we don't cancel, rather than set, the interrupt.
            if s == 0 && u == 0 {
                u = 1;
            }
            secs = s;
            usecs = u;
        }

        timeval.it_value.tv_sec = secs as libc::time_t;
        timeval.it_value.tv_usec = usecs as libc::suseconds_t;

        // We must enable the signal handler before calling setitimer(); doing
        // it the other way risks the interrupt firing before alarm_enabled is
        // set, so the handler would do nothing.
        enable_alarm();

        // If there is already an interrupt pending at or before the needed
        // time, we need not do anything more.
        if SIGNAL_PENDING.with(|c| c.get()) && nearest_timeout >= SIGNAL_DUE_AT.with(|c| c.get()) {
            return Ok(());
        }

        // As with enable_alarm(), we must set signal_pending *before* calling
        // setitimer().
        SIGNAL_DUE_AT.with(|c| c.set(nearest_timeout));
        SIGNAL_PENDING.with(|c| c.set(true));

        // Set the alarm timer.
        let rc = unsafe {
            libc::setitimer(libc::ITIMER_REAL, &timeval, std::ptr::null_mut())
        };
        if rc != 0 {
            // Clearing signal_pending here is a bit pro forma, but not entirely
            // so, since something in the FATAL exit path could try to use
            // timeout facilities.
            SIGNAL_PENDING.with(|c| c.set(false));
            let errno = std::io::Error::last_os_error();
            return Err(elog_fatal(format!(
                "could not enable SIGALRM timer: {errno}"
            )));
        }
    }
    Ok(())
}

/*****************************************************************************
 * Signal handler
 *****************************************************************************/

/// Signal handler for SIGALRM.
///
/// Process any active timeout reasons and then reschedule the interrupt as
/// needed. `_signal_arg` is C's `SIGNAL_ARGS` (the signal number).
fn handle_sig_alarm(_signal_arg: i32) {
    // Bump the holdoff counter, so nothing we call processes interrupts
    // directly. No timeout handler should do that, but be sure.
    backend_utils_init_small_seams::hold_interrupts::call();

    // SIGALRM is always cause for waking anything waiting on the process latch.
    backend_storage_ipc_latch_seams::set_latch_my_latch::call();

    // Always reset signal_pending, even if !alarm_enabled, since no signal is
    // now pending.
    SIGNAL_PENDING.with(|c| c.set(false));

    // Fire any pending timeouts, but only if we're enabled to do so.
    if ALARM_ENABLED.with(|c| c.get()) {
        // Disable alarms, in case this platform lets signal handlers interrupt
        // themselves; schedule_alarm() will re-enable if appropriate.
        disable_alarm();

        let num_active = TIMEOUT_DATA.with(|d| d.borrow().num_active_timeouts);
        if num_active > 0 {
            let mut now = backend_utils_adt_timestamp_seams::get_current_timestamp::call();

            // While the first pending timeout has been reached ...
            loop {
                // Read the frontmost reached timeout and remove it, then run
                // its handler outside the borrow (the handler may re-enter the
                // public API, e.g. CheckDeadLock).
                let fired = TIMEOUT_DATA.with(|d| {
                    let mut data = d.borrow_mut();
                    if data.num_active_timeouts == 0
                        || now < data.all_timeouts[data.active_timeouts[0]].fin_time
                    {
                        return None;
                    }
                    let this_idx = data.active_timeouts[0];

                    // Remove it from the active list.
                    remove_timeout_index(&mut data, 0)
                        .expect("front of active list is always in range");

                    // Mark it as fired.
                    data.all_timeouts[this_idx].indicator = true;

                    let handler = data.all_timeouts[this_idx].timeout_handler;
                    let interval = data.all_timeouts[this_idx].interval_in_ms;
                    let fin_time = data.all_timeouts[this_idx].fin_time;
                    Some((this_idx, handler, interval, fin_time))
                });

                let Some((this_idx, handler, interval, fin_time)) = fired else {
                    break;
                };

                // Call its handler function.
                (handler.expect("fired timeout must have a handler"))();

                // If it should fire repeatedly, re-enable it.
                if interval > 0 {
                    // To guard against drift, schedule the next instance based
                    // on the intended firing time rather than the actual one;
                    // but if we missed an entire cycle, fall back to "now".
                    let mut new_fin_time = timestamptz_plus_milliseconds(fin_time, interval);
                    if new_fin_time < now {
                        new_fin_time = timestamptz_plus_milliseconds(now, interval);
                    }
                    TIMEOUT_DATA
                        .with(|d| enable_timeout(&mut d.borrow_mut(), this_idx, now, new_fin_time, interval))
                        .expect("re-enabling a periodic timeout cannot overflow the active list");
                }

                // The handler might not be cheap (CheckDeadLock for instance),
                // so update our idea of "now" after each one.
                now = backend_utils_adt_timestamp_seams::get_current_timestamp::call();
            }

            // Done firing timeouts, so reschedule next interrupt if any.
            TIMEOUT_DATA
                .with(|d| schedule_alarm(&d.borrow(), now))
                .expect("rescheduling from the signal handler cannot fail");
        }
    }

    backend_utils_init_small_seams::resume_interrupts::call();
}

/*****************************************************************************
 * Public API
 *****************************************************************************/

/// `InitializeTimeouts()` — initialize the timeout module.
///
/// Must be called in every process that wants to use timeouts. If the process
/// was forked from another also using this module, call this before
/// re-enabling signals, so parent handlers don't run in the child.
pub fn InitializeTimeouts() {
    // Initialize, or re-initialize, all local state.
    disable_alarm();

    TIMEOUT_DATA.with(|d| {
        let mut data = d.borrow_mut();
        data.num_active_timeouts = 0;
        for i in 0..MAX_TIMEOUTS {
            data.all_timeouts[i].index = i;
            data.all_timeouts[i].active = false;
            data.all_timeouts[i].indicator = false;
            data.all_timeouts[i].timeout_handler = None;
            data.all_timeouts[i].start_time = 0;
            data.all_timeouts[i].fin_time = 0;
            data.all_timeouts[i].interval_in_ms = 0;
        }
    });

    ALL_TIMEOUTS_INITIALIZED.with(|c| c.set(true));

    // Now establish the signal handler.
    port_pqsignal_seams::pqsignal::call(libc::SIGALRM, SigHandler::Handler(handle_sig_alarm));
}

/// `RegisterTimeout(id, handler)` — register a timeout reason.
///
/// For predefined timeouts, just registers the callback. For user-defined
/// timeouts, pass `USER_TIMEOUT`; an id is then allocated and returned.
pub fn RegisterTimeout(id: TimeoutId, handler: TimeoutHandlerProc) -> TimeoutId {
    debug_assert!(ALL_TIMEOUTS_INITIALIZED.with(|c| c.get()));

    // There's no need to disable the signal handler here.

    let idx = TIMEOUT_DATA.with(|d| {
        let mut data = d.borrow_mut();
        let mut idx = id.as_index();

        if idx >= TimeoutId::USER_TIMEOUT.as_index() {
            // Allocate a user-defined timeout reason.
            idx = TimeoutId::USER_TIMEOUT.as_index();
            while idx < MAX_TIMEOUTS {
                if data.all_timeouts[idx].timeout_handler.is_none() {
                    break;
                }
                idx += 1;
            }
            if idx >= MAX_TIMEOUTS {
                // ereport(FATAL): cannot add more timeout reasons.
                panic!(
                    "{}",
                    elog_fatal_code(
                        "cannot add more timeout reasons".to_string(),
                        ERRCODE_CONFIGURATION_LIMIT_EXCEEDED,
                    )
                );
            }
        }

        debug_assert!(data.all_timeouts[idx].timeout_handler.is_none());
        data.all_timeouts[idx].timeout_handler = Some(handler);
        idx
    });

    TimeoutId::from_index(idx)
}

/// `reschedule_timeouts()` — reschedule any pending SIGALRM interrupt.
///
/// Useful during error recovery if query cancel lost a SIGALRM event. Not
/// necessary if any public enable_/disable_ function runs in the same area,
/// since those all `schedule_alarm()` internally.
pub fn reschedule_timeouts() {
    // For flexibility, allow this to be called before we're initialized.
    if !ALL_TIMEOUTS_INITIALIZED.with(|c| c.get()) {
        return;
    }

    // Disable timeout interrupts for safety.
    disable_alarm();

    // Reschedule the interrupt, if any timeouts remain active.
    let num_active = TIMEOUT_DATA.with(|d| d.borrow().num_active_timeouts);
    if num_active > 0 {
        let now = backend_utils_adt_timestamp_seams::get_current_timestamp::call();
        TIMEOUT_DATA
            .with(|d| schedule_alarm(&d.borrow(), now))
            .expect("reschedule_timeouts: schedule_alarm");
    }
}

/// `enable_timeout_after(id, delay_ms)` — fire `id` after `delay_ms` ms.
pub fn enable_timeout_after(id: TimeoutId, delay_ms: i32) -> PgResult<()> {
    // Disable timeout interrupts for safety.
    disable_alarm();

    // Queue the timeout at the appropriate time.
    let now = backend_utils_adt_timestamp_seams::get_current_timestamp::call();
    let fin_time = timestamptz_plus_milliseconds(now, delay_ms);
    TIMEOUT_DATA.with(|d| enable_timeout(&mut d.borrow_mut(), id.as_index(), now, fin_time, 0))?;

    // Set the timer interrupt.
    TIMEOUT_DATA.with(|d| schedule_alarm(&d.borrow(), now))
}

/// `enable_timeout_every(id, fin_time, delay_ms)` — fire periodically, first
/// at `fin_time`, then every `delay_ms` ms.
pub fn enable_timeout_every(id: TimeoutId, fin_time: TimestampTz, delay_ms: i32) {
    disable_alarm();

    let now = backend_utils_adt_timestamp_seams::get_current_timestamp::call();
    TIMEOUT_DATA
        .with(|d| enable_timeout(&mut d.borrow_mut(), id.as_index(), now, fin_time, delay_ms))
        .expect("enable_timeout_every: enable_timeout");

    TIMEOUT_DATA
        .with(|d| schedule_alarm(&d.borrow(), now))
        .expect("enable_timeout_every: schedule_alarm");
}

/// `enable_timeout_at(id, fin_time)` — fire `id` at the specified time.
///
/// Supports cases that calculate the timeout by reference to a point other
/// than "now"; otherwise prefer `enable_timeout_after`.
pub fn enable_timeout_at(id: TimeoutId, fin_time: TimestampTz) -> PgResult<()> {
    disable_alarm();

    let now = backend_utils_adt_timestamp_seams::get_current_timestamp::call();
    TIMEOUT_DATA.with(|d| enable_timeout(&mut d.borrow_mut(), id.as_index(), now, fin_time, 0))?;

    TIMEOUT_DATA.with(|d| schedule_alarm(&d.borrow(), now))
}

/// `enable_timeouts(timeouts, count)` — enable multiple timeouts at once,
/// like calling `enable_timeout_after`/`enable_timeout_at` repeatedly but with
/// one `GetCurrentTimestamp()` and `setitimer()`.
pub fn enable_timeouts(timeouts: &[EnableTimeoutParams]) -> PgResult<()> {
    disable_alarm();

    // Queue the timeout(s) at the appropriate times.
    let now = backend_utils_adt_timestamp_seams::get_current_timestamp::call();

    TIMEOUT_DATA.with(|d| {
        let mut data = d.borrow_mut();
        for t in timeouts {
            let id = t.id.as_index();
            match t.r#type {
                TimeoutType::TMPARAM_AFTER => {
                    let fin_time = timestamptz_plus_milliseconds(now, t.delay_ms);
                    enable_timeout(&mut data, id, now, fin_time, 0)?;
                }
                TimeoutType::TMPARAM_AT => {
                    enable_timeout(&mut data, id, now, t.fin_time, 0)?;
                }
                TimeoutType::TMPARAM_EVERY => {
                    let fin_time = timestamptz_plus_milliseconds(now, t.delay_ms);
                    enable_timeout(&mut data, id, now, fin_time, t.delay_ms)?;
                }
            }
        }
        Ok::<(), PgError>(())
    })?;

    // Set the timer interrupt.
    TIMEOUT_DATA.with(|d| schedule_alarm(&d.borrow(), now))
}

/// `disable_timeout(id, keep_indicator)` — cancel the specified timeout.
///
/// The fired indicator is reset unless `keep_indicator`. Other active timeouts
/// remain in force; disabling an already-disabled timeout is not an error.
pub fn disable_timeout(id: TimeoutId, keep_indicator: bool) {
    debug_assert!(ALL_TIMEOUTS_INITIALIZED.with(|c| c.get()));

    disable_alarm();

    let now = TIMEOUT_DATA.with(|d| {
        let mut data = d.borrow_mut();
        let idx = id.as_index();
        debug_assert!(data.all_timeouts[idx].timeout_handler.is_some());

        // Find the timeout and remove it from the active list.
        if data.all_timeouts[idx].active {
            let pos =
                find_active_timeout(&data, idx).expect("active timeout must be in active list");
            remove_timeout_index(&mut data, pos)
                .expect("disable_timeout: remove_timeout_index");
        }

        // Mark it inactive, whether it was active or not.
        if !keep_indicator {
            data.all_timeouts[idx].indicator = false;
        }

        if data.num_active_timeouts > 0 {
            Some(backend_utils_adt_timestamp_seams::get_current_timestamp::call())
        } else {
            None
        }
    });

    // Reschedule the interrupt, if any timeouts remain active.
    if let Some(now) = now {
        TIMEOUT_DATA
            .with(|d| schedule_alarm(&d.borrow(), now))
            .expect("disable_timeout: schedule_alarm");
    }
}

/// `disable_timeouts(timeouts, count)` — cancel multiple timeouts at once.
///
/// Each indicator is reset unless its `keep_indicator`. Like calling
/// `disable_timeout` repeatedly but with one `GetCurrentTimestamp()`/
/// `setitimer()`.
pub fn disable_timeouts(timeouts: &[DisableTimeoutParams]) -> PgResult<()> {
    debug_assert!(ALL_TIMEOUTS_INITIALIZED.with(|c| c.get()));

    disable_alarm();

    let num_active = TIMEOUT_DATA.with(|d| {
        let mut data = d.borrow_mut();
        for t in timeouts {
            let idx = t.id.as_index();
            debug_assert!(data.all_timeouts[idx].timeout_handler.is_some());

            if data.all_timeouts[idx].active {
                let pos = find_active_timeout(&data, idx)
                    .expect("active timeout must be in active list");
                remove_timeout_index(&mut data, pos)?;
            }

            if !t.keep_indicator {
                data.all_timeouts[idx].indicator = false;
            }
        }
        Ok::<usize, PgError>(data.num_active_timeouts)
    })?;

    // Reschedule the interrupt, if any timeouts remain active.
    if num_active > 0 {
        let now = backend_utils_adt_timestamp_seams::get_current_timestamp::call();
        TIMEOUT_DATA.with(|d| schedule_alarm(&d.borrow(), now))?;
    }
    Ok(())
}

/// `disable_all_timeouts(keep_indicators)` — disable the signal handler,
/// remove all timeouts from the active list, optionally resetting indicators.
pub fn disable_all_timeouts(keep_indicators: bool) -> PgResult<()> {
    disable_alarm();

    // We used to disable the timer interrupt here, but in common usage it's
    // cheaper to leave it enabled; that may save us re-enabling it shortly.

    TIMEOUT_DATA.with(|d| {
        let mut data = d.borrow_mut();
        data.num_active_timeouts = 0;
        for i in 0..MAX_TIMEOUTS {
            data.all_timeouts[i].active = false;
            if !keep_indicators {
                data.all_timeouts[i].indicator = false;
            }
        }
    });
    Ok(())
}

/// `get_timeout_active(id)` — true if the timeout is enabled and not yet
/// fired. Subject to race conditions (it could fire right after we look).
pub fn get_timeout_active(id: TimeoutId) -> bool {
    TIMEOUT_DATA.with(|d| d.borrow().all_timeouts[id.as_index()].active)
}

/// `get_timeout_indicator(id, reset_indicator)` — the I've-been-fired
/// indicator. Resets it when returning true if `reset_indicator`; never resets
/// when returning false (avoids missing a timeout due to races).
pub fn get_timeout_indicator(id: TimeoutId, reset_indicator: bool) -> bool {
    TIMEOUT_DATA.with(|d| {
        let mut data = d.borrow_mut();
        let idx = id.as_index();
        if data.all_timeouts[idx].indicator {
            if reset_indicator {
                data.all_timeouts[idx].indicator = false;
            }
            return true;
        }
        false
    })
}

/// `get_timeout_start_time(id)` — when the timeout was most recently
/// activated; 0 if never activated in this process.
pub fn get_timeout_start_time(id: TimeoutId) -> TimestampTz {
    TIMEOUT_DATA.with(|d| d.borrow().all_timeouts[id.as_index()].start_time)
}

/// `get_timeout_finish_time(id)` — when the timeout is, or most recently was,
/// due to fire; 0 if never activated in this process.
pub fn get_timeout_finish_time(id: TimeoutId) -> TimestampTz {
    TIMEOUT_DATA.with(|d| d.borrow().all_timeouts[id.as_index()].fin_time)
}

/// `elog(FATAL, ...)`: build a FATAL PgError.
fn elog_fatal(message: String) -> PgError {
    PgError::new(FATAL, message)
}

/// `ereport(FATAL, (errcode(code), errmsg(...)))`.
fn elog_fatal_code(message: String, code: types_error::SqlState) -> PgError {
    PgError::new(FATAL, message).with_sqlstate(code)
}

#[cfg(test)]
mod tests;

/// Adapter: `enable_timeout_after` for the `more2-seams` declaration, whose
/// `TimeoutId` is `types_core::TimeoutId` (an identical-discriminant copy of
/// the timeout-reason enum that the xact/postinit consumers use).
fn enable_timeout_after_core(id: types_core::TimeoutId, delay_ms: i32) -> PgResult<()> {
    enable_timeout_after(TimeoutId::from_index(id as usize), delay_ms)
}

/// Adapter: `disable_timeout` for the `more2-seams` declaration. The C
/// function is `void`; the seam's `PgResult` failure surface is never
/// exercised here, so we always return `Ok`.
fn disable_timeout_core(id: types_core::TimeoutId, keep_indicator: bool) -> PgResult<()> {
    disable_timeout(TimeoutId::from_index(id as usize), keep_indicator);
    Ok(())
}

/// Adapter: `reschedule_timeouts` for the `more2-seams` declaration. The C
/// function is `void`; always returns `Ok`.
fn reschedule_timeouts_seam() -> PgResult<()> {
    reschedule_timeouts();
    Ok(())
}

/// Install this crate's seams. This unit owns `timeout.c`, hence both
/// `backend-utils-misc-timeout-seams` and the timeout declarations of the
/// pre-existing `backend-utils-misc-more2-seams` (the seam crate the xact and
/// postinit consumers actually call). Both map to `timeout.c`, so both must be
/// installed here.
pub fn init_seams() {
    use backend_utils_misc_timeout_seams as s;

    s::enable_timeouts::set(enable_timeouts);
    s::disable_all_timeouts::set(disable_all_timeouts);
    s::initialize_timeouts::set(InitializeTimeouts);
    s::register_timeout::set(RegisterTimeout);
    s::enable_timeout_every::set(enable_timeout_every);
    s::disable_timeout::set(disable_timeout);
    s::enable_timeout_after::set(enable_timeout_after);
    s::disable_timeouts::set(disable_timeouts);
    s::get_timeout_start_time::set(get_timeout_start_time);

    use backend_utils_misc_more2_seams as m2;

    m2::enable_timeout_after::set(enable_timeout_after_core);
    m2::disable_timeout::set(disable_timeout_core);
    m2::reschedule_timeouts::set(reschedule_timeouts_seam);

    // conffiles.c: this unit owns the conffiles-seams declarations guc-file.l
    // calls. Adapt the owned `(&str, &Path)` signatures to the seam's
    // `(String, Option<PathBuf>)` shape.
    backend_utils_misc_conffiles_seams::absolute_config_location::set(|location, calling_file| {
        conffiles::absolute_config_location(&location, calling_file.as_deref())
    });
    backend_utils_misc_conffiles_seams::get_conf_files_in_dir::set(
        |includedir, calling_file, elevel| {
            conffiles::get_conf_files_in_dir(&includedir, calling_file.as_deref(), elevel)
        },
    );

    // injection_point.c: this unit owns the injection-point-seams declarations
    // ipci.c sizes/initializes (the `#else`/disabled-build arms here).
    backend_storage_ipc_injection_point_seams::injection_point_shmem_size::set(
        injection_point::injection_point_shmem_size,
    );
    backend_storage_ipc_injection_point_seams::injection_point_shmem_init::set(
        injection_point::injection_point_shmem_init,
    );
}
