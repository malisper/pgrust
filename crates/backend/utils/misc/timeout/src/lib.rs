#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

// Thread-model SIGALRM: a shared timer thread posts + wakes the backend,
// which runs handle_sig_alarm synchronously at drain points; same-thread
// delivery removes C's volatile races (notes/timeout-threads.md).

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

// pgsync by crate law (permit-s5, census row 16): the slots registry is
// locked by the (now door-registered) timer thread AND by arming backends;
// a raw std lock shared with a registered thread is the
// permit-holder-blocks-raw watchdog wedge shape (s2 AVAILABLE precedent).
// Native arm = identical std re-exports (zero cost).
use pgsync::{Mutex, OnceLock};

use pg_clock::Deadline;

use init_small::globals;
use types_core::TimestampTz;
use types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED;
use types_storage::latch::LatchHandle;

pub use timeout_seams::{
    DisableTimeoutParams, TimeoutId, CLIENT_CONNECTION_CHECK_TIMEOUT, DEADLOCK_TIMEOUT,
    IDLE_IN_TRANSACTION_SESSION_TIMEOUT, IDLE_SESSION_TIMEOUT, IDLE_STATS_UPDATE_TIMEOUT,
    LOCK_TIMEOUT, MAX_TIMEOUTS, STANDBY_DEADLOCK_TIMEOUT, STANDBY_LOCK_TIMEOUT, STANDBY_TIMEOUT,
    STARTUP_PACKET_TIMEOUT, STARTUP_PROGRESS_TIMEOUT, STATEMENT_TIMEOUT, TRANSACTION_TIMEOUT,
    USER_TIMEOUT,
};

pub type TimeoutHandlerProc = fn();

const N_TIMEOUTS: usize = MAX_TIMEOUTS as usize;

#[derive(Clone, Copy)]
pub enum EnableTimeoutParams {
    After { id: TimeoutId, delay_ms: i32 },
    At { id: TimeoutId, fin_time: TimestampTz },
    Every { id: TimeoutId, delay_ms: i32 },
}

#[derive(Clone, Copy)]
struct TimeoutParams {
    index: TimeoutId,
    active: bool,
    indicator: bool,
    timeout_handler: Option<TimeoutHandlerProc>,
    start_time: TimestampTz,
    fin_time: TimestampTz,
    interval_in_ms: i32,
}

struct TimeoutData {
    all_timeouts: [TimeoutParams; N_TIMEOUTS],
    // Active list ordered by fin_time then priority; ids index all_timeouts.
    num_active: usize,
    active_timeouts: [TimeoutId; N_TIMEOUTS],
}

impl TimeoutData {
    const fn new() -> Self {
        TimeoutData {
            all_timeouts: [TimeoutParams {
                index: 0,
                active: false,
                indicator: false,
                timeout_handler: None,
                start_time: 0,
                fin_time: 0,
                interval_in_ms: 0,
            }; N_TIMEOUTS],
            num_active: 0,
            active_timeouts: [0; N_TIMEOUTS],
        }
    }
}

thread_local! {
    static DATA: RefCell<TimeoutData> = const { RefCell::new(TimeoutData::new()) };
    static ALL_TIMEOUTS_INITIALIZED: Cell<bool> = const { Cell::new(false) };
    static ALARM_ENABLED: Cell<bool> = const { Cell::new(false) };
    static SIGNAL_PENDING: Cell<bool> = const { Cell::new(false) };
    static SIGNAL_DUE_AT: Cell<TimestampTz> = const { Cell::new(0) };
    // This backend's slot flag, shared with the timer thread.
    static POSTED: RefCell<Option<Arc<AtomicBool>>> = const { RefCell::new(None) };
}

struct TimerSlot {
    // DST P2 (contract §1.3): deadlines live in pg_clock's monotonic domain
    // (law §0.3 keeps them coherent with the backend's wall-domain fin_time
    // under sim); the timer thread parks on the Waiter instead of a raw
    // Condvar, so statement/deadlock timeouts are virtual-time-driven.
    deadline: Option<Deadline>,
    latch: Option<LatchHandle>,
    posted: Arc<AtomicBool>,
    // The owner's InterruptPending: raised with `posted` so a CPU-bound
    // CHECK_FOR_INTERRUPTS reaches the drain, as C's SIGALRM preempts it.
    interrupt_flag: &'static AtomicBool,
}

struct TimerShared {
    slots: Mutex<HashMap<i32, TimerSlot>>,
    // The timer thread's packed waiter::WakerHandle word (0 until the thread
    // publishes it). Armers unpark through this instead of a Condvar notify.
    timer_waker: AtomicU64,
}

fn timer() -> &'static TimerShared {
    static TIMER: OnceLock<&'static TimerShared> = OnceLock::new();
    TIMER.get_or_init(|| {
        let shared: &'static TimerShared = Box::leak(Box::new(TimerShared {
            slots: Mutex::new(HashMap::new()),
            timer_waker: AtomicU64::new(0),
        }));
        // PERMIT-S5 (P3 census row 16): the timer thread spawns through the
        // pgsync::thread child-registering wrapper — under the permit
        // scheduler it registers a slot (synthetic vpid, spawn-fenced) and
        // gates on its first grant, so its deadline parks drive virtual
        // time and its fire-wakes are scheduled ops instead of External
        // (OS-timed) wakes. Native arm = the std re-export, byte-identical.
        // wasm32: wasm32-wasip1 has no threads, so no timer can fire
        // asynchronously (and no SIGALRM exists either) — armed timeouts
        // are recorded but never fire. Known limitation of the boot
        // increment: statement_timeout/lock_timeout are inert on wasm.
        #[cfg(not(target_family = "wasm"))]
        pgsync::thread::Builder::new()
            .name("pg-timeout-timer".into())
            .spawn(move || timer_thread(shared))
            .expect("could not spawn timeout timer thread");
        shared
    })
}

fn timer_thread(shared: &'static TimerShared) -> ! {
    loop {
        // Dekker discipline (waiter::current_handle doc): publish the waker
        // handle BEFORE scanning, then scan, then park. An armer inserts its
        // slot and THEN unparks — so either its insert is seen by this scan,
        // or its unpark hits the just-published handle and the park below
        // returns immediately (unparks are latched). No arm can be lost.
        shared
            .timer_waker
            .store(waiter::current_handle().as_u64(), Ordering::SeqCst);

        let mut slots = shared.slots.lock().unwrap();
        let mut nearest: Option<Deadline> = None;
        let mut fired: Vec<(Arc<AtomicBool>, &'static AtomicBool, Option<LatchHandle>)> =
            Vec::new();
        for slot in slots.values_mut() {
            if let Some(dl) = slot.deadline {
                if dl.expired() {
                    slot.deadline = None;
                    fired.push((Arc::clone(&slot.posted), slot.interrupt_flag, slot.latch));
                } else if nearest.map_or(true, |n| dl < n) {
                    nearest = Some(dl);
                }
            }
        }
        drop(slots);
        if !fired.is_empty() {
            for (posted, interrupt_flag, latch) in fired {
                // The kill(pid, SIGALRM) edge: post, then wake the backend.
                posted.store(true, Ordering::SeqCst);
                interrupt_flag.store(true, Ordering::SeqCst);
                if let Some(latch) = latch {
                    latch::SetLatch(latch);
                }
            }
            continue;
        }
        match nearest {
            Some(dl) => {
                let rem = dl.remaining_ms();
                if rem > 0 {
                    // Timed park: deadline judged by the installed
                    // WaiterClock, whose now_ms delegates to pg_clock (§1.1
                    // invariant) — one timeline with `dl`.
                    let _ = waiter::park_timeout(Duration::from_millis(rem as u64));
                }
            }
            // Untimed park until the next arm (recheck cadence backstop
            // applies, as for every untimed waiter park).
            None => {
                let _ = waiter::park();
            }
        }
    }
}

// setitimer(ITIMER_REAL, {delay, no interval}): one relative one-shot timer
// per backend, a new arm overwriting the previous.
fn arm_timer(delay: Duration) {
    let shared = timer();
    let posted = POSTED.with(|p| {
        p.borrow()
            .as_ref()
            .map(Arc::clone)
            .expect("timeout: InitializeTimeouts has not run in this backend")
    });
    let mut slots = shared.slots.lock().unwrap();
    slots.insert(
        globals::MyProcPid(),
        TimerSlot {
            deadline: Some(Deadline::after(delay)),
            latch: globals::MyLatch(),
            posted,
            interrupt_flag: globals::interrupt_pending_flag(),
        },
    );
    drop(slots);
    // Insert-then-unpark (see the timer_thread Dekker note). A 0 word
    // (thread not yet published) is a Stale no-op — the thread's first scan
    // runs after publish and sees this slot.
    let _ = waiter::unpark_word(shared.timer_waker.load(Ordering::SeqCst));
}

fn disable_alarm() {
    ALARM_ENABLED.with(|c| c.set(false));
}

fn enable_alarm() {
    ALARM_ENABLED.with(|c| c.set(true));
}

fn timestamptz_plus_ms(tz: TimestampTz, ms: i32) -> TimestampTz {
    tz + ms as TimestampTz * 1000
}

fn find_active_timeout(data: &TimeoutData, id: TimeoutId) -> Option<usize> {
    (0..data.num_active).find(|&i| data.active_timeouts[i] == id)
}

fn insert_timeout(data: &mut TimeoutData, id: TimeoutId, index: usize) {
    // C elog(FATAL)s on an out-of-range index; it is an internal invariant.
    assert!(
        index <= data.num_active,
        "timeout index {index} out of range 0..{}",
        data.num_active
    );
    debug_assert!(!data.all_timeouts[id as usize].active);
    data.all_timeouts[id as usize].active = true;

    let mut i = data.num_active;
    while i > index {
        data.active_timeouts[i] = data.active_timeouts[i - 1];
        i -= 1;
    }
    data.active_timeouts[index] = id;
    data.num_active += 1;
}

fn remove_timeout_index(data: &mut TimeoutData, index: usize) {
    assert!(
        index < data.num_active,
        "timeout index {index} out of range 0..{}",
        data.num_active as isize - 1
    );
    let id = data.active_timeouts[index];
    debug_assert!(data.all_timeouts[id as usize].active);
    data.all_timeouts[id as usize].active = false;

    for i in (index + 1)..data.num_active {
        data.active_timeouts[i - 1] = data.active_timeouts[i];
    }
    data.num_active -= 1;
}

fn enable_timeout(
    data: &mut TimeoutData,
    id: TimeoutId,
    now: TimestampTz,
    fin_time: TimestampTz,
    interval_in_ms: i32,
) {
    debug_assert!(ALL_TIMEOUTS_INITIALIZED.with(|c| c.get()));
    debug_assert!(data.all_timeouts[id as usize].timeout_handler.is_some());

    if data.all_timeouts[id as usize].active {
        let pos = find_active_timeout(data, id).unwrap();
        remove_timeout_index(data, pos);
    }

    // Sort by fin_time, then priority (lower id first).
    let mut i = 0;
    while i < data.num_active {
        let old = &data.all_timeouts[data.active_timeouts[i] as usize];
        if fin_time < old.fin_time || (fin_time == old.fin_time && id < old.index) {
            break;
        }
        i += 1;
    }

    let t = &mut data.all_timeouts[id as usize];
    t.indicator = false;
    t.start_time = now;
    t.fin_time = fin_time;
    t.interval_in_ms = interval_in_ms;

    insert_timeout(data, id, i);
}

// TimestampDifference (timestamp.c): non-negative (secs, usecs).
fn timestamp_difference(start: TimestampTz, stop: TimestampTz) -> (i64, i64) {
    let diff = stop - start;
    if diff <= 0 {
        (0, 0)
    } else {
        (diff / 1_000_000, diff % 1_000_000)
    }
}

fn schedule_alarm(data: &TimeoutData, now: TimestampTz) {
    if data.num_active == 0 {
        return;
    }
    let secs;
    let mut usecs;

    // A pending wake more than 10ms overdue is assumed lost.
    if SIGNAL_PENDING.with(|c| c.get()) && now > SIGNAL_DUE_AT.with(|c| c.get()) + 10 * 1000 {
        SIGNAL_PENDING.with(|c| c.set(false));
    }

    let nearest_timeout = data.all_timeouts[data.active_timeouts[0] as usize].fin_time;
    if now > nearest_timeout {
        SIGNAL_PENDING.with(|c| c.set(false));
        secs = 0;
        usecs = 1;
    } else {
        (secs, usecs) = timestamp_difference(now, nearest_timeout);
        if secs == 0 && usecs == 0 {
            usecs = 1;
        }
    }

    enable_alarm();

    // A wake already due at or before the needed time suffices (C's
    // setitimer-avoidance strategy, kept verbatim).
    if SIGNAL_PENDING.with(|c| c.get()) && nearest_timeout >= SIGNAL_DUE_AT.with(|c| c.get()) {
        return;
    }

    SIGNAL_DUE_AT.with(|c| c.set(nearest_timeout));
    SIGNAL_PENDING.with(|c| c.set(true));

    // C's setitimer FATAL branch has no analog: arming cannot fail.
    arm_timer(Duration::new(secs as u64, (usecs * 1000) as u32));
}

fn handle_sig_alarm() {
    globals::HoldInterrupts();

    latch::SetLatch(globals::MyLatch().expect("timeout drain without MyLatch"));

    SIGNAL_PENDING.with(|c| c.set(false));

    if ALARM_ENABLED.with(|c| c.get()) {
        disable_alarm();

        if DATA.with(|d| d.borrow().num_active) > 0 {
            let mut now = timestamp_seams::get_current_timestamp::call();

            loop {
                // Handler runs with no borrow held (handlers re-enter).
                let fired = DATA.with(|d| {
                    let data = &mut *d.borrow_mut();
                    if data.num_active == 0
                        || now < data.all_timeouts[data.active_timeouts[0] as usize].fin_time
                    {
                        return None;
                    }
                    let id = data.active_timeouts[0];
                    remove_timeout_index(data, 0);
                    let t = &mut data.all_timeouts[id as usize];
                    t.indicator = true;
                    Some((id, t.timeout_handler, t.interval_in_ms, t.fin_time))
                });
                let Some((id, handler, interval, fin_time)) = fired else {
                    break;
                };

                (handler.expect("fired timeout has a handler"))();

                if interval > 0 {
                    // Anti-drift: schedule off the intended firing time.
                    let mut new_fin = timestamptz_plus_ms(fin_time, interval);
                    if new_fin < now {
                        new_fin = timestamptz_plus_ms(now, interval);
                    }
                    DATA.with(|d| enable_timeout(&mut d.borrow_mut(), id, now, new_fin, interval));
                }

                now = timestamp_seams::get_current_timestamp::call();
            }

            DATA.with(|d| schedule_alarm(&d.borrow(), now));
        }
    }

    globals::ResumeInterrupts();
}

/// Synchronous SIGALRM delivery: fires iff the timer thread posted a wake.
pub fn ProcessTimeoutInterrupt() {
    // Load-first fast path: this now also runs on every client-IO interrupt
    // check (the ProcessClientRead/WriteInterrupt SIGALRM rendering), so the
    // no-wake case must not clone the Arc.
    let posted = POSTED.with(|p| {
        p.borrow().as_ref().filter(|a| a.load(Ordering::SeqCst)).map(Arc::clone)
    });
    if let Some(posted) = posted {
        if posted.swap(false, Ordering::SeqCst) {
            handle_sig_alarm();
        }
    }
}

// BUG FIX (DEFAULT ON; PGRUST_TIMEOUT_INIT_RESET=0|off restores the legacy
// lost-timeout behavior for ONE TRAIN as the bisection escape, then the knob
// is removed — this is a correctness fix, not a perf arm):
// re-running InitializeTimeouts on a live connection thread (the session
// main re-init after the startup-packet/auth phase) replaces this thread's
// timer slot with `deadline: None`, destroying a still-armed wake — while
// the SIGNAL_PENDING/SIGNAL_DUE_AT thread-locals survive untouched. Every
// later schedule_alarm whose nearest fin_time lands at or past the stale
// SIGNAL_DUE_AT then takes the setitimer-avoidance skip and trusts a wake
// that can no longer arrive: the timeout silently never fires until a
// schedule_alarm runs after SIGNAL_DUE_AT has passed (the "assumed lost"
// recovery). Net effect: any timeout armed within the first
// authentication_timeout seconds of a session whose deadline extends past
// that window is lost (C-divergent — C is immune because its kernel itimer
// survives InitializeTimeouts, keeping signal_pending truthful). The fix
// resets the pending-wake bookkeeping to match the destroyed slot, so the
// next schedule_alarm always arms a real wake. Witnesses: the
// reinit_keeps_long_deadlines_armable unit below (default world) + the
// timeout-e2e early-session long-timeout scenario (both engines).
fn timeout_init_reset_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_TIMEOUT_INIT_RESET").map_or(true, |v| {
            !(v == "0" || v.eq_ignore_ascii_case("off") || v.eq_ignore_ascii_case("false"))
        })
    })
}

pub fn InitializeTimeouts() {
    disable_alarm();

    if timeout_init_reset_enabled() {
        SIGNAL_PENDING.with(|c| c.set(false));
        SIGNAL_DUE_AT.with(|c| c.set(0));
    }

    DATA.with(|d| {
        let data = &mut *d.borrow_mut();
        data.num_active = 0;
        for (i, t) in data.all_timeouts.iter_mut().enumerate() {
            t.index = i as TimeoutId;
            t.active = false;
            t.indicator = false;
            t.timeout_handler = None;
            t.start_time = 0;
            t.fin_time = 0;
            t.interval_in_ms = 0;
        }
    });

    ALL_TIMEOUTS_INITIALIZED.with(|c| c.set(true));

    // C: pqsignal(SIGALRM, handle_sig_alarm).
    let posted = POSTED.with(|p| {
        let mut p = p.borrow_mut();
        if p.is_none() {
            *p = Some(Arc::new(AtomicBool::new(false)));
        }
        Arc::clone(p.as_ref().unwrap())
    });
    posted.store(false, Ordering::SeqCst);
    let shared = timer();
    shared.slots.lock().unwrap().insert(
        globals::MyProcPid(),
        TimerSlot {
            deadline: None,
            latch: globals::MyLatch(),
            posted,
            interrupt_flag: globals::interrupt_pending_flag(),
        },
    );
}

pub fn RegisterTimeout(id: TimeoutId, handler: TimeoutHandlerProc) -> TimeoutId {
    debug_assert!(ALL_TIMEOUTS_INITIALIZED.with(|c| c.get()));

    DATA.with(|d| {
        let data = &mut *d.borrow_mut();
        let mut id = id;
        if id >= USER_TIMEOUT {
            id = (USER_TIMEOUT..MAX_TIMEOUTS)
                .find(|&i| data.all_timeouts[i as usize].timeout_handler.is_none())
                .unwrap_or_else(|| {
                    // C ereport(FATAL)s; backend-fatal either way.
                    panic!(
                        "cannot add more timeout reasons ({:?})",
                        ERRCODE_CONFIGURATION_LIMIT_EXCEEDED
                    )
                });
        }
        debug_assert!(data.all_timeouts[id as usize].timeout_handler.is_none());
        data.all_timeouts[id as usize].timeout_handler = Some(handler);
        id
    })
}

pub fn reschedule_timeouts() {
    if !ALL_TIMEOUTS_INITIALIZED.with(|c| c.get()) {
        return;
    }
    disable_alarm();
    if DATA.with(|d| d.borrow().num_active) > 0 {
        let now = timestamp_seams::get_current_timestamp::call();
        DATA.with(|d| schedule_alarm(&d.borrow(), now));
    }
}

pub fn enable_timeout_after(id: TimeoutId, delay_ms: i32) {
    disable_alarm();
    let now = timestamp_seams::get_current_timestamp::call();
    let fin_time = timestamptz_plus_ms(now, delay_ms);
    DATA.with(|d| {
        let data = &mut *d.borrow_mut();
        enable_timeout(data, id, now, fin_time, 0);
        schedule_alarm(data, now);
    });
}

pub fn enable_timeout_every(id: TimeoutId, fin_time: TimestampTz, delay_ms: i32) {
    disable_alarm();
    let now = timestamp_seams::get_current_timestamp::call();
    DATA.with(|d| {
        let data = &mut *d.borrow_mut();
        enable_timeout(data, id, now, fin_time, delay_ms);
        schedule_alarm(data, now);
    });
}

pub fn enable_timeout_at(id: TimeoutId, fin_time: TimestampTz) {
    disable_alarm();
    let now = timestamp_seams::get_current_timestamp::call();
    DATA.with(|d| {
        let data = &mut *d.borrow_mut();
        enable_timeout(data, id, now, fin_time, 0);
        schedule_alarm(data, now);
    });
}

pub fn enable_timeouts(timeouts: &[EnableTimeoutParams]) {
    disable_alarm();
    let now = timestamp_seams::get_current_timestamp::call();
    DATA.with(|d| {
        let data = &mut *d.borrow_mut();
        for t in timeouts {
            match *t {
                EnableTimeoutParams::After { id, delay_ms } => {
                    enable_timeout(data, id, now, timestamptz_plus_ms(now, delay_ms), 0);
                }
                EnableTimeoutParams::At { id, fin_time } => {
                    enable_timeout(data, id, now, fin_time, 0);
                }
                EnableTimeoutParams::Every { id, delay_ms } => {
                    enable_timeout(data, id, now, timestamptz_plus_ms(now, delay_ms), delay_ms);
                }
            }
        }
        schedule_alarm(data, now);
    });
}

pub fn disable_timeout(id: TimeoutId, keep_indicator: bool) {
    debug_assert!(ALL_TIMEOUTS_INITIALIZED.with(|c| c.get()));
    debug_assert!(DATA.with(|d| d.borrow().all_timeouts[id as usize].timeout_handler.is_some()));

    disable_alarm();
    DATA.with(|d| {
        let data = &mut *d.borrow_mut();
        if data.all_timeouts[id as usize].active {
            let pos = find_active_timeout(data, id).unwrap();
            remove_timeout_index(data, pos);
        }
        if !keep_indicator {
            data.all_timeouts[id as usize].indicator = false;
        }
    });
    if DATA.with(|d| d.borrow().num_active) > 0 {
        let now = timestamp_seams::get_current_timestamp::call();
        DATA.with(|d| schedule_alarm(&d.borrow(), now));
    }
}

pub fn disable_timeouts(timeouts: &[DisableTimeoutParams]) {
    debug_assert!(ALL_TIMEOUTS_INITIALIZED.with(|c| c.get()));

    disable_alarm();
    DATA.with(|d| {
        let data = &mut *d.borrow_mut();
        for t in timeouts {
            debug_assert!(data.all_timeouts[t.id as usize].timeout_handler.is_some());
            if data.all_timeouts[t.id as usize].active {
                let pos = find_active_timeout(data, t.id).unwrap();
                remove_timeout_index(data, pos);
            }
            if !t.keep_indicator {
                data.all_timeouts[t.id as usize].indicator = false;
            }
        }
    });
    if DATA.with(|d| d.borrow().num_active) > 0 {
        let now = timestamp_seams::get_current_timestamp::call();
        DATA.with(|d| schedule_alarm(&d.borrow(), now));
    }
}

pub fn disable_all_timeouts(keep_indicators: bool) {
    disable_alarm();

    DATA.with(|d| {
        let data = &mut *d.borrow_mut();
        data.num_active = 0;
        for t in data.all_timeouts.iter_mut() {
            t.active = false;
            if !keep_indicators {
                t.indicator = false;
            }
        }
    });
}

pub fn get_timeout_active(id: TimeoutId) -> bool {
    DATA.with(|d| d.borrow().all_timeouts[id as usize].active)
}

pub fn get_timeout_indicator(id: TimeoutId, reset_indicator: bool) -> bool {
    DATA.with(|d| {
        let data = &mut *d.borrow_mut();
        if data.all_timeouts[id as usize].indicator {
            if reset_indicator {
                data.all_timeouts[id as usize].indicator = false;
            }
            return true;
        }
        false
    })
}

pub fn get_timeout_start_time(id: TimeoutId) -> TimestampTz {
    DATA.with(|d| d.borrow().all_timeouts[id as usize].start_time)
}

pub fn get_timeout_finish_time(id: TimeoutId) -> TimestampTz {
    DATA.with(|d| d.borrow().all_timeouts[id as usize].fin_time)
}

pub fn init_seams() {
    use timeout_seams as s;
    s::initialize_timeouts::set(InitializeTimeouts);
    s::register_timeout::set(RegisterTimeout);
    s::reschedule_timeouts::set(|| {
        reschedule_timeouts();
        Ok(())
    });
    s::enable_timeout_after::set(|id, delay_ms| {
        enable_timeout_after(id, delay_ms);
        Ok(())
    });
    s::enable_timeouts::set(|timeouts| {
        let params: Vec<EnableTimeoutParams> = timeouts
            .iter()
            .map(|t| EnableTimeoutParams::After {
                id: t.id,
                delay_ms: t.delay_ms,
            })
            .collect();
        enable_timeouts(&params);
        Ok(())
    });
    s::disable_timeout::set(|id, keep_indicator| {
        disable_timeout(id, keep_indicator);
        Ok(())
    });
    s::disable_timeouts::set(disable_timeouts);
    s::disable_all_timeouts::set(|keep_indicators| {
        disable_all_timeouts(keep_indicators);
        Ok(())
    });
    s::get_timeout_active::set(get_timeout_active);
    s::get_timeout_indicator::set(get_timeout_indicator);
    s::get_timeout_start_time::set(get_timeout_start_time);
    s::get_timeout_finish_time::set(get_timeout_finish_time);
    s::process_timeout_interrupt::set(ProcessTimeoutInterrupt);
}

#[cfg(test)]
mod tests;
