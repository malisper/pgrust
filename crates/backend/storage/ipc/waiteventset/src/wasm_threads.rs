// wasm32 + atomics backend (wasm32-wasip1-threads). Same shape as the
// epoll/kqueue backends, one readiness source shorter: this target has real
// threads (wasi-libc pthreads over a shared memory) but still no sockets and
// no pipe(2), so the ONLY event that can ever be delivered is the latch, and
// the only wake route into a blocked backend is `SetLatch` ->
// `waiter::unpark`.
//
// Hence the split from `epoll.rs`: there is no fd to hand a kernel, so the
// latch wait cannot use the waiter's fd-park mode (`LATCH_FD_PARK` below is
// false and the generic loop leaves this thread's waiter in `Idle`); the
// block itself IS `waiter::park`/`park_timeout` on the slot's Mutex+Condvar
// — a `memory.atomic.wait32` under wasi-libc, the same primitive
// `pthread_join` already blocks on here.
//
// Ordering: the generic `wait_loop` publishes this thread's waker into
// `Latch.waker`, arms `maybe_sleeping`, and re-checks `is_set` BEFORE
// calling us (the Dekker arm). A `SetLatch` that lands in the window
// between that re-check and the park below unparks an `Idle` waiter, which
// latches `Notified`, and `park_core` consumes it and returns immediately —
// so no wake can be lost here, exactly as the wake-pipe byte cannot be lost
// on the epoll path.
//
// Inert / rejected, as on the stub:
//   * WL_POSTMASTER_DEATH / WL_EXIT_ON_PM_DEATH — one address space, the
//     postmaster cannot die while a backend runs; the generic layer
//     registers them without handing us anything.
//   * socket events — WASI p1 has no sockets. The wire lanes that run on
//     this target (`--stdio-wire` / `--stdio-wire-threaded`) do blocking
//     `read(0)`/`write(1)` on the host's SAB pipes and never call
//     WaitLatchOrSocket, so rejecting them costs nothing and keeps the
//     failure loud if something ever does.

use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;

use crate::{wes_error, Latch, PgResult, WaitEvent, WaitEventSetData};
use types_core::PGINVALID_SOCKET;
use types_storage::waiteventset::WL_LATCH_SET;

// Latch waits block in `wait_block` on the waiter itself (non-fd `Parked`
// mode), not in a kernel call with the wake pipe registered: the generic
// loop must NOT put this thread's waiter into fd-park mode first.
pub(crate) const LATCH_FD_PARK: bool = false;

pub(crate) struct BackendSet {}

impl BackendSet {
    pub(crate) fn create(_nevents: i32) -> PgResult<Self> {
        Ok(BackendSet {})
    }

    pub(crate) fn free(&self) {}

    pub(crate) fn register(&self, event: &WaitEvent, _old_events: u32) -> PgResult<()> {
        // Latch registrations carry PGINVALID_SOCKET on wasm (no wake pipe:
        // wakeup_read_fd's wasm arm); pm-death is registered inert by the
        // generic layer. Anything with a real fd is a socket wait.
        if event.fd != PGINVALID_SOCKET {
            return Err(wes_error(
                "socket wait events are not supported on wasm32 (WASI p1 has no sockets)",
            ));
        }
        Ok(())
    }
}

pub(crate) fn wait_block(
    set: &mut WaitEventSetData,
    latch: Option<&'static Latch>,
    cur_timeout: i64,
    occurred_events: &mut [WaitEvent],
) -> PgResult<i32> {
    let Some(l) = latch else {
        // No latch in the set: nothing on this target can make a registered
        // event ready, so the wait degenerates to time.
        if cur_timeout == 0 {
            return Ok(-1); // poll: nothing can be ready
        }
        if cur_timeout > 0 {
            // waiter::sleep, not thread::sleep: rides the one monotonic
            // authority (and a stray unpark ending it early is harmless —
            // the generic loop recomputes the remaining timeout).
            waiter::sleep(Duration::from_millis(cur_timeout as u64));
            return Ok(-1);
        }
        return Err(wes_error(
            "infinite WaitEventSet wait with no latch would deadlock on wasm32 \
             (no sockets, no postmaster-death route)",
        ));
    };

    // Zero timeout: the generic loop's "poll once for the other events that
    // fit" lap after it already reported the latch. Nothing else exists.
    if cur_timeout == 0 || occurred_events.is_empty() {
        return Ok(-1);
    }

    let parked = if cur_timeout > 0 {
        waiter::park_timeout(Duration::from_millis(cur_timeout as u64))
    } else {
        waiter::park()
    };

    // C's post-epoll_wait test, minus the pipe: the latch is authoritative
    // whichever way the park ended (Notified / recheck cadence / timeout).
    if l.maybe_sleeping.load(SeqCst) != 0 && l.is_set() {
        let cur_event = &set.events[set.latch_pos as usize];
        occurred_events[0] = WaitEvent {
            pos: cur_event.pos,
            user_data: cur_event.user_data,
            events: WL_LATCH_SET,
            fd: PGINVALID_SOCKET,
        };
        return Ok(1);
    }

    // Either the caller's deadline expired or this was a lost-wake recheck
    // lap / a stale unpark: report "nothing ready" and let the generic loop
    // decide (it re-arms until the CALLER's deadline, GL-RECWAKE-1).
    let _ = parked;
    Ok(-1)
}
