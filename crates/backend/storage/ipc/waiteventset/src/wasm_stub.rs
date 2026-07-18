// wasm32 backend (P5 boot increment). wasm32-wasip1 is single-threaded and
// socketless: there is no wake pipe (no pipe(2)), no fd to watch for a
// latch, and no second thread that could set a latch while this one blocks.
// Readiness therefore degenerates to time:
//   * poll (timeout 0)  -> nothing is ever ready, return "timeout";
//   * timed wait        -> sleep the full interval (poll_oneoff clock via
//                          std::thread::sleep), then "timeout";
//   * infinite latch wait -> a genuine deadlock on this target -- reported
//                          as a clean ERROR, never a hang.
// Socket events cannot be registered (no sockets on WASI p1); latch and
// postmaster-death registrations are position bookkeeping only (the death
// event is inert by construction in one address space, and the latch fast
// path lives in the generic wait_loop, which re-checks is_set before and
// after every block).

use crate::{wes_error, Latch, PgResult, WaitEvent, WaitEventSetData};
use types_core::PGINVALID_SOCKET;

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
    _set: &mut WaitEventSetData,
    latch: Option<&'static Latch>,
    cur_timeout: i64,
    _occurred_events: &mut [WaitEvent],
) -> PgResult<i32> {
    if cur_timeout == 0 {
        return Ok(-1); // poll: nothing can be ready
    }
    if cur_timeout > 0 {
        std::thread::sleep(std::time::Duration::from_millis(cur_timeout as u64));
        return Ok(-1);
    }
    // Infinite wait: with one thread and no signals, nobody can ever set
    // the latch or produce an event while we sleep.
    let _ = latch;
    Err(wes_error(
        "infinite WaitEventSet wait would deadlock on wasm32 (single thread, no wake sources)",
    ))
}
