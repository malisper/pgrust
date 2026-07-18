// wasm32 stub backend (P5 WS-TOOLCHAIN compile bring-up). C's #ifdef ladder
// (WAIT_USE_EPOLL / WAIT_USE_KQUEUE / WAIT_USE_POLL) has no wasm arm; the
// functional backend here — WASI poll_oneoff readiness + latch park — is the
// boot increment, not this one. Creating a wait-event set on wasm therefore
// reports a clean ERROR (no panics on ported paths); nothing can reach
// register/wait_block without a successfully created set.

use crate::{wes_error, Latch, PgResult, WaitEvent, WaitEventSetData};

pub(crate) struct BackendSet {}

impl BackendSet {
    pub(crate) fn create(_nevents: i32) -> PgResult<Self> {
        Err(wes_error(
            "WaitEventSet is not implemented on wasm32 (WASI poll_oneoff backend is the boot increment)",
        ))
    }

    pub(crate) fn free(&self) {}

    pub(crate) fn register(&self, _event: &WaitEvent, _old_events: u32) -> PgResult<()> {
        Err(wes_error(
            "WaitEventSet is not implemented on wasm32 (WASI poll_oneoff backend is the boot increment)",
        ))
    }
}

pub(crate) fn wait_block(
    _set: &mut WaitEventSetData,
    _latch: Option<&'static Latch>,
    _cur_timeout: i64,
    _occurred_events: &mut [WaitEvent],
) -> PgResult<i32> {
    Err(wes_error(
        "WaitEventSet is not implemented on wasm32 (WASI poll_oneoff backend is the boot increment)",
    ))
}
