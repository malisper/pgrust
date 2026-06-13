//! The process-global GUCs and `conn_timing` owned by `backend_startup.c`
//! (lines 45-58). These are per-*backend* state (every backend a separate
//! copy diverging via SET / session state), so they are `thread_local!`, never
//! shared statics (AGENTS.md "Backend-global state").

use std::cell::Cell;

use types_core::TimestampTz;

/// `bool Trace_connection_negotiation = false;` (backend_startup.c:46) — the
/// `trace_connection_negotiation` developer GUC.
pub mod trace_connection_negotiation {
    use super::Cell;

    thread_local! {
        static VALUE: Cell<bool> = const { Cell::new(false) };
    }

    pub fn get() -> bool {
        VALUE.with(Cell::get)
    }

    pub fn set(value: bool) {
        VALUE.with(|c| c.set(value));
    }
}

/// `uint32 log_connections = 0;` (backend_startup.c:47) — the resolved
/// `log_connections` aspect-flag mask (the assign hook stores the flags here).
pub mod log_connections {
    use super::Cell;

    thread_local! {
        static VALUE: Cell<u32> = const { Cell::new(0) };
    }

    pub fn get() -> u32 {
        VALUE.with(Cell::get)
    }

    pub fn set(value: u32) {
        VALUE.with(|c| c.set(value));
    }
}

/// `ConnectionTiming conn_timing = {.ready_for_use = TIMESTAMP_MINUS_INFINITY};`
/// (backend_startup.c:58). `ready_for_use` starts at the
/// `TIMESTAMP_MINUS_INFINITY` sentinel so `PostgresMain` can tell whether it
/// has already been set; every other field starts at zero.
pub mod conn_timing {
    use super::{Cell, TimestampTz};

    /// `TIMESTAMP_MINUS_INFINITY` (`datatype/timestamp.h`) == `INT64_MIN + 1`.
    const TIMESTAMP_MINUS_INFINITY: TimestampTz = i64::MIN + 1;

    thread_local! {
        static SOCKET_CREATE: Cell<TimestampTz> = const { Cell::new(0) };
        static READY_FOR_USE: Cell<TimestampTz> = const { Cell::new(TIMESTAMP_MINUS_INFINITY) };
        static FORK_START: Cell<TimestampTz> = const { Cell::new(0) };
        static FORK_END: Cell<TimestampTz> = const { Cell::new(0) };
        static AUTH_START: Cell<TimestampTz> = const { Cell::new(0) };
        static AUTH_END: Cell<TimestampTz> = const { Cell::new(0) };
    }

    pub fn socket_create() -> TimestampTz {
        SOCKET_CREATE.with(Cell::get)
    }
    pub fn ready_for_use() -> TimestampTz {
        READY_FOR_USE.with(Cell::get)
    }
    pub fn fork_start() -> TimestampTz {
        FORK_START.with(Cell::get)
    }
    pub fn fork_end() -> TimestampTz {
        FORK_END.with(Cell::get)
    }
    pub fn auth_start() -> TimestampTz {
        AUTH_START.with(Cell::get)
    }
    pub fn auth_end() -> TimestampTz {
        AUTH_END.with(Cell::get)
    }

    pub fn set_socket_create(v: TimestampTz) {
        SOCKET_CREATE.with(|c| c.set(v));
    }
    pub fn set_ready_for_use(v: TimestampTz) {
        READY_FOR_USE.with(|c| c.set(v));
    }
    pub fn set_fork_start(v: TimestampTz) {
        FORK_START.with(|c| c.set(v));
    }
    pub fn set_fork_end(v: TimestampTz) {
        FORK_END.with(|c| c.set(v));
    }
    pub fn set_auth_start(v: TimestampTz) {
        AUTH_START.with(|c| c.set(v));
    }
    pub fn set_auth_end(v: TimestampTz) {
        AUTH_END.with(|c| c.set(v));
    }
}
