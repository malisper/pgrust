use std::cell::Cell;

use types_startup::ConnectionTiming;

thread_local! {
    static CONN_TIMING: Cell<ConnectionTiming> = const {
        assert!(!core::mem::needs_drop::<ConnectionTiming>());
        Cell::new(ConnectionTiming::INIT)
    };
    static LOG_CONNECTIONS: Cell<u32> = const { Cell::new(0) };
    static TRACE_CONNECTION_NEGOTIATION: Cell<bool> = const { Cell::new(false) };
    // GUC string slot: set-rarely, leaked like init_small's DataDir.
    static LOG_CONNECTIONS_STRING: Cell<Option<&'static str>> = const { Cell::new(None) };
}

pub mod conn_timing {
    use super::CONN_TIMING;
    use types_core::TimestampTz;
    use types_startup::ConnectionTiming;

    pub fn get() -> ConnectionTiming {
        CONN_TIMING.get()
    }

    macro_rules! setter {
        ($($name:ident, $field:ident;)+) => {
            $(pub fn $name(v: TimestampTz) {
                let mut t = CONN_TIMING.get();
                t.$field = v;
                CONN_TIMING.set(t);
            })+
        };
    }

    setter! {
        set_socket_create, socket_create;
        set_fork_start, fork_start;
        set_fork_end, fork_end;
        set_auth_start, auth_start;
        set_auth_end, auth_end;
        set_ready_for_use, ready_for_use;
    }
}

pub mod log_connections {
    use super::LOG_CONNECTIONS;

    pub fn get() -> u32 {
        LOG_CONNECTIONS.get()
    }

    pub fn set(flags: u32) {
        LOG_CONNECTIONS.set(flags);
    }
}

// bool LoadedSSL: set by the postmaster (secure_initialize), read by every
// backend thread.
pub mod loaded_ssl {
    use std::sync::atomic::{AtomicBool, Ordering};

    static LOADED_SSL: AtomicBool = AtomicBool::new(false);

    pub fn get() -> bool {
        LOADED_SSL.load(Ordering::Relaxed)
    }

    pub fn set(v: bool) {
        LOADED_SSL.store(v, Ordering::Relaxed);
    }
}

pub mod trace_connection_negotiation {
    use super::TRACE_CONNECTION_NEGOTIATION;

    pub fn get() -> bool {
        TRACE_CONNECTION_NEGOTIATION.get()
    }

    pub fn set(v: bool) {
        TRACE_CONNECTION_NEGOTIATION.set(v);
    }
}

pub mod log_connections_string {
    use super::LOG_CONNECTIONS_STRING;

    pub fn get() -> Option<String> {
        LOG_CONNECTIONS_STRING.get().map(str::to_string)
    }

    pub fn set(v: Option<String>) {
        LOG_CONNECTIONS_STRING.set(v.map(|s| &*s.leak()));
    }
}
