//! `utils/init/miscinit.c` (unit: backend-utils-init-miscinit) —
//! backend-identity state.

use core::cell::Cell;

thread_local! {
    /// Mirrors `MyBackendType == B_LOGGER` (miscinit.c); widens to the full
    /// `BackendType` enum when miscinit ports (types.md rule 7 applies then).
    static AM_SYSLOGGER: Cell<bool> = const { Cell::new(false) };
}

pub fn am_syslogger() -> bool {
    AM_SYSLOGGER.with(Cell::get)
}
pub fn set_am_syslogger(value: bool) {
    AM_SYSLOGGER.with(|c| c.set(value));
}
