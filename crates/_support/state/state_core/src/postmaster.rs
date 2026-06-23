//! `postmaster/postmaster.c` (unit: backend-postmaster-postmaster) —
//! stderr-redirection state.

use core::cell::Cell;

thread_local! {
    /// `redirection_done` (postmaster.c): stderr redirected into the
    /// syslogger pipe.
    static REDIRECTION_DONE: Cell<bool> = const { Cell::new(false) };
}

pub fn redirection_done() -> bool {
    REDIRECTION_DONE.with(Cell::get)
}
pub fn set_redirection_done(value: bool) {
    REDIRECTION_DONE.with(|c| c.set(value));
}
