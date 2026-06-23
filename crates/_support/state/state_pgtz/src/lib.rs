//! Per-backend STATE STUB for the pgtz unit (`timezone/pgtz.c`): its GUC
//! timezone globals. Storage + accessors only (see `state-core`'s crate doc
//! for the state-stack rules); one layer above `state-core` because the
//! values carry `types-pgtime` types.
//!
//! `Rc<pg_tz>` deliberately: the struct carries parsed tzdata, and C shares
//! a `pg_tz *` — an `Rc` clone per read is the pointer-copy analog, where a
//! struct clone per read would copy the whole zone state.

use std::cell::RefCell;
use std::rc::Rc;

use pgtime::pg_tz;
use pgtime::pgtime::state;

thread_local! {
    /// `pg_tz *log_timezone` (pgtz.c). guc.c guarantees at least GMT before
    /// anything can log, hence the GMT boot value.
    static LOG_TIMEZONE: RefCell<Rc<pg_tz>> =
        RefCell::new(Rc::new(pg_tz::new(String::from("GMT"), state::default())));
    /// `pg_tz *session_timezone` (pgtz.c), same boot rule (`GMT` until the
    /// TimeZone GUC is assigned).
    static SESSION_TIMEZONE: RefCell<Rc<pg_tz>> =
        RefCell::new(Rc::new(pg_tz::new(String::from("GMT"), state::default())));
}

pub fn log_timezone() -> Rc<pg_tz> {
    LOG_TIMEZONE.with(|c| c.borrow().clone())
}
pub fn set_log_timezone(tz: Rc<pg_tz>) {
    LOG_TIMEZONE.with(|c| *c.borrow_mut() = tz);
}
pub fn session_timezone() -> Rc<pg_tz> {
    SESSION_TIMEZONE.with(|c| c.borrow().clone())
}
pub fn set_session_timezone(tz: Rc<pg_tz>) {
    SESSION_TIMEZONE.with(|c| *c.borrow_mut() = tz);
}
