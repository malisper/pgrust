//! pqcomm.c's GUC-backed globals, owned here per the elog precedent: each
//! holds PostgreSQL's boot default and exposes a `pub` setter for the GUC
//! unit to call when it lands. Per AGENTS.md "Backend-global state" these are
//! per-backend values, so they live in `thread_local!`.
//!
//! `Unix_socket_permissions` / `Unix_socket_group` are defined in pqcomm.c
//! itself; the `tcp_keepalives_*` / `tcp_user_timeout` ints are defined in
//! guc_tables.c but consumed only by pqcomm.c (whose assign/show hooks this
//! crate also owns), so their backing store lives here too.

use std::cell::{Cell, RefCell};

thread_local! { static UNIX_SOCKET_PERMISSIONS: Cell<i32> = const { Cell::new(0o777) }; }
thread_local! { static UNIX_SOCKET_GROUP: RefCell<String> = const { RefCell::new(String::new()) }; }
thread_local! { static TCP_KEEPALIVES_IDLE: Cell<i32> = const { Cell::new(0) }; }
thread_local! { static TCP_KEEPALIVES_INTERVAL: Cell<i32> = const { Cell::new(0) }; }
thread_local! { static TCP_KEEPALIVES_COUNT: Cell<i32> = const { Cell::new(0) }; }
thread_local! { static TCP_USER_TIMEOUT: Cell<i32> = const { Cell::new(0) }; }

pub fn unix_socket_permissions() -> i32 {
    UNIX_SOCKET_PERMISSIONS.with(Cell::get)
}

pub fn set_unix_socket_permissions(v: i32) {
    UNIX_SOCKET_PERMISSIONS.with(|c| c.set(v));
}

pub fn unix_socket_group() -> String {
    UNIX_SOCKET_GROUP.with(|c| c.borrow().clone())
}

pub fn set_unix_socket_group(v: &str) {
    UNIX_SOCKET_GROUP.with(|c| *c.borrow_mut() = v.to_owned());
}

pub fn tcp_keepalives_idle() -> i32 {
    TCP_KEEPALIVES_IDLE.with(Cell::get)
}

pub fn set_tcp_keepalives_idle(v: i32) {
    TCP_KEEPALIVES_IDLE.with(|c| c.set(v));
}

pub fn tcp_keepalives_interval() -> i32 {
    TCP_KEEPALIVES_INTERVAL.with(Cell::get)
}

pub fn set_tcp_keepalives_interval(v: i32) {
    TCP_KEEPALIVES_INTERVAL.with(|c| c.set(v));
}

pub fn tcp_keepalives_count() -> i32 {
    TCP_KEEPALIVES_COUNT.with(Cell::get)
}

pub fn set_tcp_keepalives_count(v: i32) {
    TCP_KEEPALIVES_COUNT.with(|c| c.set(v));
}

pub fn tcp_user_timeout() -> i32 {
    TCP_USER_TIMEOUT.with(Cell::get)
}

pub fn set_tcp_user_timeout(v: i32) {
    TCP_USER_TIMEOUT.with(|c| c.set(v));
}
