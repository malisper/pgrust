//! `contrib/postgres_fdw`: option validation, shippability, deparser, the
//! planner arms (phase 1), and — phase 2 — the connection layer
//! (connection.c over `crates/interfaces/pgclient`) plus the read-only
//! executor data flow (cursor-batched foreign scans). Joins/upper pushdown,
//! DML, async execution and remote estimates are phase 3 and raise clean
//! named errors.
#![allow(non_snake_case)]

pub mod connection;
pub mod deparse;
pub mod exec;
pub mod option;
pub mod plan;
pub mod relinfo;
pub mod shippable;
pub mod transmission;

use types_error::ErrorLocation;

pub(crate) const LIBRARY: &str = "postgres_fdw";

#[track_caller]
pub(crate) fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

pub fn init_seams() {
    plan::install();
}
