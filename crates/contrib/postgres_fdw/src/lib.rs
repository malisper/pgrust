//! `contrib/postgres_fdw` phase 1: option validation, shippability,
//! deparser, and the planner arms. The connection layer (connection.c and
//! the executor data flow) is phase 2, pending the shared pgclient seam;
//! executing a foreign scan raises a clean named error until then.
#![allow(non_snake_case)]

pub mod option;
pub mod shippable;

use types_error::ErrorLocation;

pub(crate) const LIBRARY: &str = "postgres_fdw";

pub(crate) fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("postgres_fdw.c", 0, funcname)
}

pub fn init_seams() {}
