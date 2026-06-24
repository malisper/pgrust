//! `src/common/link-canary.c` — a one-function unit that exists only to detect
//! a build misconfiguration where the *frontend* copy of a dual-compiled common
//! file is mistakenly linked into the backend (or vice versa).
//!
//! Bootstrap calls [`pg_link_canary_is_frontend`] once to force the unit to be
//! linked in; it returns `false` in the backend build.

#![no_std]
#![allow(non_snake_case)]

/// `pg_link_canary_is_frontend()` (link-canary.c) — `true` in a `FRONTEND`
/// build, `false` in the backend. This is the backend copy, so it returns
/// `false`.
pub fn pg_link_canary_is_frontend() -> bool {
    false
}

/// Install this unit's inward seam.
pub fn init_seams() {
    link_canary_seams::pg_link_canary_is_frontend::set(pg_link_canary_is_frontend);
}
