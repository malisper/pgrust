//! `backend-access-gin-core-probe` — the GIN access method's self-contained
//! support routines, ported from four PostgreSQL 18.3 files:
//!
//!   * [`ginarrayproc`] — `ginarrayproc.c`: the `anyarray_ops` GIN support
//!     procedures (`extractValue`/`extractQuery`/`consistent`/`triConsistent`).
//!   * [`ginlogic`] — `ginlogic.c`: the binary-/ternary-logic consistent-check
//!     routing (`direct*`/`shim*` helpers + `ginInitConsistentFunction`).
//!   * [`ginpostinglist`] — `ginpostinglist.c`: the posting-list varbyte codec
//!     and item-pointer merge.
//!   * [`ginvalidate`] — `ginvalidate.c`: the GIN opclass validator
//!     (`ginvalidate`/`ginadjustmembers`).
//!
//! Each module reproduces the C logic exactly. External subsystem calls go
//! through the relevant owner seam crates:
//!   * `get_typlenbyvalalign` / `deconstruct_array` (lsyscache / arrayfuncs) for
//!     the array extract functions;
//!   * `tbm_add_tuples` (tidbitmap) for the posting-list-to-bitmap decoder;
//!   * the AM-validator substrate (syscache / lsyscache / regproc / amvalidate /
//!     error) for the validator;
//!   * the two fmgr `consistent`/`triConsistent` call seams declared in
//!     `backend-access-gin-core-probe-seams` for the ternary-logic shims. Those
//!     fmgr-call seams are owned by the (not-yet-ported) fmgr GIN-call
//!     dispatcher and panic loudly until it installs them (mirror-pg-and-panic).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// Fallible cores return the shared `PgResult<_>` (== `Result<_, PgError>`);
// the un-boxed return type is the project-wide contract these ports match.
#![allow(clippy::result_large_err)]

pub mod ginarrayproc;
pub mod ginlogic;
pub mod ginpostinglist;
pub mod ginvalidate;

/// Install this crate's owned seams. This crate owns no inward seams — the two
/// `gin_consistent_call_*` declarations in `backend-access-gin-core-probe-seams`
/// are *outward* calls into the fmgr GIN-call dispatcher (their real owner),
/// which installs them when it lands. The hook keeps `seams-init` wiring
/// uniform.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
