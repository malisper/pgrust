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

pub mod dispatch;
pub mod extdispatch;
pub mod fmgr_builtins;
pub mod ginarrayproc;
pub mod ginlogic;
pub mod ginpostinglist;
pub mod ginvalidate;

/// Install this crate's seams. This crate is the single installer of the GIN
/// `anyarray_ops` opclass support-procedure dispatch: the `gin_extract_value` /
/// `gin_extract_query` seams (declared in `backend-access-gin-ginutil-seams`)
/// and the `gin_consistent_call_{bool,tri}` seams (declared in
/// `backend-access-gin-core-probe-seams`) are routed by support-proc OID to the
/// ported [`ginarrayproc`] bodies (the owned analog of the opclass fmgr
/// dispatch — the `internal`-typed out-parameters cannot cross the by-word fmgr
/// `Datum` lane). It also registers the `fmgr_builtins[]` rows for those procs
/// so `index_getprocinfo` → `fmgr_info` resolves their `internal`-language
/// prosrc names (without which `CREATE INDEX ... USING gin` errors `internal
/// function "ginarrayextract" is not in internal lookup table`).
pub fn init_seams() {
    dispatch::install();
    fmgr_builtins::register_gin_array_proc_builtins();
}

#[cfg(test)]
mod tests;
