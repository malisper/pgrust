//! Port of two PostgreSQL `utils/adt` files (postgres-18.3):
//!
//! * [`pg_lsn`] — `src/backend/utils/adt/pg_lsn.c`, the operations for the
//!   `pg_lsn` datatype (an `XLogRecPtr`, a 64-bit unsigned integer rendered as
//!   `"%X/%X"` text).
//! * [`trigfuncs`] — `src/backend/utils/adt/trigfuncs.c`, the single builtin
//!   trigger-support function `suppress_redundant_updates_trigger`.
//!
//! Both files are node-tree-independent SQL-callable functions. The pure cores
//! take/return decoded scalars (`pg_lsn` = [`XLogRecPtr`](types_core::XLogRecPtr),
//! tuples = owned [`HeapTupleData`](types_tuple::heaptuple::HeapTupleData)),
//! matching the sibling idiomatic adt crates; no V1 fmgr dispatch glue is added
//! here (this tree's adt crates expose the cores, not a builtin registry).
//!
//! # Externals / seams
//!
//! * The `pg_lsn` arithmetic operators bridge into the ported numeric crate
//!   exactly as the C does via `DirectFunctionCall` into `numeric_in` /
//!   `numeric_add` / `numeric_sub` / `numeric_pg_lsn` — direct deps, no cycle.
//!   `numeric_pg_lsn` (`numeric.c`) is reproduced 1:1 in [`pg_lsn`] over the
//!   numeric crate's public API.
//! * `trigfuncs.c` reads its `TriggerData` context through the
//!   `trigger.c` owner's seam crate
//!   ([`trigger_seams`]): `called_as_trigger`, `tg_event`,
//!   `tg_trigtuple`, `tg_newtuple`. `trigger.c` is not yet ported, so those
//!   seams panic until it lands (mirror-PG-and-panic).
//!
//! This crate owns no inward seam crate (no cyclic caller needs it), so it
//! declares no seams and has no `init_seams()`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// Every fallible function returns the shared `types_error::PgResult`
// (== `Result<_, PgError>`); the project-wide error contract these ports match.
#![allow(clippy::result_large_err)]

pub mod fmgr_builtins;
pub mod pg_lsn;
pub mod trigfuncs;

/// Register this crate's fmgr builtins (the `pg_lsn.c` SQL-callable functions)
/// so by-OID dispatch resolves them. Called from `seams-init::init_all`.
pub fn init_seams() {
    fmgr_builtins::register_pg_lsn_builtins();
    fmgr_builtins::register_trigfuncs_builtins();
}
