//! Port of `src/backend/utils/adt/version.c` (PostgreSQL 18.3) — returns the
//! PostgreSQL version string.
//!
//! The single SQL-callable function is `pgsql_version` (catalog `version()`,
//! `pg_proc` OID 89, returns `text`). It is a pure leaf: it wraps the
//! compile-time `PG_VERSION_STR` into a `text` `Datum` via the varlena owner's
//! `cstring_to_text`. Nothing calls into this unit across a cycle, so it owns
//! no inward seam crate and `init_seams()` is empty.

#![no_std]

use mcx::Mcx;
use types_datum::Datum;
use types_error::PgResult;

use backend_utils_adt_varlena_seams::cstring_to_text;

/// `PG_VERSION_STR` (`pg_config.h`) — the full version banner emitted by
/// `version()`. `configure` defines it as
/// `"PostgreSQL " PG_VERSION " on " host ", compiled by " cc ", " bits "-bit"`.
/// This is the value `configure` produced for the porting target (matching the
/// c2rust rendering of this unit).
pub const PG_VERSION_STR: &str =
    "PostgreSQL 18.3 on aarch64-darwin, compiled by clang-21.0.0, 64-bit";

/// `pgsql_version(PG_FUNCTION_ARGS)` (version.c) —
/// `PG_RETURN_TEXT_P(cstring_to_text(PG_VERSION_STR))`.
///
/// `mcx` is the caller's current memory context (the C `palloc` target inside
/// `cstring_to_text`). OOM `ereport(ERROR)` is carried on `Err`.
pub fn pgsql_version<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Datum> {
    cstring_to_text::call(mcx, PG_VERSION_STR)
}

/// Install this unit's inward seams. This unit owns none (no cyclic inward
/// consumer); kept for the uniform `seams-init` wiring contract and the
/// `recurrence_guard` check.
pub fn init_seams() {}
