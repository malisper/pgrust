//! Port of `src/backend/utils/adt/version.c` (PostgreSQL 18.3) — returns the
//! PostgreSQL version string.
//!
//! The single SQL-callable function is `pgsql_version` (catalog `version()`,
//! `pg_proc` OID 89, returns `text`). It is a pure leaf: it wraps the
//! compile-time `PG_VERSION_STR` into a `text` `Datum` via the varlena owner's
//! `cstring_to_text`. Nothing calls into this unit across a cycle, so it owns
//! no inward seam crate and `init_seams()` is empty.
//!
//! Datum-unification status: this unit has **no own-logic shim use**. Its only
//! contact with the bare-word `datum::Datum` is the return type it
//! forwards verbatim from `varlena_seams::cstring_to_text`
//! (owned by the `backend-utils-adt-varlena` lane, migrated in this same batch).
//! A `text` result is by-reference, so its canonical form is
//! `types_tuple::Datum::ByRef`; once the `cstring_to_text` seam flips to
//! the canonical value enum, `pgsql_version`'s return type follows it with no
//! other change. We therefore name the seam's return type inline rather than
//! importing the shim, so this crate carries no standalone `use datum`.

// NB: not `#![no_std]` — the fmgr builtin boundary (`fmgr_builtins`) uses
// `std` (the by-ref `String`/`Vec<u8>` result lane and `panic_any` for
// `ereport`).

use mcx::Mcx;
use types_error::PgResult;

use varlena_seams::{cstring_to_text, cstring_to_text_v};

pub mod fmgr_builtins;

/// `PG_VERSION_STR` (`pg_config.h`) — the full version banner emitted by
/// `version()`. `configure` defines it as
/// `"PostgreSQL " PG_VERSION " on " host ", compiled by " cc ", " bits "-bit"`.
/// This is the value `configure` produced for the porting target (matching the
/// c2rust rendering of this unit).
// DELIBERATE DIVERGENCE (user-approved): the version() banner is branded
// "pgrust" instead of the faithful "PostgreSQL". This is an intentional cosmetic
// divergence from upstream — no regress test diffs the version() banner (the 3
// callers only regex-match platform substrings like 'linux-gnu'), and
// server_version / server_version_num are unchanged.
pub const PG_VERSION_STR: &str =
    "pgrust 18.3 on aarch64-darwin, compiled by clang-21.0.0, 64-bit";

/// `pgsql_version(PG_FUNCTION_ARGS)` (version.c) —
/// `PG_RETURN_TEXT_P(cstring_to_text(PG_VERSION_STR))`.
///
/// `mcx` is the caller's current memory context (the C `palloc` target inside
/// `cstring_to_text`). OOM `ereport(ERROR)` is carried on `Err`.
pub fn pgsql_version<'mcx>(mcx: Mcx<'mcx>) -> PgResult<datum::Datum> {
    cstring_to_text::call(mcx, PG_VERSION_STR)
}

/// `pgsql_version(PG_FUNCTION_ARGS)` over the unified value type — the
/// migration-target form of [`pgsql_version`]. The `text` result is the
/// `Datum::ByRef` varlena built by `cstring_to_text_v`. Used by the fmgr
/// builtin adapter, which needs the flat varlena bytes for the by-reference
/// result lane.
pub fn pgsql_version_v<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<types_tuple::heaptuple::Datum<'mcx>> {
    cstring_to_text_v::call(mcx, PG_VERSION_STR)
}

/// Install this unit's inward seams and register its SQL-callable builtins.
/// This unit owns no inward seam (no cyclic inward consumer), but it does
/// register `version()` (OID 89) into the fmgr-core builtin table here so
/// by-OID dispatch resolves it. Called by `seams-init::init_all`.
pub fn init_seams() {
    fmgr_builtins::register_version_builtins();
}
