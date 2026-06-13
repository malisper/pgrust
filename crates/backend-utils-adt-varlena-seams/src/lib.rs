//! Seam declarations for the `backend-utils-adt-varlena` unit
//! (`utils/adt/varlena.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use mcx::{Mcx, PgString, PgVec};
use types_error::PgResult;

seam_core::seam!(
    /// `cstring_to_text(s)` (varlena.c) — build a `text` varlena from a
    /// string, allocated in `mcx` (C: palloc in the caller's current
    /// context), returned as the `Datum` callers pass on (the
    /// `CStringGetTextDatum(s)` macro of builtins.h). OOM `ereport(ERROR)`
    /// carried on `Err`.
    pub fn cstring_to_text<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        s: &str,
    ) -> types_error::PgResult<types_datum::Datum>
);

seam_core::seam!(
    /// `SplitIdentifierString(rawstring, ',', &namelist)` (varlena.c) for the
    /// comma separator: parse a comma-separated list of identifiers,
    /// downcasing and dequoting per identifier rules. `Ok(None)` is the C
    /// `false` return (syntax error); the returned strings are the
    /// truncated/downcased names, allocated in `mcx` (C: pstrdup + List in
    /// the current context). `Err` carries OOM from the copies.
    pub fn split_identifier_string<'mcx>(
        mcx: Mcx<'mcx>,
        raw: &str,
    ) -> PgResult<Option<PgVec<'mcx, PgString<'mcx>>>>
);
