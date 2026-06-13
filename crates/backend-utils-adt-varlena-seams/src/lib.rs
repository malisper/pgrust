//! Seam declarations for the `backend-utils-adt-varlena` unit
//! (`utils/adt/varlena.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

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
    /// `bool SplitIdentifierString(char *rawstring, char separator,
    /// List **namelist)` (varlena.c) — parse a comma-separated identifier
    /// list, downcasing/de-quoting each. Returns the parsed names, or `None`
    /// when the list syntax is invalid (the C `return false`).
    pub fn split_identifier_string(rawstring: &str, separator: char) -> Option<Vec<String>>
);
