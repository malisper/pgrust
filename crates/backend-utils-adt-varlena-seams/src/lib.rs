//! Seam declarations for the `backend-utils-adt-varlena` unit
//! (`utils/adt/varlena.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;

seam_core::seam!(
    /// `SplitIdentifierString(rawstring, ',', &namelist)` (varlena.c) for the
    /// comma separator: parse a comma-separated list of identifiers,
    /// downcasing and dequoting per identifier rules. `Ok(None)` is the C
    /// `false` return (syntax error); the returned strings are the
    /// truncated/downcased names. `Err` carries OOM from the copies.
    pub fn split_identifier_string(raw: &str) -> PgResult<Option<Vec<String>>>
);
