//! Seam declarations for the `backend-utils-adt-format-type` unit
//! (`utils/adt/format_type.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `format_type_be(type_oid)` (format_type.c): the type's printable name
    /// for backend error messages. `Err` carries the invalid-type
    /// cache-lookup `elog(ERROR)`.
    ///
    /// Owned-`String` sanction: this seam is error-message-only — every
    /// consumer feeds the result straight into a `PgError` message, which is
    /// already global-allocator `String` territory. If a non-diagnostic
    /// consumer appears, re-sign as
    /// `fn format_type_be<'mcx>(mcx, Oid) -> PgResult<PgString<'mcx>>` per
    /// AGENTS.md "Allocating seams take Mcx".
    pub fn format_type_be(type_oid: Oid) -> PgResult<String>
);
