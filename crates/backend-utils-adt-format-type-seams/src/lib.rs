//! Seam declarations for the `backend-utils-adt-format-type` unit
//! (`utils/adt/format_type.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `format_type_be(type_oid)` (format_type.c): the type's printable name
    /// for backend error messages, palloc'd in the caller's current context
    /// (`mcx`). `Err` carries the invalid-type cache-lookup `elog(ERROR)`
    /// and OOM.
    pub fn format_type_be<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        type_oid: Oid,
    ) -> PgResult<mcx::PgString<'mcx>>
);

seam_core::seam!(
    /// `format_type_be(type_oid)` (format_type.c) as consumed by callers that
    /// only need the printable name to interpolate into an owned
    /// `errmsg(...)` string (no `Mcx` in scope) — the funcapi polymorphic
    /// resolvers' `"... but type %s"` messages. Returns an owned `String`
    /// (the C result is a transient palloc'd cstring the caller copies into
    /// the error text). `Err` carries the invalid-type cache-lookup
    /// `elog(ERROR)`.
    pub fn format_type_be_owned(type_oid: Oid) -> PgResult<String>
);
