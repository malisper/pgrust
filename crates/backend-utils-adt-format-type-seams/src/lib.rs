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
    /// `format_type_be(type_oid)` (format_type.c) for transient error-message
    /// use by callers that thread no `Mcx` (e.g. lsyscache.c's `get_cast_oid`,
    /// whose C signature takes no context and renders the type names only into
    /// the `ereport(ERROR)` text). The owner formats in a scratch context and
    /// returns an owned `String`. `Err` carries the invalid-type cache-lookup
    /// `elog(ERROR)` and OOM.
    pub fn format_type_be_str(type_oid: Oid) -> PgResult<String>
);
