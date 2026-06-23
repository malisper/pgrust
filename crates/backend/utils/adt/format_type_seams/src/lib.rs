//! Seam declarations for the `backend-utils-adt-format-type` unit
//! (`utils/adt/format_type.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use ::types_core::Oid;
use ::types_error::PgResult;

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
    /// `format_type_extended(type_oid, typemod, flags)` (format_type.c): the
    /// type's printable name with the `FORMAT_TYPE_*` flag bits applied.
    /// `Ok(None)` is the C `NULL` (only with `FORMAT_TYPE_INVALID_AS_NULL` when
    /// the type is gone). palloc'd in `mcx`; `Err` carries the invalid-type
    /// cache-lookup `elog(ERROR)` and OOM.
    pub fn format_type_extended<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        type_oid: Oid,
        typemod: i32,
        flags: u16,
    ) -> PgResult<Option<mcx::PgString<'mcx>>>
);

seam_core::seam!(
    /// `format_type_be_qualified(type_oid)` (format_type.c): like
    /// [`format_type_be`] but always schema-qualifies the type name
    /// (`FORMAT_TYPE_FORCE_QUALIFY`), palloc'd in `mcx`. `Err` carries the
    /// invalid-type cache-lookup `elog(ERROR)` and OOM.
    pub fn format_type_be_qualified<'mcx>(
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

seam_core::seam!(
    /// `format_type_be(type_oid)` (format_type.c) for transient error-message
    /// use by callers that thread no `Mcx` (e.g. lsyscache.c's `get_cast_oid`,
    /// whose C signature takes no context and renders the type names only into
    /// the `ereport(ERROR)` text). The owner formats in a scratch context and
    /// returns an owned `String`. `Err` carries the invalid-type cache-lookup
    /// `elog(ERROR)` and OOM.
    pub fn format_type_be_str(type_oid: Oid) -> PgResult<String>
);

seam_core::seam!(
    /// `type_maximum_size(type_oid, typemod)` (format_type.c): the maximum
    /// on-disk width of a variable-width type at the given typmod, or `-1` when
    /// not determinable. Used by lsyscache.c's `get_typavgwidth`. `Err` carries
    /// the type-lookup `ereport(ERROR)` surface.
    pub fn type_maximum_size(type_oid: Oid, typemod: i32) -> PgResult<i32>
);

/// `utils/builtins.h`: `FORMAT_TYPE_TYPEMOD_GIVEN`.
pub const FORMAT_TYPE_TYPEMOD_GIVEN: u16 = 0x01;
/// `utils/builtins.h`: `FORMAT_TYPE_ALLOW_INVALID`.
pub const FORMAT_TYPE_ALLOW_INVALID: u16 = 0x02;
/// `utils/builtins.h`: `FORMAT_TYPE_FORCE_QUALIFY`.
pub const FORMAT_TYPE_FORCE_QUALIFY: u16 = 0x04;
/// `utils/builtins.h`: `FORMAT_TYPE_INVALID_AS_NULL`.
pub const FORMAT_TYPE_INVALID_AS_NULL: u16 = 0x08;
