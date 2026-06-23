//! Seam-boundary value types for `backend/utils/adt/format_type.c`.
//!
//! `format_type_extended` reads several columns out of the `pg_type` syscache
//! tuple (`SearchSysCache1(TYPEOID)` + `GETSTRUCT`) and keeps the whole
//! formatting decision in-crate. [`TypeFormInfo`] is the projection of the
//! `Form_pg_type` fields it consumes, copied out of the catcache into the
//! caller's `Mcx` (the C reads are in-place on the cache tuple; the owned
//! model copies and drop is the release).

#![no_std]

use ::mcx::PgString;
use ::types_core::Oid;

/// The decomposed view of one `pg_type` row that `format_type_extended`
/// reads via `GETSTRUCT((Form_pg_type) tuple)`.
#[derive(Debug)]
pub struct TypeFormInfo<'mcx> {
    /// `typelem` — element type for a (true) array type, else `InvalidOid`.
    pub typelem: Oid,
    /// `typsubscript` (`regproc`) — the subscripting handler proc; an array
    /// type has `F_ARRAY_SUBSCRIPT_HANDLER`.
    pub typsubscript: Oid,
    /// `typstorage` (`char`) — TOAST storage strategy; `TYPSTORAGE_PLAIN`
    /// array types are not deconstructed.
    pub typstorage: i8,
    /// `typmodout` (`regproc`) — the type-specific typmod-output proc, or
    /// `InvalidOid` for the default integer-in-parens decoration.
    pub typmodout: Oid,
    /// `typnamespace` — the type's schema, used for qualification.
    pub typnamespace: Oid,
    /// `typname` (`NameData`) — the catalog type name (NUL-trimmed).
    pub typname: PgString<'mcx>,
}
