//! Type vocabulary for `access/common/tupdesc.c`. Lives here (not in the owning
//! crate) so the `lookup_type` seam declaration can reference [`PgTypeInfo`]
//! without depending on the owning crate; `backend-access-common-tupdesc`
//! re-exports it.

use types_core::primitive::Oid;

/// The slice of a `pg_type` row that `TupleDescInitEntry` reads out of the
/// type-cache tuple (`Form_pg_type`): the type-dependent attribute fields it
/// stamps onto a `Form_pg_attribute` (`typlen`/`typbyval`/`typalign`/
/// `typstorage`/`typcollation`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PgTypeInfo {
    /// `Form_pg_type.typlen` -> `att->attlen`.
    pub typlen: i16,
    /// `Form_pg_type.typbyval` -> `att->attbyval`.
    pub typbyval: bool,
    /// `Form_pg_type.typalign` -> `att->attalign`.
    pub typalign: i8,
    /// `Form_pg_type.typstorage` -> `att->attstorage`.
    pub typstorage: i8,
    /// `Form_pg_type.typcollation` -> `att->attcollation`.
    pub typcollation: Oid,
}
