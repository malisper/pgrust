//! Parse-tree vocabulary (nodes/parsenodes.h), trimmed.

use mcx::PgBox;
use types_core::primitive::{Index, Oid};
use types_tuple::access::LOCKMODE;

use crate::bitmapset::Bitmapset;

/// `RTEKind` (nodes/parsenodes.h).
pub type RTEKind = u32;

pub const RTE_RELATION: RTEKind = 0;
pub const RTE_SUBQUERY: RTEKind = 1;
pub const RTE_JOIN: RTEKind = 2;
pub const RTE_FUNCTION: RTEKind = 3;
pub const RTE_TABLEFUNC: RTEKind = 4;
pub const RTE_VALUES: RTEKind = 5;
pub const RTE_CTE: RTEKind = 6;
pub const RTE_NAMEDTUPLESTORE: RTEKind = 7;
pub const RTE_RESULT: RTEKind = 8;
pub const RTE_GROUP: RTEKind = 9;

/// `RangeTblEntry` (nodes/parsenodes.h), trimmed to the fields ports consume.
#[derive(Clone, Copy, Debug, Default)]
pub struct RangeTblEntry {
    /// `RTEKind rtekind`.
    pub rtekind: RTEKind,
    /// `Oid relid` — OID of the relation (RTE_RELATION).
    pub relid: Oid,
    /// `char relkind` — relation kind.
    pub relkind: i8,
    /// `int rellockmode` — lock level that the query requires.
    pub rellockmode: LOCKMODE,
    /// `Index perminfoindex` — 1-based index of this RTE's
    /// `RTEPermissionInfo` in the query's `rteperminfos` list, or 0.
    pub perminfoindex: Index,
}

/// `RTEPermissionInfo` (nodes/parsenodes.h), trimmed.
#[derive(Debug, Default)]
pub struct RTEPermissionInfo<'mcx> {
    /// `Oid relid` — relation the permissions apply to.
    pub relid: Oid,
    /// `Oid checkAsUser` — user to check access as, or 0 for current user.
    pub checkAsUser: Oid,
    /// `Bitmapset *insertedCols` — columns needing INSERT permission.
    pub insertedCols: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `Bitmapset *updatedCols` — columns needing UPDATE permission.
    pub updatedCols: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
}
