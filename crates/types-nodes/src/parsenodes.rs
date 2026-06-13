//! Parse-tree vocabulary (nodes/parsenodes.h), trimmed.

use mcx::PgBox;
use types_core::primitive::{Index, Oid};
use types_tuple::access::LOCKMODE;

use crate::bitmapset::Bitmapset;

/// `RTEKind` (nodes/parsenodes.h) — values verified against PostgreSQL 18.3.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum RTEKind {
    /// ordinary relation reference
    #[default]
    RTE_RELATION = 0,
    /// subquery in FROM
    RTE_SUBQUERY = 1,
    /// join
    RTE_JOIN = 2,
    /// function in FROM
    RTE_FUNCTION = 3,
    /// TableFunc(.., column list)
    RTE_TABLEFUNC = 4,
    /// VALUES (<exprlist>), (<exprlist>), ...
    RTE_VALUES = 5,
    /// common table expr (WITH list element)
    RTE_CTE = 6,
    /// tuplestore, e.g. for AFTER triggers
    RTE_NAMEDTUPLESTORE = 7,
    /// RTE represents an empty FROM clause (added by the planner)
    RTE_RESULT = 8,
    /// the grouping step
    RTE_GROUP = 9,
}

pub use RTEKind::{
    RTE_CTE, RTE_FUNCTION, RTE_GROUP, RTE_JOIN, RTE_NAMEDTUPLESTORE, RTE_RELATION, RTE_RESULT,
    RTE_SUBQUERY, RTE_TABLEFUNC, RTE_VALUES,
};

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
