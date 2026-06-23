//! `pg_operator` catalog row layout and constants (`catalog/pg_operator.h`,
//! PostgreSQL 18.3) — the full `FormData_pg_operator` field set (15 attributes)
//! plus the relation/index OIDs the `pg_operator.c` catalog owner needs.

use ::types_core::primitive::Oid;

/// `OperatorRelationId` — `pg_operator` (OID 2617).
pub const OperatorRelationId: Oid = 2617;
/// `OperatorOidIndexId` — `pg_operator_oid_index` (OID 2688).
pub const OperatorOidIndexId: Oid = 2688;
/// `OperatorNameNspIndexId` — `pg_operator_oprname_l_r_n_index` (OID 2689),
/// the unique `(oprname, oprleft, oprright, oprnamespace)` index.
pub const OperatorNameNspIndexId: Oid = 2689;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_operator).
 * ======================================================================== */

pub const Anum_pg_operator_oid: i16 = 1;
pub const Anum_pg_operator_oprname: i16 = 2;
pub const Anum_pg_operator_oprnamespace: i16 = 3;
pub const Anum_pg_operator_oprowner: i16 = 4;
pub const Anum_pg_operator_oprkind: i16 = 5;
pub const Anum_pg_operator_oprcanmerge: i16 = 6;
pub const Anum_pg_operator_oprcanhash: i16 = 7;
pub const Anum_pg_operator_oprleft: i16 = 8;
pub const Anum_pg_operator_oprright: i16 = 9;
pub const Anum_pg_operator_oprresult: i16 = 10;
pub const Anum_pg_operator_oprcom: i16 = 11;
pub const Anum_pg_operator_oprnegate: i16 = 12;
pub const Anum_pg_operator_oprcode: i16 = 13;
pub const Anum_pg_operator_oprrest: i16 = 14;
pub const Anum_pg_operator_oprjoin: i16 = 15;

/// `Natts_pg_operator` — number of attributes in `pg_operator`.
pub const Natts_pg_operator: i32 = 15;

/// `FormData_pg_operator` (`catalog/pg_operator.h`) — the full owned view of an
/// operator's `(Form_pg_operator) GETSTRUCT(tup)` row, in genbki field order.
/// The catalog owner (`pg_operator.c`) materializes this from the heap tuple via
/// the `fetch_operator_form` seam, and uses it as the `makeOperatorDependencies`
/// argument (the idiomatic equivalent of the `HeapTuple`/`GETSTRUCT` pair).
#[derive(Clone, Debug)]
pub struct FormPgOperator {
    /// `oid` — the operator's own OID.
    pub oid: Oid,
    /// `oprname` — operator name (`NameData`).
    pub oprname: String,
    /// `oprnamespace` — namespace containing this operator.
    pub oprnamespace: Oid,
    /// `oprowner` — operator owner.
    pub oprowner: Oid,
    /// `oprkind` — `'l'` for prefix or `'b'` for infix.
    pub oprkind: i8,
    /// `oprcanmerge` — operator supports merge joins.
    pub oprcanmerge: bool,
    /// `oprcanhash` — operator supports hash joins.
    pub oprcanhash: bool,
    /// `oprleft` — left argument type, or `InvalidOid` (prefix operator).
    pub oprleft: Oid,
    /// `oprright` — right argument type.
    pub oprright: Oid,
    /// `oprresult` — result type; can be `InvalidOid` in a shell.
    pub oprresult: Oid,
    /// `oprcom` — commutator operator, or `InvalidOid`.
    pub oprcom: Oid,
    /// `oprnegate` — negator operator, or `InvalidOid`.
    pub oprnegate: Oid,
    /// `oprcode` — underlying function; can be `InvalidOid` in a shell.
    pub oprcode: Oid,
    /// `oprrest` — restriction-selectivity estimator, or `InvalidOid`.
    pub oprrest: Oid,
    /// `oprjoin` — join-selectivity estimator, or `InvalidOid`.
    pub oprjoin: Oid,
}
