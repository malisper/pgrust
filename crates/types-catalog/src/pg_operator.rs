//! `pg_operator` catalog row layout and constants (`catalog/pg_operator.h`,
//! PostgreSQL 18.3), trimmed to what `operatorcmds.c` reads off the operator's
//! `Form_pg_operator` and the attribute numbers `AlterOperator` packs into the
//! `values`/`replaces` arrays.

use types_core::primitive::Oid;

/// `OperatorRelationId` — `pg_operator` (OID 2617).
pub const OperatorRelationId: Oid = 2617;

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

/// `FormData_pg_operator` (`catalog/pg_operator.h`) — projected to the fields
/// `operatorcmds.c` reads off the cached/copied operator tuple. The catalog
/// owner (`pg_operator.c`) materializes this from the heap tuple via the
/// `fetch_operator_form` seam.
#[derive(Clone, Debug)]
pub struct FormPgOperator {
    /// `oid` — the operator's own OID.
    pub oid: Oid,
    /// `oprname` — operator name (`NameData`).
    pub oprname: String,
    /// `oprleft` — left argument type, or `InvalidOid`.
    pub oprleft: Oid,
    /// `oprright` — right argument type.
    pub oprright: Oid,
    /// `oprresult` — result type.
    pub oprresult: Oid,
    /// `oprcom` — commutator operator, or `InvalidOid`.
    pub oprcom: Oid,
    /// `oprnegate` — negator operator, or `InvalidOid`.
    pub oprnegate: Oid,
    /// `oprcanmerge` — operator supports merge joins.
    pub oprcanmerge: bool,
    /// `oprcanhash` — operator supports hash joins.
    pub oprcanhash: bool,
}
