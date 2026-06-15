//! `pg_aggregate` catalog row layout and constants (`catalog/pg_aggregate.h`,
//! PostgreSQL 18.3), trimmed to what consumers of the `agg_row_by_oid`
//! syscache projection read.

use types_core::primitive::Oid;

/* ==========================================================================
 * Catalog relation + index OIDs (pg_aggregate.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `AggregateRelationId` — `pg_aggregate` (OID 2600).
pub const AggregateRelationId: Oid = 2600;
/// `AggregateFnoidIndexId` — `pg_aggregate_fnoid_index` (OID 2650).
pub const AggregateFnoidIndexId: Oid = 2650;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_aggregate).
 * ======================================================================== */

pub const Anum_pg_aggregate_aggfnoid: i16 = 1;
pub const Anum_pg_aggregate_aggkind: i16 = 2;
pub const Anum_pg_aggregate_aggnumdirectargs: i16 = 3;
pub const Anum_pg_aggregate_aggtransfn: i16 = 4;
pub const Anum_pg_aggregate_aggfinalfn: i16 = 5;

/* ==========================================================================
 * aggkind codes (pg_aggregate.h).
 * ======================================================================== */

/// `AGGKIND_NORMAL` — plain aggregate.
pub const AGGKIND_NORMAL: i8 = b'n' as i8;
/// `AGGKIND_ORDERED_SET` — ordered-set aggregate.
pub const AGGKIND_ORDERED_SET: i8 = b'o' as i8;
/// `AGGKIND_HYPOTHETICAL` — hypothetical-set aggregate.
pub const AGGKIND_HYPOTHETICAL: i8 = b'h' as i8;

/// `AGGKIND_IS_ORDERED_SET(kind)` — ordered-set or hypothetical-set.
#[inline]
pub fn AGGKIND_IS_ORDERED_SET(kind: i8) -> bool {
    kind != AGGKIND_NORMAL
}

/* ==========================================================================
 * Row carriers.
 * ======================================================================== */

/// The `pg_aggregate` columns `func_get_detail` (`parse_func.c`) reads out of
/// `(Form_pg_aggregate) GETSTRUCT(SearchSysCache1(AGGFNOID, funcid))` for an
/// aggregate function: the aggregate kind and the count of direct arguments
/// (`catDirectArgs`). All fixed-width non-null columns.
#[derive(Clone, Copy, Debug)]
pub struct AggRow {
    /// `classForm->aggkind` — the raw C `char` (`AGGKIND_*`).
    pub aggkind: i8,
    /// `classForm->aggnumdirectargs` — `int16` in the catalog, read into the
    /// `int catDirectArgs` local in C.
    pub aggnumdirectargs: i32,
}
