//! Scan-cursor value types for the pg_depend systable scans
//! (`backend/catalog/pg_depend.c`): what one matching catalog row looks like
//! when it crosses the systable-scan seam.

use crate::catalog_dependency::FormData_pg_depend;
use types_core::primitive::Oid;
use types_tuple::heaptuple::ItemPointerData;

/// One pg_depend catalog row returned by a seam scan: the `GETSTRUCT` form
/// plus the heap TID (`tup->t_self`), so delete/update legs can address the
/// row.
#[derive(Clone, Copy, Debug)]
pub struct DependTuple {
    /// `(Form_pg_depend) GETSTRUCT(tup)` — the row's catalog columns.
    pub form: FormData_pg_depend,
    /// `tup->t_self` — the row's heap location.
    pub tid: ItemPointerData,
}

/// Which pg_depend index a scan runs against.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependIndex {
    /// `DependDependerIndexId` — keyed on (classid, objid, objsubid).
    Depender,
    /// `DependReferenceIndexId` — keyed on (refclassid, refobjid, refobjsubid).
    Reference,
}

/// The equality scan keys handed to `systable_beginscan` (`ScanKeyInit` with
/// `BTEqualStrategyNumber`).
#[derive(Clone, Copy, Debug, Default)]
pub struct DependScanKeys {
    /// First key column (`classid` or `refclassid`) — `F_OIDEQ`.
    pub key0_oid: Oid,
    /// Second key column (`objid` or `refobjid`) — `F_OIDEQ`.
    pub key1_oid: Oid,
    /// Optional third key column (`objsubid` or `refobjsubid`) — `F_INT4EQ`.
    pub key2_int4: Option<i32>,
}
