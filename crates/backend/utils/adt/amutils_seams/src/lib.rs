//! Seam declarations for the `backend-utils-adt-amutils` unit
//! (`src/backend/utils/adt/amutils.c` — the SQL-level index access-method
//! property-reporting functions).
//!
//! The *logic* of amutils.c — `lookup_prop_name`, `test_indoption`, and the
//! whole `indexam_property` decision tree — is ported in-crate in the
//! `backend-utils-adt-amutils` owner. amutils.c is otherwise a thin SQL
//! wrapper that reaches into two genuinely external subsystems, both crossing
//! the seams declared here:
//!
//!  * **`utils/cache/syscache.c` / `utils/cache/relcache.c`** — the `pg_class`
//!    and `pg_index` syscache tuples (and the `indoption` `int2vector`). These
//!    two seams (`index_relation`, `index_form`) are installed by the syscache
//!    owner (`backend-utils-cache-syscache`).
//!  * **`access/index/amapi.c` + `access/index/indexam.c`** — the index AM API:
//!    `GetIndexAmRoutineByAmId` (the capability-flag projection), the AM's
//!    `amproperty` / `ambuildphasename` callbacks (which the unified
//!    `IndexAmRoutine` vtable does not carry — they are dispatched by AM OID by
//!    name, exactly as `amvalidate` is), and the generic
//!    `index_open` / `index_can_return` / `index_close` fallback. These four
//!    seams (`am_routine`, `am_property`, `index_can_return`,
//!    `am_buildphasename`) are installed by the amapi owner
//!    (`backend-access-index-amapi`).
//!
//! The seam surface is deliberately narrow: each slot returns the *raw* AM
//! capability flags or the *raw* catalog rows, so every branch of the C
//! decision logic stays in-crate and is ported 1:1.
//!
//! C's `GetIndexAmRoutineByAmId(amoid, noerror = true)` — amutils always passes
//! `noerror = true` — returns NULL silently on a missing AM / handler. The
//! `am_routine` seam mirrors that with `Ok(None)`.

use types_core::Oid;
use types_error::PgResult;

/// `IndexAMProperty` (`access/amapi.h`) — the recognized index-AM property
/// codes that `indexam_property` dispatches on. `AMPROP_UNKNOWN` is the
/// "name not recognized" sentinel (`lookup_prop_name` returns it for any AM-
/// defined property name, which is not an error).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexAmProperty {
    /// `AMPROP_UNKNOWN`
    Unknown,
    /// `AMPROP_ASC`
    Asc,
    /// `AMPROP_DESC`
    Desc,
    /// `AMPROP_NULLS_FIRST`
    NullsFirst,
    /// `AMPROP_NULLS_LAST`
    NullsLast,
    /// `AMPROP_ORDERABLE`
    Orderable,
    /// `AMPROP_DISTANCE_ORDERABLE`
    DistanceOrderable,
    /// `AMPROP_RETURNABLE`
    Returnable,
    /// `AMPROP_SEARCH_ARRAY`
    SearchArray,
    /// `AMPROP_SEARCH_NULLS`
    SearchNulls,
    /// `AMPROP_CLUSTERABLE`
    Clusterable,
    /// `AMPROP_INDEX_SCAN`
    IndexScan,
    /// `AMPROP_BITMAP_SCAN`
    BitmapScan,
    /// `AMPROP_BACKWARD_SCAN`
    BackwardScan,
    /// `AMPROP_CAN_ORDER`
    CanOrder,
    /// `AMPROP_CAN_UNIQUE`
    CanUnique,
    /// `AMPROP_CAN_MULTI_COL`
    CanMultiCol,
    /// `AMPROP_CAN_EXCLUDE`
    CanExclude,
    /// `AMPROP_CAN_INCLUDE`
    CanInclude,
}

/// The scalar `IndexAmRoutine` capability flags `indexam_property` reads, plus
/// the `routine->amX != NULL` "callback is present" booleans the C tests
/// (`has_amproperty`, `has_amcanreturn`, `has_amgettuple`, `has_amgetbitmap`,
/// `has_ambuildphasename`). The unified vtable carries the flags directly; the
/// `has_*` fields are derived by the installer from the AM's routine.
#[derive(Clone, Copy, Debug, Default)]
pub struct IndexAmRoutineFlags {
    pub amcanorder: bool,
    pub amcanorderbyop: bool,
    pub amcanbackward: bool,
    pub amcanunique: bool,
    pub amcanmulticol: bool,
    pub amsearcharray: bool,
    pub amsearchnulls: bool,
    pub amclusterable: bool,
    pub amcaninclude: bool,
    /// `routine->amproperty != NULL`
    pub has_amproperty: bool,
    /// `routine->amcanreturn != NULL`
    pub has_amcanreturn: bool,
    /// `routine->amgettuple != NULL`
    pub has_amgettuple: bool,
    /// `routine->amgetbitmap != NULL`
    pub has_amgetbitmap: bool,
    /// `routine->ambuildphasename != NULL`
    pub has_ambuildphasename: bool,
}

/// The fixed-width `Form_pg_class` columns `indexam_property` reads off
/// `SearchSysCache1(RELOID, ObjectIdGetDatum(index_oid))`.
#[derive(Clone, Copy, Debug, Default)]
pub struct IndexRelationInfo {
    /// `relkind` — must be `RELKIND_INDEX` or `RELKIND_PARTITIONED_INDEX`.
    pub relkind: u8,
    /// `relam` — the index's access-method OID.
    pub relam: Oid,
    /// `relnatts` — the number of index columns.
    pub relnatts: i16,
}

/// The `Form_pg_index` columns `indexam_property` reads off
/// `SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid))`, plus the
/// `indoption` `int2vector` (`SysCacheGetAttrNotNull(... Anum_pg_index_indoption)`).
#[derive(Clone, Debug, Default)]
pub struct IndexFormInfo {
    pub indexrelid: Oid,
    pub indnatts: i16,
    pub indnkeyatts: i16,
    /// `int2vector indoption` — per-column AM option flags.
    pub indoption: Vec<i16>,
}

/// The arguments the AM's `amproperty` callback receives
/// (`routine->amproperty(index_oid, attno, prop, propname, &res, &isnull)`).
#[derive(Clone, Debug)]
pub struct AmPropertyRequest {
    pub amoid: Oid,
    pub index_oid: Oid,
    pub attno: i32,
    pub prop: IndexAmProperty,
    pub propname: String,
}

seam_core::seam!(
    /// `GetIndexAmRoutineByAmId(amoid, noerror = true)` (amapi.c) projected to
    /// the scalar `IndexAmRoutine` flags + `routine->amX != NULL` booleans
    /// `indexam_property` reads. `Ok(None)` for the C `routine == NULL`
    /// (missing AM / handler) path — amutils calls this with `noerror = true`,
    /// so the not-found cases are a silent NULL result, not an error.
    pub fn am_routine(amoid: Oid) -> PgResult<Option<IndexAmRoutineFlags>>
);

seam_core::seam!(
    /// `routine->amproperty(index_oid, attno, prop, propname, &res, &isnull)`
    /// — the AM's optional property callback, dispatched by AM OID by name
    /// (the unified `IndexAmRoutine` vtable does not carry `amproperty`, the
    /// same as `amvalidate`). Returns `Ok(None)` for the C `false` ("not
    /// handled, fall through to generic logic"), or `Ok(Some((res, isnull)))`
    /// for the C `true` (the callback answered: `res`/`isnull` out-params).
    /// Some AMs' `amproperty` (gist/spgist) do catalog lookups + allocate, so
    /// the seam takes the caller's `Mcx` (C's `CurrentMemoryContext`).
    pub fn am_property<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        req: AmPropertyRequest,
    ) -> PgResult<Option<(bool, bool)>>
);

seam_core::seam!(
    /// `SearchSysCache1(RELOID, ObjectIdGetDatum(index_oid))` + `GETSTRUCT`
    /// projected to `(relkind, relam, relnatts)`. `Ok(None)` on a cache miss
    /// (`!HeapTupleIsValid`); the installer owns the `ReleaseSysCache`.
    pub fn index_relation(index_oid: Oid) -> PgResult<Option<IndexRelationInfo>>
);

seam_core::seam!(
    /// `SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid))` +
    /// `GETSTRUCT` + `SysCacheGetAttrNotNull(... Anum_pg_index_indoption)`
    /// projected to `(indexrelid, indnatts, indnkeyatts, indoption)`.
    /// `Ok(None)` on a cache miss; the installer owns the `ReleaseSysCache`.
    pub fn index_form(index_oid: Oid) -> PgResult<Option<IndexFormInfo>>
);

seam_core::seam!(
    /// The generic `AMPROP_RETURNABLE` fallback when the AM has `amcanreturn`
    /// but no overriding `amproperty`:
    /// `indexrel = index_open(index_oid, AccessShareLock);`
    /// `res = index_can_return(indexrel, attno);`
    /// `index_close(indexrel, AccessShareLock);` `Err` carries the
    /// `index_open` / `index_can_return` `ereport(ERROR)`s. The transient
    /// `index_open` allocates the relcache-handle in the caller's `Mcx`
    /// (C's `CurrentMemoryContext`), so the seam takes one.
    pub fn index_can_return<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        index_oid: Oid,
        attno: i32,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `name = routine->ambuildphasename(phasenum);` then
    /// `CStringGetTextDatum(name)` (or `PG_RETURN_NULL` for a NULL name) — the
    /// AM's optional build-phase-name callback, dispatched by AM OID by name
    /// (the unified vtable does not carry `ambuildphasename`). `Ok(None)` for
    /// the C NULL name; `Ok(Some(name))` for the phase name string. The caller
    /// has already checked `routine->ambuildphasename != NULL`.
    pub fn am_buildphasename(amoid: Oid, phasenum: i64) -> PgResult<Option<String>>
);
