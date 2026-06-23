//! `pg_aggregate` catalog row layout and constants (`catalog/pg_aggregate.h`,
//! PostgreSQL 18.3).
//!
//! Carries both the trimmed `AggRow` syscache projection (what `func_get_detail`
//! reads) and the full `FormData_pg_aggregate` fixed-part struct + the
//! `PgAggregateInsertRow` carrier that `AggregateCreate` (`pg_aggregate.c`)
//! builds for `heap_form_tuple` / `heap_modify_tuple`.

extern crate alloc;
use alloc::string::String;
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
pub const Anum_pg_aggregate_aggcombinefn: i16 = 6;
pub const Anum_pg_aggregate_aggserialfn: i16 = 7;
pub const Anum_pg_aggregate_aggdeserialfn: i16 = 8;
pub const Anum_pg_aggregate_aggmtransfn: i16 = 9;
pub const Anum_pg_aggregate_aggminvtransfn: i16 = 10;
pub const Anum_pg_aggregate_aggmfinalfn: i16 = 11;
pub const Anum_pg_aggregate_aggfinalextra: i16 = 12;
pub const Anum_pg_aggregate_aggmfinalextra: i16 = 13;
pub const Anum_pg_aggregate_aggfinalmodify: i16 = 14;
pub const Anum_pg_aggregate_aggmfinalmodify: i16 = 15;
pub const Anum_pg_aggregate_aggsortop: i16 = 16;
pub const Anum_pg_aggregate_aggtranstype: i16 = 17;
pub const Anum_pg_aggregate_aggtransspace: i16 = 18;
pub const Anum_pg_aggregate_aggmtranstype: i16 = 19;
pub const Anum_pg_aggregate_aggmtransspace: i16 = 20;
pub const Anum_pg_aggregate_agginitval: i16 = 21;
pub const Anum_pg_aggregate_aggminitval: i16 = 22;

/// `Natts_pg_aggregate` — 20 fixed columns + the two `CATALOG_VARLEN` `text`
/// columns (`agginitval`, `aggminitval`).
pub const Natts_pg_aggregate: usize = 22;

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
/// (`catDirectArgs`). All fixed-width non-null columns. Also read by
/// `AggregateCreate`'s REPLACE path (`oldagg->aggkind` /
/// `oldagg->aggnumdirectargs`).
#[derive(Clone, Copy, Debug)]
pub struct AggRow {
    /// `classForm->aggkind` — the raw C `char` (`AGGKIND_*`).
    pub aggkind: i8,
    /// `classForm->aggnumdirectargs` — `int16` in the catalog, read into the
    /// `int catDirectArgs` local in C.
    pub aggnumdirectargs: i32,
}

/// The fixed-part columns of `FormData_pg_aggregate` (`catalog/pg_aggregate.h`),
/// in catalog field order. The two trailing `CATALOG_VARLEN` text columns
/// (`agginitval` / `aggminitval`) are carried separately on
/// [`PgAggregateInsertRow`] (they are nullable). `regproc` / `Oid` columns are
/// `Oid`; the `char` columns are `i8`; `int16`/`int32` are `i16`/`i32`.
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_aggregate {
    pub aggfnoid: Oid,
    pub aggkind: i8,
    pub aggnumdirectargs: i16,
    pub aggtransfn: Oid,
    pub aggfinalfn: Oid,
    pub aggcombinefn: Oid,
    pub aggserialfn: Oid,
    pub aggdeserialfn: Oid,
    pub aggmtransfn: Oid,
    pub aggminvtransfn: Oid,
    pub aggmfinalfn: Oid,
    pub aggfinalextra: bool,
    pub aggmfinalextra: bool,
    pub aggfinalmodify: i8,
    pub aggmfinalmodify: i8,
    pub aggsortop: Oid,
    pub aggtranstype: Oid,
    pub aggtransspace: i32,
    pub aggmtranstype: Oid,
    pub aggmtransspace: i32,
}

/// The full `Form_pg_aggregate` projection (`GETSTRUCT(SearchSysCache1(AGGFNOID,
/// aggfnoid))`) plus the two trailing `CATALOG_VARLEN` text columns, deserialized
/// to owned strings. This is what `ExecInitAgg`'s `fetch_agg_form`
/// (`nodeAgg.c:3490`) reads while the `aggTuple` is pinned: every aggregate
/// support-function Oid (`aggtransfn`/`aggfinalfn`/`aggcombinefn`/`aggserialfn`/
/// `aggdeserialfn` + the moving-aggregate `aggmtransfn`/`aggminvtransfn`/
/// `aggmfinalfn`), the `aggfinalextra`/`aggmfinalextra` flags, the
/// `aggfinalmodify`/`aggmfinalmodify` chars, `aggkind`, the transition-type and
/// space columns, and the (nullable) `agginitval`/`aggminitval` text initial
/// values. Carried as one value because the C holds the syscache tuple pinned
/// across all of these reads.
///
/// `regproc`/`Oid` columns are `Oid`; the `char` columns are `i8`; the
/// `int16`/`int32` columns are `i16`/`i32`; the `bool` columns are `bool`. The
/// two `CATALOG_VARLEN` `text` columns are `Option<String>` (`None` when SQL
/// NULL, matching the C `isnull` from `SysCacheGetAttr`); the caller deserializes
/// the present text via the type's input function (`GetAggInitVal`).
#[derive(Clone, Debug)]
pub struct AggFormData {
    /// `aggform->aggfnoid` — the aggregate's pg_proc OID (the search key).
    pub aggfnoid: Oid,
    /// `aggform->aggkind` — the raw C `char` (`AGGKIND_*`).
    pub aggkind: i8,
    /// `aggform->aggnumdirectargs` — count of ordered-set direct arguments.
    pub aggnumdirectargs: i16,
    /// `aggform->aggtransfn` — the state transition function.
    pub aggtransfn: Oid,
    /// `aggform->aggfinalfn` — the final function (or `InvalidOid`).
    pub aggfinalfn: Oid,
    /// `aggform->aggcombinefn` — the combine function (or `InvalidOid`).
    pub aggcombinefn: Oid,
    /// `aggform->aggserialfn` — the serialization function (or `InvalidOid`).
    pub aggserialfn: Oid,
    /// `aggform->aggdeserialfn` — the deserialization function (or `InvalidOid`).
    pub aggdeserialfn: Oid,
    /// `aggform->aggmtransfn` — the moving-aggregate transition function.
    pub aggmtransfn: Oid,
    /// `aggform->aggminvtransfn` — the moving-aggregate inverse transition fn.
    pub aggminvtransfn: Oid,
    /// `aggform->aggmfinalfn` — the moving-aggregate final function.
    pub aggmfinalfn: Oid,
    /// `aggform->aggfinalextra` — pass extra dummy args to the final fn.
    pub aggfinalextra: bool,
    /// `aggform->aggmfinalextra` — same, for the moving-aggregate final fn.
    pub aggmfinalextra: bool,
    /// `aggform->aggfinalmodify` — final-fn modify behaviour (`AGGMODIFY_*`).
    pub aggfinalmodify: i8,
    /// `aggform->aggmfinalmodify` — same, for the moving-aggregate final fn.
    pub aggmfinalmodify: i8,
    /// `aggform->aggsortop` — the associated sort operator (or `InvalidOid`).
    pub aggsortop: Oid,
    /// `aggform->aggtranstype` — the transition (state) data type.
    pub aggtranstype: Oid,
    /// `aggform->aggtransspace` — declared transition-value size estimate.
    pub aggtransspace: i32,
    /// `aggform->aggmtranstype` — the moving-aggregate transition data type.
    pub aggmtranstype: Oid,
    /// `aggform->aggmtransspace` — moving-aggregate transition size estimate.
    pub aggmtransspace: i32,
    /// `SysCacheGetAttr(AGGFNOID, .., Anum_pg_aggregate_agginitval, &isnull)` —
    /// the initial transition value text (`None` when SQL NULL).
    pub agginitval: Option<String>,
    /// `SysCacheGetAttr(AGGFNOID, .., Anum_pg_aggregate_aggminitval, &isnull)` —
    /// the moving-aggregate initial value text (`None` when SQL NULL).
    pub aggminitval: Option<String>,
}

/// The `values[]` / `nulls[]` arrays `AggregateCreate` builds for
/// `heap_form_tuple` / `heap_modify_tuple`. The fixed columns are the
/// [`FormData_pg_aggregate`]; `agginitval` / `aggminitval` are the two nullable
/// `text` columns (`CStringGetTextDatum(agginitval)` when present, otherwise
/// `nulls[..] = true`).
#[derive(Clone, Debug)]
pub struct PgAggregateInsertRow {
    pub form: FormData_pg_aggregate,
    pub agginitval: Option<String>,
    pub aggminitval: Option<String>,
}

/// The three `replaces[]` columns `AggregateCreate` pins to the old tuple on the
/// REPLACE path (`replaces[aggfnoid/aggkind/aggnumdirectargs - 1] = false`); all
/// other columns are replaced. Carried so the indexing keystone's
/// `heap_modify_tuple` can reproduce the C `replaces[]` array.
#[derive(Clone, Copy, Debug)]
pub struct PgAggregateReplaces {
    pub aggfnoid: bool,
    pub aggkind: bool,
    pub aggnumdirectargs: bool,
}
