//! Carrier types for the `backend/statistics` subsystem (extended statistics).
//!
//! Owned-tree mirrors of the C structs in `statistics/statistics.h` and
//! `statistics/extended_stats_internal.h`. Only the functional-dependency
//! carriers (`MVDependencies`/`MVDependency`) and the `STATS_*` constants are
//! modeled so far (consumed by `backend-statistics-dependencies`); other
//! extended-stat carriers (MCV / ndistinct / multi-sort) are added when their
//! owners land.

#![no_std]

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;
use ::mcx::Mcx;
use types_core::{AttrNumber, Oid};
use ::types_tuple::pg_type::FormData_pg_type;
// The canonical `'mcx` byte-lane value type (`ByVal(usize)` / `ByRef(PgVec<u8>)`).
// Statistic values that may be pass-by-reference (text/numeric/varchar/…) must
// live in this safe byte lane, NOT a bare `usize` word (which cannot carry the
// referenced bytes and would dangle when copied into a temporary context).
use types_tuple::{Datum, FormedTuple, TupleDesc};

/// `STATISTIC_NUM_SLOTS` (`catalog/pg_statistic.h:127`): the number of
/// statistic-kind slots in a `pg_statistic` row (and thus in [`VacAttrStats`]).
pub const STATISTIC_NUM_SLOTS: usize = 5;

/* ---------------------------------------------------------------------------
 * `pg_statistic` catalog vocabulary (`catalog/pg_statistic.h`, generated
 * `pg_statistic_d.h`) consumed by `update_attstats` (commands/analyze.c). The
 * attribute numbers follow the 1-based `FormData_pg_statistic` field order; the
 * slotted fields (`stakind1`/`staop1`/`stacoll1`/`stanumbers1`/`stavalues1`)
 * are the first of `STATISTIC_NUM_SLOTS` consecutive columns each.
 * ------------------------------------------------------------------------- */

/// `StatisticRelationId` (`catalog/pg_statistic.h:29`) — the OID of the
/// `pg_statistic` catalog (`2619`).
pub const StatisticRelationId: Oid = 2619;

/// `Natts_pg_statistic` (`catalog/pg_statistic_d.h`) — number of columns in a
/// `pg_statistic` row (3 key + nullfrac/width/distinct + 5×{kind,op,coll,numbers,values}).
pub const Natts_pg_statistic: usize = 31;

/// `Anum_pg_statistic_starelid` (1).
pub const Anum_pg_statistic_starelid: usize = 1;
/// `Anum_pg_statistic_staattnum` (2).
pub const Anum_pg_statistic_staattnum: usize = 2;
/// `Anum_pg_statistic_stainherit` (3).
pub const Anum_pg_statistic_stainherit: usize = 3;
/// `Anum_pg_statistic_stanullfrac` (4).
pub const Anum_pg_statistic_stanullfrac: usize = 4;
/// `Anum_pg_statistic_stawidth` (5).
pub const Anum_pg_statistic_stawidth: usize = 5;
/// `Anum_pg_statistic_stadistinct` (6).
pub const Anum_pg_statistic_stadistinct: usize = 6;
/// `Anum_pg_statistic_stakind1` (7) — first of `STATISTIC_NUM_SLOTS` `stakindN`.
pub const Anum_pg_statistic_stakind1: usize = 7;
/// `Anum_pg_statistic_staop1` (12) — first of `STATISTIC_NUM_SLOTS` `staopN`.
pub const Anum_pg_statistic_staop1: usize = 12;
/// `Anum_pg_statistic_stacoll1` (17) — first of `STATISTIC_NUM_SLOTS` `stacollN`.
pub const Anum_pg_statistic_stacoll1: usize = 17;
/// `Anum_pg_statistic_stanumbers1` (22) — first of `STATISTIC_NUM_SLOTS`
/// `stanumbersN`.
pub const Anum_pg_statistic_stanumbers1: usize = 22;
/// `Anum_pg_statistic_stavalues1` (27) — first of `STATISTIC_NUM_SLOTS`
/// `stavaluesN`.
pub const Anum_pg_statistic_stavalues1: usize = 27;

/// `STATISTIC_KIND_MCV` (`catalog/pg_statistic.h:190`) — most-common-values slot.
pub const STATISTIC_KIND_MCV: i16 = 1;
/// `STATISTIC_KIND_HISTOGRAM` (`catalog/pg_statistic.h:210`) — histogram slot.
pub const STATISTIC_KIND_HISTOGRAM: i16 = 2;
/// `STATISTIC_KIND_CORRELATION` (`catalog/pg_statistic.h:222`) — correlation slot.
pub const STATISTIC_KIND_CORRELATION: i16 = 3;

/// `FLOAT4OID` (`catalog/pg_type.dat`) — the `float4` (`real`) type OID (`700`),
/// used by `update_attstats` to build the `stanumbersN` `float4[]` arrays.
pub const FLOAT4OID: Oid = 700;

/// `STATS_MAX_DIMENSIONS` (`statistics/statistics.h`): the maximum number of
/// columns/expressions an extended statistics object may cover.
pub const STATS_MAX_DIMENSIONS: usize = 8;

/// `STATS_DEPS_MAGIC` (`statistics/statistics.h`): magic number identifying a
/// serialized `MVDependencies` blob (`0xB4549A2C`).
pub const STATS_DEPS_MAGIC: u32 = 0xB454_9A2C;

/// `STATS_DEPS_TYPE_BASIC` (`statistics/statistics.h`): the only dependency
/// serialization type.
pub const STATS_DEPS_TYPE_BASIC: u32 = 1;

/// `STATS_NDISTINCT_MAGIC` (`statistics/statistics.h`): magic number identifying
/// a serialized `MVNDistinct` blob (`0xA352BFA4`).
pub const STATS_NDISTINCT_MAGIC: u32 = 0xA352_BFA4;

/// `STATS_NDISTINCT_TYPE_BASIC` (`statistics/statistics.h`): the only ndistinct
/// serialization type.
pub const STATS_NDISTINCT_TYPE_BASIC: u32 = 1;

/// `STATS_EXT_DEPENDENCIES` (`catalog/pg_statistic_ext.h`): the 'f' kind char.
pub const STATS_EXT_DEPENDENCIES: i8 = b'f' as i8;

/// `STATS_EXT_NDISTINCT` (`catalog/pg_statistic_ext.h`): the 'd' kind char.
pub const STATS_EXT_NDISTINCT: i8 = b'd' as i8;

/// `STATS_EXT_MCV` (`catalog/pg_statistic_ext.h`): the 'm' kind char.
pub const STATS_EXT_MCV: i8 = b'm' as i8;

/// `STATS_EXT_EXPRESSIONS` (`catalog/pg_statistic_ext.h`): the 'e' kind char.
pub const STATS_EXT_EXPRESSIONS: i8 = b'e' as i8;

/* ---------------------------------------------------------------------------
 * Multivariate MCV lists (`statistics/statistics.h`).
 * ------------------------------------------------------------------------- */

/// `STATS_MCV_MAGIC` (`statistics/statistics.h:66`): magic number identifying a
/// serialized MCV-list blob (`0xE1A651C2`, "marks serialized bytea").
pub const STATS_MCV_MAGIC: u32 = 0xE1A6_51C2;

/// `STATS_MCV_TYPE_BASIC` (`statistics/statistics.h:67`): the only MCV
/// serialization type ("basic MCV list type").
pub const STATS_MCV_TYPE_BASIC: u32 = 1;

/// `MAX_STATISTICS_TARGET` (`commands/vacuum.h:324`).
pub const MAX_STATISTICS_TARGET: i32 = 10000;

/// `STATS_MCVLIST_MAX_ITEMS` (`statistics/statistics.h:70`):
/// `= MAX_STATISTICS_TARGET`.
pub const STATS_MCVLIST_MAX_ITEMS: i32 = MAX_STATISTICS_TARGET;

/// `MCVItem` (`statistics/statistics.h`).
///
/// C is a struct of pointers into a single chunk:
/// ```c
/// typedef struct MCVItem {
///     double      frequency;       /* frequency of this combination */
///     double      base_frequency;  /* frequency if independent */
///     bool       *isnull;          /* NULL flags */
///     Datum      *values;          /* item values */
/// } MCVItem;
/// ```
/// The owned mirror replaces the `Datum *`/`bool *` arrays with owned `Vec`s;
/// the invariant `values.len() == isnull.len() == ndimensions` is upheld by the
/// (de)serializers and the build loop.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MCVItem<'mcx> {
    /// frequency of this combination
    pub frequency: f64,
    /// frequency if independent
    pub base_frequency: f64,
    /// NULL flags
    pub isnull: Vec<bool>,
    /// item values (the safe `'mcx` byte lane)
    pub values: Vec<Datum<'mcx>>,
}

/// `MCVList` (`statistics/statistics.h`).
///
/// C is a flexible-array struct:
/// ```c
/// typedef struct MCVList {
///     uint32      magic;        /* magic constant marker */
///     uint32      type;         /* type of MCV list (BASIC) */
///     uint32      nitems;       /* number of MCV items in the array */
///     AttrNumber  ndimensions;  /* number of dimensions */
///     Oid         types[STATS_MAX_DIMENSIONS]; /* OIDs of data types */
///     MCVItem     items[FLEXIBLE_ARRAY_MEMBER]; /* array of MCV items */
/// } MCVList;
/// ```
/// The owned mirror replaces the FAM with an owned `Vec<MCVItem>`; the invariant
/// `items.len() == nitems` is upheld by the (de)serializers and the build loop.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MCVList<'mcx> {
    /// magic constant marker
    pub magic: u32,
    /// type of MCV list (BASIC)
    pub r#type: u32,
    /// number of MCV items in the array
    pub nitems: u32,
    /// number of dimensions
    pub ndimensions: AttrNumber,
    /// OIDs of data types
    pub types: [Oid; STATS_MAX_DIMENSIONS],
    /// array of MCV items
    pub items: Vec<MCVItem<'mcx>>,
}

/// `DimensionInfo` (`statistics/extended_stats_internal.h`): (de)serialization
/// info for one dimension.
///
/// ```c
/// typedef struct DimensionInfo {
///     int     nvalues;        /* number of deduplicated values */
///     int     nbytes;         /* number of bytes (serialized) */
///     int     nbytes_aligned; /* size of deserialized data with alignment */
///     int     typlen;         /* pg_type.typlen */
///     bool    typbyval;       /* pg_type.typbyval */
/// } DimensionInfo;
/// ```
/// The raw struct bytes (four `int`s = 16 bytes + one `bool`, padded to a 4-byte
/// boundary = 20 bytes) are copied into the serialized representation, so the
/// layout must match PG exactly for the bytea to interop with the real catalog.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DimensionInfo {
    /// number of deduplicated values
    pub nvalues: i32,
    /// number of bytes (serialized)
    pub nbytes: i32,
    /// size of deserialized data with alignment
    pub nbytes_aligned: i32,
    /// pg_type.typlen
    pub typlen: i32,
    /// pg_type.typbyval
    pub typbyval: bool,
}

/// `SortItem` (`statistics/extended_stats_internal.h`): a row of values+nulls
/// with a multiplicity count.
///
/// ```c
/// typedef struct SortItem {
///     Datum      *values;
///     bool       *isnull;
///     int         count;
/// } SortItem;
/// ```
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SortItem<'mcx> {
    pub values: Vec<Datum<'mcx>>,
    pub isnull: Vec<bool>,
    pub count: i32,
}

/* ---------------------------------------------------------------------------
 * VacAttrStats (`commands/vacuum.h:116`) — per-column ANALYZE working state.
 * ------------------------------------------------------------------------- */

/// `AnalyzeAttrFetchFunc` (`commands/vacuum.h:108`):
/// `Datum (*)(VacAttrStatsP stats, int rownum, bool *isNull)`.
///
/// The fetch function projects the `rownum`-th sampled value of the analyzed
/// column out of the `VacAttrStats` working state, setting `*isNull`. Producers
/// (`std_fetch_func` / `ind_fetch_func`) and the consumers (the `compute_stats`
/// routines) all live in the not-yet-ported ANALYZE driver
/// (`backend-commands-analyze`); this carrier models the field faithfully as the
/// safe function-pointer alias over the owned [`VacAttrStats`].
pub type AnalyzeAttrFetchFunc =
    for<'mcx> fn(stats: &VacAttrStats<'mcx>, rownum: i32, is_null: &mut bool) -> Datum<'mcx>;

/// `AnalyzeAttrComputeStatsFunc` (`commands/vacuum.h:111`):
/// `void (*)(VacAttrStatsP, AnalyzeAttrFetchFunc, int samplerows, double totalrows)`.
///
/// The type-specific statistics-computation routine selected by the column's
/// `typanalyze` function. It fills the output fields of `VacAttrStats`. Lives in
/// the ANALYZE driver; modeled faithfully here as a function-pointer alias over
/// the owned [`VacAttrStats`].
pub type AnalyzeAttrComputeStatsFunc = for<'mcx> fn(
    stats: &mut VacAttrStats<'mcx>,
    fetchfunc: AnalyzeAttrFetchFunc,
    samplerows: i32,
    totalrows: f64,
);

/// `VacAttrStats` (`commands/vacuum.h:116`) — the per-column working state the
/// ANALYZE machinery passes through the type-specific `typanalyze` /
/// `compute_stats` routines and the extended-statistics build framework.
///
/// Field-for-field mirror of the C struct. Two model adaptations follow this
/// repo's conventions:
///
///   * the C `Datum *stavalues[]` / `float4 *stanumbers[]` heap arrays become
///     owned `Vec<Datum>` / `Vec<f32>` per slot (the safe value lane; the C
///     `numvalues[n]` / `numnumbers[n]` lengths are the corresponding
///     `Vec::len()`, kept as explicit fields to stay 1:1 with the C struct and
///     the catalog write);
///   * `MemoryContext anl_context` becomes [`::mcx::Mcx`], `Form_pg_type attrtype`
///     becomes `Option<FormData_pg_type>` (NULL before set up), `TupleDesc`
///     becomes the repo `TupleDesc`, and `HeapTuple *rows` becomes
///     `Vec<HeapTuple>` (the std-fetch sample-row access info).
///
/// The C `Datum *exprvals` / `bool *exprnulls` index-fetch flat buffers become
/// owned `Vec`s with the same `rowstride` semantics.
pub struct VacAttrStats<'mcx> {
    /* ----- set up by main ANALYZE code before invoking typanalyze ----- */
    /// `attstattarget` — -1 to use default.
    pub attstattarget: i32,
    /// `attrtypid` — type of data being analyzed.
    pub attrtypid: Oid,
    /// `attrtypmod` — typmod of data being analyzed.
    pub attrtypmod: i32,
    /// `attrtype` — copy of the `pg_type` row for `attrtypid` (`Form_pg_type`).
    pub attrtype: Option<FormData_pg_type>,
    /// `attrcollid` — collation of data being analyzed.
    pub attrcollid: Oid,
    /// `anl_context` — where to save long-lived data (`MemoryContext`).
    pub anl_context: Option<Mcx<'mcx>>,

    /* ----- filled in by the typanalyze routine (unless it returns false) ----- */
    /// `compute_stats` — type-specific statistics-computation function pointer.
    pub compute_stats: Option<AnalyzeAttrComputeStatsFunc>,
    /// `minrows` — minimum # of rows wanted for stats.
    pub minrows: i32,
    /// `extra_data` — for extra type-specific data (C `void *`).
    ///
    /// The type-specific payload (e.g. `StdAnalyzeData`) lives in the ANALYZE
    /// driver; carried here as the owner-resolved identity tag mirroring the C
    /// `void *`.
    pub extra_data: u64,

    /* ----- filled in by the compute_stats routine (zero-initialized) ----- */
    /// `stats_valid`.
    pub stats_valid: bool,
    /// `stanullfrac` — fraction of entries that are NULL.
    pub stanullfrac: f32,
    /// `stawidth` — average width of column values.
    pub stawidth: i32,
    /// `stadistinct` — # distinct values.
    pub stadistinct: f32,
    /// `stakind[STATISTIC_NUM_SLOTS]`.
    pub stakind: [i16; STATISTIC_NUM_SLOTS],
    /// `staop[STATISTIC_NUM_SLOTS]`.
    pub staop: [Oid; STATISTIC_NUM_SLOTS],
    /// `stacoll[STATISTIC_NUM_SLOTS]`.
    pub stacoll: [Oid; STATISTIC_NUM_SLOTS],
    /// `numnumbers[STATISTIC_NUM_SLOTS]` — length of each `stanumbers[n]`.
    pub numnumbers: [i32; STATISTIC_NUM_SLOTS],
    /// `stanumbers[STATISTIC_NUM_SLOTS]` — owned mirror of `float4 *stanumbers[]`.
    pub stanumbers: [Vec<f32>; STATISTIC_NUM_SLOTS],
    /// `numvalues[STATISTIC_NUM_SLOTS]` — length of each `stavalues[n]`.
    pub numvalues: [i32; STATISTIC_NUM_SLOTS],
    /// `stavalues[STATISTIC_NUM_SLOTS]` — owned mirror of `Datum *stavalues[]`
    /// (the safe `'mcx` byte value lane).
    pub stavalues: [Vec<Datum<'mcx>>; STATISTIC_NUM_SLOTS],

    /* ----- describe the stavalues[n] element types ----- */
    /// `statypid[STATISTIC_NUM_SLOTS]`.
    pub statypid: [Oid; STATISTIC_NUM_SLOTS],
    /// `statyplen[STATISTIC_NUM_SLOTS]`.
    pub statyplen: [i16; STATISTIC_NUM_SLOTS],
    /// `statypbyval[STATISTIC_NUM_SLOTS]`.
    pub statypbyval: [bool; STATISTIC_NUM_SLOTS],
    /// `statypalign[STATISTIC_NUM_SLOTS]` (C `char`).
    pub statypalign: [i8; STATISTIC_NUM_SLOTS],

    /* ----- private to the main ANALYZE code ----- */
    /// `tupattnum` — attribute number within tuples.
    pub tupattnum: i32,
    /// `rows` — access info for the std fetch function (C `HeapTuple *rows`).
    ///
    /// The C `HeapTuple` is a fully materialized tuple (header + user-data area)
    /// the fetch routine reads attributes from via `heap_getattr`. The repo's
    /// header-only `HeapTuple` alias (`Option<PgBox<HeapTupleData>>`) cannot
    /// carry the user-data bytes `heap_getattr`/`nocachegetattr` need, so the
    /// reservoir holds the repo's real materialized-tuple carrier
    /// [`::types_tuple::FormedTuple`] (header + `data`), which is exactly what
    /// `ExecCopySlotHeapTuple` produces. This is the C-faithful carrier for
    /// `HeapTuple *rows` in this repo's value model; it is private to the main
    /// ANALYZE code (no other unit reads it).
    pub rows: Vec<FormedTuple<'mcx>>,
    /// `tupDesc` — tuple descriptor for `rows`.
    pub tup_desc: TupleDesc<'mcx>,
    /// `exprvals` — access info for the index fetch function (C `Datum *exprvals`).
    pub exprvals: Vec<Datum<'mcx>>,
    /// `exprnulls` — companion nulls for `exprvals` (C `bool *exprnulls`).
    pub exprnulls: Vec<bool>,
    /// `rowstride` — stride between rows in `exprvals`/`exprnulls`.
    pub rowstride: i32,
}

impl<'mcx> VacAttrStats<'mcx> {
    /// Build a minimal `VacAttrStats` carrying just the per-column type metadata
    /// the extended-statistics build kernels read (`attrtypid`, `attrtypmod`,
    /// `attrcollid`, `attstattarget`, plus the long-lived `anl_context`). In the
    /// C model the `StatsBuildData.stats[]` matrix simply aliases the live
    /// `VacAttrStats *` pointers; the owned `StatsBuildData.stats: Vec<...>`
    /// instead holds these copies, which is sufficient because the build kernels
    /// (`ndistinct_for_combination` / `dependency_degree` / `build_sorted_items`)
    /// only read these scalar fields. The heavy `tup_desc` / `rows` /
    /// per-slot output vectors are owned by the live ANALYZE matrix (used by the
    /// owner's `make_build_data` value extraction) and are left empty here.
    pub fn for_ext_build(
        attstattarget: i32,
        attrtypid: Oid,
        attrtypmod: i32,
        attrcollid: Oid,
        anl_context: Option<Mcx<'mcx>>,
    ) -> Self {
        VacAttrStats {
            attstattarget,
            attrtypid,
            attrtypmod,
            attrtype: None,
            attrcollid,
            anl_context,
            compute_stats: None,
            minrows: 0,
            extra_data: 0,
            stats_valid: false,
            stanullfrac: 0.0,
            stawidth: 0,
            stadistinct: 0.0,
            stakind: [0; STATISTIC_NUM_SLOTS],
            staop: [0; STATISTIC_NUM_SLOTS],
            stacoll: [0; STATISTIC_NUM_SLOTS],
            numnumbers: [0; STATISTIC_NUM_SLOTS],
            stanumbers: Default::default(),
            numvalues: [0; STATISTIC_NUM_SLOTS],
            stavalues: Default::default(),
            statypid: [0; STATISTIC_NUM_SLOTS],
            statyplen: [0; STATISTIC_NUM_SLOTS],
            statypbyval: [false; STATISTIC_NUM_SLOTS],
            statypalign: [0; STATISTIC_NUM_SLOTS],
            tupattnum: 0,
            rows: Vec::new(),
            tup_desc: None,
            exprvals: Vec::new(),
            exprnulls: Vec::new(),
            rowstride: 0,
        }
    }
}

/// `StatsBuildData` (`statistics/extended_stats_internal.h:61`) — a unified
/// representation of the sampled data the extended statistics is built on.
///
/// ```c
/// typedef struct StatsBuildData {
///     int            numrows;
///     int            nattnums;
///     AttrNumber    *attnums;
///     VacAttrStats **stats;
///     Datum        **values;
///     bool         **nulls;
/// } StatsBuildData;
/// ```
///
/// The owned mirror replaces the C pointer-of-pointer arrays with owned `Vec`s:
///
///   * `attnums` -> `Vec<AttrNumber>` (length `nattnums`);
///   * `stats` (the `VacAttrStats *` matrix) -> `Vec<VacAttrStats<'mcx>>`
///     (one per analyzed column, length `nattnums`);
///   * `values` / `nulls` (the `nattnums`-by-`numrows` value/null matrices) ->
///     `Vec<Vec<Datum>>` / `Vec<Vec<bool>>` (outer length `nattnums`, each inner
///     length `numrows`).
///
/// The build-side seams (`statext_mcv_build` / `dependency_degree` /
/// `ndistinct_for_combination`) take this carrier by reference; their bodies
/// (multi-sort support, `lookup_type_cache(...)->lt_opr`, `build_sorted_items`)
/// are filled by the ANALYZE owner when it lands.
pub struct StatsBuildData<'mcx> {
    /// `numrows` — number of sampled rows.
    pub numrows: i32,
    /// `nattnums` — number of analyzed columns/expressions.
    pub nattnums: i32,
    /// `attnums` — analyzed attribute numbers (length `nattnums`).
    pub attnums: Vec<AttrNumber>,
    /// `stats` — per-column `VacAttrStats` (length `nattnums`).
    pub stats: Vec<VacAttrStats<'mcx>>,
    /// `values` — per-column sampled value arrays (outer `nattnums`, inner `numrows`).
    pub values: Vec<Vec<Datum<'mcx>>>,
    /// `nulls` — per-column sampled null flags (outer `nattnums`, inner `numrows`).
    pub nulls: Vec<Vec<bool>>,
}

/// `ArrayAnalyzeExtraData` (`utils/adt/array_typanalyze.c:36`) — the
/// element-type metadata `array_typanalyze` gathers from the type cache for
/// `compute_array_stats`.
///
/// The C struct carries `FmgrInfo *cmp` / `FmgrInfo *hash` (pointers into the
/// element type's long-lived typcache entry) plus the saved
/// `std_compute_stats` / `std_extra_data` from `std_typanalyze`. In this repo's
/// value model:
///
///   * `cmp` / `hash` carry the support functions' **proc OIDs** (not
///     `FmgrInfo`): the fmgr `FunctionCall1Coll` / `FunctionCall2Coll`
///     invocations are routed through owner seams that build a fresh
///     `FmgrInfo` from the OID, mirroring the rangetypes approach of seaming
///     fmgr calls by the owner;
///   * the saved standard `compute_stats` routine is reached through the
///     `std_compute_stats` analyze-seam (it lives in the unported analyze.c),
///     so `std_compute_stats` / `std_extra_data` are not carried here.
///
/// It lives in this `types-statistics` crate (which already hosts
/// [`VacAttrStats`]) so both the array-typanalyze leaf and the typcache-seam
/// installer can name it without a dependency cycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArrayAnalyzeExtraData {
    /// element type's OID (`type_id`).
    pub type_id: Oid,
    /// default equality operator's OID (`eq_opr`).
    pub eq_opr: Oid,
    /// collation to use (`coll_id`).
    pub coll_id: Oid,
    /// physical properties of element type (`typbyval`).
    pub typbyval: bool,
    /// `typlen`.
    pub typlen: i16,
    /// `typalign` (C `char`).
    pub typalign: i8,
    /// the element type's comparison support function OID (C `FmgrInfo *cmp`).
    pub cmp: Oid,
    /// the element type's hash support function OID (C `FmgrInfo *hash`).
    pub hash: Oid,
}

/// `MVDependency` (`statistics/statistics.h`).
///
/// C is a flexible-array struct:
/// ```c
/// typedef struct MVDependency {
///     double      degree;          /* degree of validity (0-1) */
///     AttrNumber  nattributes;     /* number of attributes */
///     AttrNumber  attributes[FLEXIBLE_ARRAY_MEMBER]; /* attribute numbers */
/// } MVDependency;
/// ```
/// The owned mirror replaces the FAM with an owned `Vec`; the invariant
/// `attributes.len() == nattributes` is upheld by the (de)serializers.
#[derive(Clone, Debug, PartialEq)]
pub struct MVDependency {
    /// degree of validity (0-1)
    pub degree: f64,
    /// number of attributes
    pub nattributes: AttrNumber,
    /// attribute numbers
    pub attributes: Vec<AttrNumber>,
}

/// `MVDependencies` (`statistics/statistics.h`).
///
/// C is a flexible-array struct of pointers:
/// ```c
/// typedef struct MVDependencies {
///     uint32          magic;       /* magic constant marker */
///     uint32          type;        /* type of MV Dependencies (BASIC) */
///     uint32          ndeps;       /* number of dependencies */
///     MVDependency   *deps[FLEXIBLE_ARRAY_MEMBER]; /* dependencies */
/// } MVDependencies;
/// ```
/// The owned mirror replaces the FAM of pointers with an owned
/// `Vec<Box<MVDependency>>`; the invariant `deps.len() == ndeps` is upheld by
/// the (de)serializers and the build loop.
#[derive(Clone, Debug, PartialEq)]
pub struct MVDependencies {
    /// magic constant marker
    pub magic: u32,
    /// type of MV Dependencies (BASIC)
    pub r#type: u32,
    /// number of dependencies
    pub ndeps: u32,
    /// dependencies (boxed, mirroring the C array of pointers)
    pub deps: Vec<Box<MVDependency>>,
}

/// `MVNDistinctItem` (`statistics/statistics.h`).
///
/// C is a flexible-array struct:
/// ```c
/// typedef struct MVNDistinctItem {
///     double      ndistinct;       /* ndistinct value for this combination */
///     int         nattributes;     /* number of attributes */
///     AttrNumber *attributes;      /* attribute numbers */
/// } MVNDistinctItem;
/// ```
/// (In C the `attributes` are `palloc`'d separately, not a FAM.) The owned mirror
/// replaces the pointer with an owned `Vec`; the invariant `attributes.len() ==
/// nattributes` is upheld by the (de)serializers and the build loop.
#[derive(Clone, Debug, PartialEq)]
pub struct MVNDistinctItem {
    /// ndistinct value for this combination
    pub ndistinct: f64,
    /// attribute numbers (`nattributes == attributes.len()`)
    pub attributes: Vec<AttrNumber>,
}

/// `MVNDistinct` (`statistics/statistics.h`).
///
/// C is a flexible-array struct:
/// ```c
/// typedef struct MVNDistinct {
///     uint32              magic;   /* magic constant marker */
///     uint32              type;    /* type of ndistinct (BASIC) */
///     uint32              nitems;  /* number of items in the statistic */
///     MVNDistinctItem     items[FLEXIBLE_ARRAY_MEMBER]; /* array of items */
/// } MVNDistinct;
/// ```
/// The owned mirror replaces the FAM with an owned `Vec`; the invariant
/// `items.len() == nitems` is upheld by the (de)serializers and the build loop.
#[derive(Clone, Debug, PartialEq)]
pub struct MVNDistinct {
    /// magic constant marker
    pub magic: u32,
    /// type of ndistinct (BASIC)
    pub r#type: u32,
    /// array of items (length is the C `nitems`)
    pub items: Vec<MVNDistinctItem>,
}
