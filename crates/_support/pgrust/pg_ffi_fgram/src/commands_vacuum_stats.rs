//! ANALYZE / VACUUM statistics ABI vocabulary.
//!
//! These `#[repr(C)]` structs and constants cross the boundary between
//! `backend-commands-analyze` (the ANALYZE driver + standard typanalyze
//! routines) and the rest of the backend.  They mirror the C definitions in:
//!   * `src/include/commands/vacuum.h`
//!     (`VacAttrStats`, `VacAttrStatsP`, `AnalyzeAttrFetchFunc`,
//!      `AnalyzeAttrComputeStatsFunc`)
//!   * `src/include/catalog/pg_statistic.h` / `pg_statistic_d.h`
//!     (`STATISTIC_NUM_SLOTS`, `STATISTIC_KIND_*`, `Anum_pg_statistic_*`,
//!      `Natts_pg_statistic`)
//!   * `src/include/utils/sampling.h`
//!     (`BlockSamplerData`, `ReservoirStateData`)
//!   * `src/include/common/prng.h` (`pg_prng_state`)

use crate::{
    AttrNumber, BlockNumber, Datum, Form_pg_type, HeapTuple, MemoryContext, Oid, TupleDesc,
};

/* ---------------------------------------------------------------------------
 * pg_statistic.h — slot constants
 * ------------------------------------------------------------------------- */

/// `STATISTIC_NUM_SLOTS` (`catalog/pg_statistic.h:127`).
pub const STATISTIC_NUM_SLOTS: usize = 5;

/// `STATISTIC_KIND_MCV` (`catalog/pg_statistic.h:190`) — most-common-values slot.
pub const STATISTIC_KIND_MCV: i16 = 1;
/// `STATISTIC_KIND_HISTOGRAM` (`catalog/pg_statistic.h:210`) — histogram slot.
pub const STATISTIC_KIND_HISTOGRAM: i16 = 2;
/// `STATISTIC_KIND_CORRELATION` (`catalog/pg_statistic.h:222`) — correlation slot.
pub const STATISTIC_KIND_CORRELATION: i16 = 3;
/// `STATISTIC_KIND_MCELEM` (`catalog/pg_statistic.h:247`) — most-common-elements slot.
pub const STATISTIC_KIND_MCELEM: i16 = 4;
/// `STATISTIC_KIND_DECHIST` (`catalog/pg_statistic.h:261`) — distinct-elements histogram.
pub const STATISTIC_KIND_DECHIST: i16 = 5;
/// `STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM` (`catalog/pg_statistic.h:273`).
pub const STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM: i16 = 6;
/// `STATISTIC_KIND_BOUNDS_HISTOGRAM` (`catalog/pg_statistic.h:284`).
pub const STATISTIC_KIND_BOUNDS_HISTOGRAM: i16 = 7;

/* ---------------------------------------------------------------------------
 * pg_statistic_d.h — attribute numbers
 * ------------------------------------------------------------------------- */

/// `Natts_pg_statistic` (`pg_statistic_d.h`).
pub const Natts_pg_statistic: usize = 31;

pub const Anum_pg_statistic_starelid: i32 = 1;
pub const Anum_pg_statistic_staattnum: i32 = 2;
pub const Anum_pg_statistic_stainherit: i32 = 3;
pub const Anum_pg_statistic_stanullfrac: i32 = 4;
pub const Anum_pg_statistic_stawidth: i32 = 5;
pub const Anum_pg_statistic_stadistinct: i32 = 6;
pub const Anum_pg_statistic_stakind1: i32 = 7;
pub const Anum_pg_statistic_stakind2: i32 = 8;
pub const Anum_pg_statistic_stakind3: i32 = 9;
pub const Anum_pg_statistic_stakind4: i32 = 10;
pub const Anum_pg_statistic_stakind5: i32 = 11;
pub const Anum_pg_statistic_staop1: i32 = 12;
pub const Anum_pg_statistic_staop2: i32 = 13;
pub const Anum_pg_statistic_staop3: i32 = 14;
pub const Anum_pg_statistic_staop4: i32 = 15;
pub const Anum_pg_statistic_staop5: i32 = 16;
pub const Anum_pg_statistic_stacoll1: i32 = 17;
pub const Anum_pg_statistic_stacoll2: i32 = 18;
pub const Anum_pg_statistic_stacoll3: i32 = 19;
pub const Anum_pg_statistic_stacoll4: i32 = 20;
pub const Anum_pg_statistic_stacoll5: i32 = 21;
pub const Anum_pg_statistic_stanumbers1: i32 = 22;
pub const Anum_pg_statistic_stanumbers2: i32 = 23;
pub const Anum_pg_statistic_stanumbers3: i32 = 24;
pub const Anum_pg_statistic_stanumbers4: i32 = 25;
pub const Anum_pg_statistic_stanumbers5: i32 = 26;
pub const Anum_pg_statistic_stavalues1: i32 = 27;
pub const Anum_pg_statistic_stavalues2: i32 = 28;
pub const Anum_pg_statistic_stavalues3: i32 = 29;
pub const Anum_pg_statistic_stavalues4: i32 = 30;
pub const Anum_pg_statistic_stavalues5: i32 = 31;

/// `Anum_pg_attribute_attstattarget` (`pg_attribute_d.h`).
pub const Anum_pg_attribute_attstattarget: i32 = 21;

/* ---------------------------------------------------------------------------
 * vacuum.h — VacAttrStats and the analyze callback function pointers
 * ------------------------------------------------------------------------- */

/// `VacAttrStatsP` — pointer to a [`VacAttrStats`] (`commands/vacuum.h:106`).
pub type VacAttrStatsP = *mut VacAttrStats;

/// `AnalyzeAttrFetchFunc` (`commands/vacuum.h:108`).
///
/// The C signature is `Datum (*)(VacAttrStatsP, int rownum, bool *isNull)`.
/// Modeled as a Rust function pointer because both the producers (the analyze
/// fetch funcs) and consumers (the `compute_stats` routines) live entirely in
/// `backend-commands-analyze`.
pub type AnalyzeAttrFetchFunc =
    unsafe fn(stats: VacAttrStatsP, rownum: i32, isNull: *mut bool) -> Datum;

/// `AnalyzeAttrComputeStatsFunc` (`commands/vacuum.h:111`).
///
/// The C signature is
/// `void (*)(VacAttrStatsP, AnalyzeAttrFetchFunc, int samplerows, double totalrows)`.
pub type AnalyzeAttrComputeStatsFunc = unsafe fn(
    stats: VacAttrStatsP,
    fetchfunc: AnalyzeAttrFetchFunc,
    samplerows: i32,
    totalrows: f64,
);

/// `VacAttrStats` (`commands/vacuum.h:116`) — per-column working state for
/// ANALYZE.  Layout matches the C struct field-for-field.
#[repr(C)]
pub struct VacAttrStats {
    /* set up by main ANALYZE code before typanalyze */
    /// -1 to use default.
    pub attstattarget: i32,
    /// type of data being analyzed.
    pub attrtypid: Oid,
    /// typmod of data being analyzed.
    pub attrtypmod: i32,
    /// copy of pg_type row for attrtypid.
    pub attrtype: Form_pg_type,
    /// collation of data being analyzed.
    pub attrcollid: Oid,
    /// where to save long-lived data.
    pub anl_context: MemoryContext,

    /* filled in by the typanalyze routine, unless it returns false */
    /// function pointer.
    pub compute_stats: Option<AnalyzeAttrComputeStatsFunc>,
    /// Minimum # of rows wanted for stats.
    pub minrows: i32,
    /// for extra type-specific data.
    pub extra_data: *mut core::ffi::c_void,

    /* filled in by the compute_stats routine (zero-initialized) */
    pub stats_valid: bool,
    /// fraction of entries that are NULL.
    pub stanullfrac: f32,
    /// average width of column values.
    pub stawidth: i32,
    /// # distinct values.
    pub stadistinct: f32,
    pub stakind: [i16; STATISTIC_NUM_SLOTS],
    pub staop: [Oid; STATISTIC_NUM_SLOTS],
    pub stacoll: [Oid; STATISTIC_NUM_SLOTS],
    pub numnumbers: [i32; STATISTIC_NUM_SLOTS],
    pub stanumbers: [*mut f32; STATISTIC_NUM_SLOTS],
    pub numvalues: [i32; STATISTIC_NUM_SLOTS],
    pub stavalues: [*mut Datum; STATISTIC_NUM_SLOTS],

    /* describe the stavalues[n] element types */
    pub statypid: [Oid; STATISTIC_NUM_SLOTS],
    pub statyplen: [i16; STATISTIC_NUM_SLOTS],
    pub statypbyval: [bool; STATISTIC_NUM_SLOTS],
    pub statypalign: [core::ffi::c_char; STATISTIC_NUM_SLOTS],

    /* private to the main ANALYZE code */
    /// attribute number within tuples.
    pub tupattnum: i32,
    /// access info for std fetch function.
    pub rows: *mut HeapTuple,
    pub tupDesc: TupleDesc,
    /// access info for index fetch function.
    pub exprvals: *mut Datum,
    pub exprnulls: *mut bool,
    pub rowstride: i32,
}

/* ---------------------------------------------------------------------------
 * common/prng.h — pg_prng_state
 * ------------------------------------------------------------------------- */

/// `pg_prng_state` (`common/prng.h`) — xoroshiro128** generator state.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct pg_prng_state {
    pub s0: u64,
    pub s1: u64,
}

/* ---------------------------------------------------------------------------
 * utils/sampling.h — block / reservoir sampler state
 * ------------------------------------------------------------------------- */

/// `BlockSamplerData` (`utils/sampling.h`) — Algorithm S from Knuth 3.4.2.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BlockSamplerData {
    /// number of blocks, known in advance.
    pub N: BlockNumber,
    /// desired sample size.
    pub n: i32,
    /// current block number.
    pub t: BlockNumber,
    /// blocks selected so far.
    pub m: i32,
    /// random generator state.
    pub randstate: pg_prng_state,
}

/// `BlockSampler` — pointer alias for [`BlockSamplerData`].
pub type BlockSampler = *mut BlockSamplerData;

/// `ReservoirStateData` (`utils/sampling.h`) — Vitter reservoir sampler state.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ReservoirStateData {
    pub W: f64,
    /// random generator state.
    pub randstate: pg_prng_state,
}

/// `ReservoirState` — pointer alias for [`ReservoirStateData`].
pub type ReservoirState = *mut ReservoirStateData;

/// `AttrNumber` re-export convenience alias (already in `types`); kept here so
/// downstream `use crate::commands_vacuum_stats::*` sees the same name.
pub type AnalyzeAttrNumber = AttrNumber;

/* ---------------------------------------------------------------------------
 * analyze.c seam value helpers
 *
 * Small by-value carriers used by the `backend-commands-analyze` runtime seam
 * to project a few fields out of opaque catalog structures (so the analyze
 * driver can stay 1:1 with the C control flow without owning relcache layout).
 * ------------------------------------------------------------------------- */

/// Result of `GetFdwRoutineForRelation(rel)->AnalyzeForeignTable(rel, ...)`:
/// whether ANALYZE is supported, an opaque tag identifying the FDW's
/// row-acquisition function, and the foreign-table page count.
#[derive(Clone, Copy, Debug, Default)]
pub struct FdwAnalyzeResult {
    /// `ok` out-parameter from `AnalyzeForeignTable`.
    pub ok: bool,
    /// Opaque dispatch tag for the returned `AcquireSampleRowsFunc`.
    pub acquirefunc_tag: u32,
    /// `relpages` out-parameter.
    pub relpages: BlockNumber,
}

/// The `IndexInfo` fields consulted in `do_analyze_rel` / `compute_index_stats`
/// for index-expression statistics.
#[derive(Clone, Copy, Debug, Default)]
pub struct AnlIndexExprInfo {
    /// `indexInfo->ii_NumIndexAttrs`.
    pub ii_num_index_attrs: i32,
    /// `indexInfo->ii_Expressions != NIL`.
    pub has_expressions: bool,
    /// `indexInfo->ii_Predicate != NIL`.
    pub has_predicate: bool,
}

/// The `Form_pg_attribute` fields consulted in `examine_attribute`.
#[derive(Clone, Copy, Debug)]
pub struct AnlAttrInfo {
    /// `attr->attisdropped`.
    pub attisdropped: bool,
    /// `attr->attgenerated`.
    pub attgenerated: core::ffi::c_char,
    /// `attr->atttypid`.
    pub atttypid: Oid,
    /// `attr->atttypmod`.
    pub atttypmod: i32,
    /// `attr->attcollation`.
    pub attcollation: Oid,
}

impl Default for AnlAttrInfo {
    fn default() -> Self {
        Self {
            attisdropped: false,
            attgenerated: 0,
            atttypid: 0,
            atttypmod: -1,
            attcollation: 0,
        }
    }
}
