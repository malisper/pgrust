//! Extended-statistics ABI vocabulary.
//!
//! These `#[repr(C)]` structs and constants cross the boundary between the
//! rewritten statistics crates (`backend-statistics-stat-utils`,
//! `-mvdistinct`, `-dependencies`, `-mcv`, `-extended-stats`,
//! `-attribute-stats`, `-relation-stats`) and the rest of the backend.  They
//! mirror the C definitions in
//!   * `src/include/statistics/statistics.h`
//!     (MVNDistinct/MVNDistinctItem, MVDependencies/MVDependency,
//!      MCVList/MCVItem, magic/type/dimension constants)
//!   * `src/include/statistics/extended_stats_internal.h`
//!     (StdAnalyzeData, ScalarItem, DimensionInfo, MultiSortSupportData,
//!      SortItem, StatsBuildData)
//!   * `src/include/nodes/pathnodes.h`  (StatisticExtInfo)
//!
//! Layout-critical fields keep their exact C order/width; flexible array
//! members are modeled as zero-length arrays (`[T; 0]`), matching the
//! workspace convention.  Catalog-side sub-objects not yet modeled in this
//! workspace are held as pointer-width opaque handles, ABI-identical to the C
//! pointers they stand in for.

use core::ffi::c_void;

use crate::sortsupport::SortSupportData;
use crate::{AttrNumber, Datum, NodeTag, Oid};

/* ---------------------------------------------------------------------------
 * statistics.h
 * ------------------------------------------------------------------------- */

/// `STATS_MAX_DIMENSIONS` — max number of attributes in an extended statistic.
pub const STATS_MAX_DIMENSIONS: usize = 8;

/* Multivariate distinct coefficients */
/// `STATS_NDISTINCT_MAGIC` — struct identifier for serialized ndistinct.
pub const STATS_NDISTINCT_MAGIC: u32 = 0xA352BFA4;
/// `STATS_NDISTINCT_TYPE_BASIC` — struct version (BASIC).
pub const STATS_NDISTINCT_TYPE_BASIC: u32 = 1;

/// `MVNDistinctItem` — a single combination of columns.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MVNDistinctItem {
    /// ndistinct value for this combination.
    pub ndistinct: f64,
    /// number of attributes.
    pub nattributes: i32,
    /// attribute numbers.
    pub attributes: *mut AttrNumber,
}

/// `MVNDistinct` — all possible combinations of columns.
#[repr(C)]
pub struct MVNDistinct {
    /// magic constant marker.
    pub magic: u32,
    /// type of ndistinct (BASIC).
    pub r#type: u32,
    /// number of items in the statistic.
    pub nitems: u32,
    /// `items[FLEXIBLE_ARRAY_MEMBER]`.
    pub items: [MVNDistinctItem; 0],
}

/* Multivariate functional dependencies */
/// `STATS_DEPS_MAGIC` — marks serialized bytea.
pub const STATS_DEPS_MAGIC: u32 = 0xB4549A2C;
/// `STATS_DEPS_TYPE_BASIC` — basic dependencies type.
pub const STATS_DEPS_TYPE_BASIC: u32 = 1;

/// `MVDependency` — functional dependency (values in one column determine
/// values in another one).
#[repr(C)]
pub struct MVDependency {
    /// degree of validity (0-1).
    pub degree: f64,
    /// number of attributes.
    pub nattributes: AttrNumber,
    /// `attributes[FLEXIBLE_ARRAY_MEMBER]` — attribute numbers.
    pub attributes: [AttrNumber; 0],
}

/// `MVDependencies` — collection of functional dependencies.
#[repr(C)]
pub struct MVDependencies {
    /// magic constant marker.
    pub magic: u32,
    /// type of MV Dependencies (BASIC).
    pub r#type: u32,
    /// number of dependencies.
    pub ndeps: u32,
    /// `deps[FLEXIBLE_ARRAY_MEMBER]` — dependencies.
    pub deps: [*mut MVDependency; 0],
}

/* Multivariate MCV lists */
/// `STATS_MCV_MAGIC` — marks serialized bytea.
pub const STATS_MCV_MAGIC: u32 = 0xE1A651C2;
/// `STATS_MCV_TYPE_BASIC` — basic MCV list type.
pub const STATS_MCV_TYPE_BASIC: u32 = 1;

/// `MAX_STATISTICS_TARGET` (`commands/vacuum.h`).
pub const MAX_STATISTICS_TARGET: i32 = 10000;
/// `STATS_MCVLIST_MAX_ITEMS` — max items in an MCV list (`= MAX_STATISTICS_TARGET`).
pub const STATS_MCVLIST_MAX_ITEMS: i32 = MAX_STATISTICS_TARGET;

/// `MCVItem` — one most-common-value combination.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MCVItem {
    /// frequency of this combination.
    pub frequency: f64,
    /// frequency if independent.
    pub base_frequency: f64,
    /// NULL flags.
    pub isnull: *mut bool,
    /// item values.
    pub values: *mut Datum,
}

/// `MCVList` — array of MCV items.
#[repr(C)]
pub struct MCVList {
    /// magic constant marker.
    pub magic: u32,
    /// type of MCV list (BASIC).
    pub r#type: u32,
    /// number of MCV items in the array.
    pub nitems: u32,
    /// number of dimensions.
    pub ndimensions: AttrNumber,
    /// OIDs of data types.
    pub types: [Oid; STATS_MAX_DIMENSIONS],
    /// `items[FLEXIBLE_ARRAY_MEMBER]` — array of MCV items.
    pub items: [MCVItem; 0],
}

/* ---------------------------------------------------------------------------
 * pathnodes.h — StatisticExtInfo
 * ------------------------------------------------------------------------- */

/// `StatisticExtInfo` — extended-statistics metadata for the planner
/// (`pathnodes.h`). Canonical definition lives in [`crate::pathnodes`] (its `rel`
/// field is the faithful `*mut RelOptInfo` per `pathnodes.h`, not an opaque
/// pointer); re-exported here.
pub use crate::pathnodes::StatisticExtInfo;

/* ---------------------------------------------------------------------------
 * extended_stats_internal.h
 * ------------------------------------------------------------------------- */

/// `StdAnalyzeData` — opclass operators/functions for a base type.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct StdAnalyzeData {
    /// '=' operator for datatype, if any.
    pub eqopr: Oid,
    /// and associated function.
    pub eqfunc: Oid,
    /// '<' operator for datatype, if any.
    pub ltopr: Oid,
}

/// `ScalarItem` — one analyzed data value plus its tuple index.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ScalarItem {
    /// a data value.
    pub value: Datum,
    /// position index for tuple it came from.
    pub tupno: i32,
}

/// `DimensionInfo` — (de)serialization info for one dimension.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DimensionInfo {
    /// number of deduplicated values.
    pub nvalues: i32,
    /// number of bytes (serialized).
    pub nbytes: i32,
    /// size of deserialized data with alignment.
    pub nbytes_aligned: i32,
    /// pg_type.typlen.
    pub typlen: i32,
    /// pg_type.typbyval.
    pub typbyval: bool,
}

/// `MultiSortSupportData` — sort support across multiple dimensions.
#[repr(C)]
pub struct MultiSortSupportData {
    /// number of dimensions.
    pub ndims: i32,
    /// `ssup[FLEXIBLE_ARRAY_MEMBER]` — sort support data for each dimension.
    pub ssup: [SortSupportData; 0],
}

/// `MultiSortSupport` — pointer alias for `MultiSortSupportData`.
pub type MultiSortSupport = *mut MultiSortSupportData;

/// `SortItem` — a row of values+nulls with a multiplicity count.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SortItem {
    pub values: *mut Datum,
    pub isnull: *mut bool,
    pub count: i32,
}

/// `StatsBuildData` — unified representation of the data statistics is built
/// on.  `stats` is an array of `VacAttrStats *` (held as opaque handles, since
/// vacuum's `VacAttrStats` is not yet modeled here).
#[repr(C)]
pub struct StatsBuildData {
    pub numrows: i32,
    pub nattnums: i32,
    pub attnums: *mut AttrNumber,
    /// `VacAttrStats **stats`.
    pub stats: *mut *mut c_void,
    /// `Datum **values`.
    pub values: *mut *mut Datum,
    /// `bool **nulls`.
    pub nulls: *mut *mut bool,
}
