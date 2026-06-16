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
use types_core::{AttrNumber, Oid};
use types_datum::Datum;

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
pub struct MCVItem {
    /// frequency of this combination
    pub frequency: f64,
    /// frequency if independent
    pub base_frequency: f64,
    /// NULL flags
    pub isnull: Vec<bool>,
    /// item values
    pub values: Vec<Datum>,
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
pub struct MCVList {
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
    pub items: Vec<MCVItem>,
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
pub struct SortItem {
    pub values: Vec<Datum>,
    pub isnull: Vec<bool>,
    pub count: i32,
}

/// Opaque handle to a `StatsBuildData` (`statistics/extended_stats_internal.h`).
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
/// The `VacAttrStats` matrix and the `Datum`/`bool` value matrices live in the
/// not-yet-ported vacuum / multi-sort subsystem (the combined
/// `backend-statistics-core` unit). The functional-dependency builder cannot
/// dereference them: it reaches the build data by this identity handle and the
/// owner-side seam (`dependency_degree`) does the per-column sort/group work.
/// This is the idiomatic stand-in for the C `StatsBuildData *` pointer, NOT a
/// transcribed byte blob.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StatsBuildDataHandle(pub u64);

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
