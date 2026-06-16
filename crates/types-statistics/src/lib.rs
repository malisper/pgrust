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
use types_core::AttrNumber;

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
