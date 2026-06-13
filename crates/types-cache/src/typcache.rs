//! Type-cache entry vocabulary (`utils/typcache.h`), trimmed to the
//! range/multirange selectivity consumers.
//!
//! The C `TypeCacheEntry` caches per-type metadata. The range selectivity
//! estimators read only the range-support fields plus the element/range
//! sub-entries; everything else in the C struct is omitted until a consumer
//! needs it.

use types_core::fmgr::FmgrInfo;
use types_core::primitive::Oid;

extern crate alloc;
use alloc::boxed::Box;

/// `TypeCacheEntry` (`typcache.h`), trimmed. For a range type
/// `rng_cmp_proc_finfo` / `rng_subdiff_finfo` are the subtype's `cmp` /
/// `subdiff` support functions, `rng_collation` the collation passed to them,
/// and `rngelemtype` the element type's entry. For a multirange type `rngtype`
/// points at the corresponding range type's entry.
#[derive(Clone, Debug, Default)]
pub struct TypeCacheEntry {
    /// `type_id` -- the type's own OID.
    pub type_id: Oid,
    /// `rng_collation` -- collation for the range's comparison/subdiff calls.
    pub rng_collation: Oid,
    /// `rng_cmp_proc_finfo` -- the subtype's `cmp` support function.
    pub rng_cmp_proc_finfo: FmgrInfo,
    /// `rng_subdiff_finfo` -- the subtype's optional `subdiff` support function
    /// (`fn_oid == InvalidOid` when absent).
    pub rng_subdiff_finfo: FmgrInfo,
    /// `rngelemtype` -- the range element type's cache entry (range types only).
    pub rngelemtype: Option<Box<TypeCacheEntry>>,
    /// `rngtype` -- the range type's cache entry (multirange types only).
    pub rngtype: Option<Box<TypeCacheEntry>>,
}
