//! Catalog-cache access vocabulary, trimmed to what the cache ports consume:
//! the syscache/catcache search-key currency (`utils/catcache.h`) and the
//! invalidation callback types (`utils/inval.h`).
//!
//! The scan-key vocabulary the cache scans use lives in the access layer:
//! the `ScanKeyData` carrier and `BTEqualStrategyNumber` in
//! `types_scan::scankey`, the comparison-proc OIDs (`F_OIDEQ`) in
//! `types_core::fmgr`, and the `ScanKeyInit` initializer in
//! `backend-access-common-scankey`. The former value-form `ScanKeyInit`
//! record here is gone; `F_OIDEQ` / `BTEqualStrategyNumber` are re-exported
//! from their canonical homes so existing `types_cache::{F_OIDEQ,
//! BTEqualStrategyNumber}` paths keep resolving.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod backend_utils_cache_catcache;
pub mod deflist;
pub mod inval;
pub mod syscache;
pub mod typcache;

pub use deflist::*;
pub use inval::*;
pub use syscache::*;
pub use typcache::TypeCacheEntry;

pub use types_core::fmgr::F_OIDEQ;
pub use types_scan::scankey::BTEqualStrategyNumber;
