//! Catalog-cache access vocabulary, trimmed to what the cache ports consume:
//! the syscache/catcache search-key currency (`utils/catcache.h`), the
//! invalidation callback types (`utils/inval.h`), and the equality scan-key
//! initializer record (`access/skey.h` `ScanKeyInit`, value form).

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

pub mod deflist;
pub mod inval;
pub mod skey;
pub mod syscache;
pub mod typcache;

pub use deflist::*;
pub use inval::*;
pub use skey::*;
pub use syscache::*;
pub use typcache::TypeCacheEntry;
