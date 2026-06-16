//! `enum SysCacheIdentifier` members (`catalog/syscache_ids.h`).

/// `SysCacheIdentifier` — the cache-id argument of `SearchSysCache*` and
/// `CacheRegisterSyscacheCallback`.
pub type SysCacheIdentifier = i32;

/// `AMOPOPID`
pub const AMOPOPID: SysCacheIdentifier = 3;
/// `AUTHMEMROLEMEM`
pub const AUTHMEMROLEMEM: SysCacheIdentifier = 9;
/// `AUTHOID`
pub const AUTHOID: SysCacheIdentifier = 11;
/// `CONSTROID`
pub const CONSTROID: SysCacheIdentifier = 19;
/// `DATABASEOID`
pub const DATABASEOID: SysCacheIdentifier = 21;
/// `NAMESPACEOID`
pub const NAMESPACEOID: SysCacheIdentifier = 38;
/// `RELOID`
pub const RELOID: SysCacheIdentifier = 57;
/// `RULERELNAME`
pub const RULERELNAME: SysCacheIdentifier = 60;
