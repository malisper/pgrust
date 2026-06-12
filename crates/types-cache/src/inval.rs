//! Invalidation callback vocabulary (`utils/inval.h`).

use types_core::Oid;
use types_datum::Datum;

/// `SyscacheCallbackFunction` — `void (*)(Datum arg, int cacheid, uint32
/// hashvalue)`. By convention a zero `hashvalue` means "flush the whole
/// cache" (see inval.c / `InvalidateSystemCachesExtended`).
pub type SyscacheCallbackFunction = fn(arg: Datum, cacheid: i32, hashvalue: u32);

/// `RelcacheCallbackFunction` — `void (*)(Datum arg, Oid relid)`. An
/// `InvalidOid` relid signals a complete reset.
pub type RelcacheCallbackFunction = fn(arg: Datum, relid: Oid);
