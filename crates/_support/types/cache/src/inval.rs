//! Invalidation callback vocabulary (`utils/inval.h`).

use types_core::Oid;

// Bare-word machine-word `Datum` (`datum::Datum`), aliased `ScalarWord`.
// The cache-invalidation callback `arg` is a plain machine word that C passes
// as `(Datum) 0` and hands back verbatim; it carries no deformed value, so it
// stays the audited bare word rather than the canonical `Datum<'mcx>` enum.
use datum::Datum as ScalarWord;

/// `SyscacheCallbackFunction` — `void (*)(Datum arg, int cacheid, uint32
/// hashvalue)`. By convention a zero `hashvalue` means "flush the whole
/// cache" (see inval.c / `InvalidateSystemCachesExtended`).
pub type SyscacheCallbackFunction = fn(arg: ScalarWord, cacheid: i32, hashvalue: u32);

/// `RelcacheCallbackFunction` — `void (*)(Datum arg, Oid relid)`. An
/// `InvalidOid` relid signals a complete reset.
pub type RelcacheCallbackFunction = fn(arg: ScalarWord, relid: Oid);
