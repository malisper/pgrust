//! Constants from `src/include/access/hash.h` (PostgreSQL 18.3), trimmed to the
//! items the hash opclass validator consumes.

use types_core::uint16;

/// `StrategyNumber` (`access/stratnum.h`) — `typedef uint16 StrategyNumber`.
pub type StrategyNumber = uint16;

// ---------------------------------------------------------------------------
// Strategy numbers (`access/hash.h` via `access/stratnum.h`).
// ---------------------------------------------------------------------------

/// `HTEqualStrategyNumber` — the hash AM's only strategy (`=`).
pub const HTEqualStrategyNumber: StrategyNumber = 1;
/// `HTMaxStrategyNumber` — one strategy in total.
pub const HTMaxStrategyNumber: StrategyNumber = 1;

// ---------------------------------------------------------------------------
// Support-function numbers (hash.h).
// ---------------------------------------------------------------------------

/// `HASHSTANDARD_PROC` — the standard (32-bit) hash function.
pub const HASHSTANDARD_PROC: uint16 = 1;
/// `HASHEXTENDED_PROC` — the optional extended (64-bit, salted) hash function.
pub const HASHEXTENDED_PROC: uint16 = 2;
/// `HASHOPTIONS_PROC` — the optional opclass-options support function.
pub const HASHOPTIONS_PROC: uint16 = 3;
/// `HASHNProcs` — number of support functions.
pub const HASHNProcs: uint16 = 3;
