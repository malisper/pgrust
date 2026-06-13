//! Seam declarations for the `backend-access-gin-core-probe` unit's outward
//! call into the fmgr layer.
//!
//! `ginlogic.c`'s `direct*`/`shim*` consistent-check helpers issue
//! `FunctionCall8Coll` / `FunctionCall7Coll` into the opclass-provided
//! `consistent` / `triConsistent` support functions. That is the fmgr calling
//! convention — a genuine external (fmgr's GIN consistent-call dispatcher is
//! not this unit's to own) — so it is reached through these two seams, keyed on
//! the runtime [`GinScanKey`] model (every argument the C functions read travels
//! in the live key). Until the fmgr GIN-call dispatcher lands and installs them,
//! a call panics loudly (mirror-PG-and-panic), which is the intended behavior
//! for an unwired genuine external.

#![allow(non_snake_case)]

use types_tsearch::backend_access_gin_ginlogic::GinScanKey;
use types_tsearch::gin::GinTernaryValue;

seam_core::seam!(
    /// `FunctionCall8Coll(key->consistentFmgrInfo, key->collation, ...)`
    /// (ginlogic.c:73): invoke the opclass boolean `consistent` support
    /// function for `key`, returning its `bool` result. The support function
    /// also sets `key.recheckCurItem`.
    pub fn gin_consistent_call_bool(key: &mut GinScanKey) -> bool
);

seam_core::seam!(
    /// `FunctionCall7Coll(key->triConsistentFmgrInfo, key->collation, ...)`
    /// (ginlogic.c:91/112): invoke the opclass ternary `triConsistent` support
    /// function for `key`, returning its `GinTernaryValue` result.
    pub fn gin_consistent_call_tri(key: &mut GinScanKey) -> GinTernaryValue
);
