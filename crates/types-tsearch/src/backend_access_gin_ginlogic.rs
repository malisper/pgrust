//! Runtime working models for `backend/access/gin/ginlogic.c`.
//!
//! [`GinScanKey`] is an idiomatic model of the `GinScanKeyData` fields
//! (`access/gin_private.h`) that the consistent-check routing reads and writes.
//! It lives in `types` (not in the owning `ginlogic` port crate) so the fmgr
//! consistent-call seams — declared in `backend-access-gin-core-probe-seams`,
//! which cannot depend on the owner crate without a cycle — can reference it by
//! value across the seam boundary, exactly as the C functions pass the live
//! `GinScanKey`.
//!
//! `GinScanKeyData` is a runtime in-memory struct (not an on-disk / ABI shape),
//! so per the porting rules it stays idiomatic rather than `#[repr(C)]`. Only
//! the fields ginlogic actually consults are modeled; the remainder of the C
//! struct (scan-entry pointers, match-status data, etc.) belongs to the
//! not-yet-ported `ginscan`/`ginget` machinery.

extern crate alloc;

use alloc::vec::Vec;

use types_core::Oid;

use crate::gin::GinTernaryValue;

/// Which boolean-consistent implementation `ginInitConsistentFunction` selected
/// for a scan key — the C function pointer `key->boolConsistentFn`, reproduced
/// as an explicit dispatch tag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GinBoolConsistentKind {
    /// `trueConsistentFn` — the EVERYTHING-key dummy.
    True,
    /// `directBoolConsistentFn` — the opclass provides a boolean consistent fn.
    Direct,
    /// `shimBoolConsistentFn` — emulate boolean via the ternary fn.
    Shim,
}

/// Which ternary-consistent implementation `ginInitConsistentFunction` selected
/// for a scan key — the C function pointer `key->triConsistentFn`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GinTriConsistentKind {
    /// `trueTriConsistentFn` — the EVERYTHING-key dummy.
    True,
    /// `directTriConsistentFn` — the opclass provides a ternary consistent fn.
    Direct,
    /// `shimTriConsistentFn` — emulate ternary via the boolean fn.
    Shim,
}

/// Idiomatic model of `GinScanKeyData` (`access/gin_private.h`), restricted to
/// the fields `ginlogic.c` reads and writes.
///
/// `boolConsistentFn`/`triConsistentFn` are C function pointers; modeled here as
/// the [`GinBoolConsistentKind`]/[`GinTriConsistentKind`] dispatch tags assigned
/// by `ginInitConsistentFunction` and dispatched by `callBoolConsistentFn` /
/// `callTriConsistentFn`. `consistentFmgrInfo`/`triConsistentFmgrInfo` are the
/// `FmgrInfo *` the key points at; modeled as the support-function OIDs the fmgr
/// consistent-call seam needs.
#[derive(Clone, Debug)]
pub struct GinScanKey {
    /// `nentries` — real number of entries in `scanEntry[]` (always > 0).
    pub nentries: u32,
    /// `nuserentries` — entries `extractQueryFn`/`consistentFn` know about.
    pub nuserentries: u32,
    /// `entryRes` — the array of check flags reported to the consistent fn.
    pub entryRes: Vec<GinTernaryValue>,
    /// The selected boolean-consistent implementation (C: `boolConsistentFn`).
    pub boolConsistentFn: GinBoolConsistentKind,
    /// The selected ternary-consistent implementation (C: `triConsistentFn`).
    pub triConsistentFn: GinTriConsistentKind,
    /// `consistentFmgrInfo->fn_oid` — the opclass boolean consistent fn OID.
    pub consistent_fmgr_oid: Oid,
    /// `triConsistentFmgrInfo->fn_oid` — the opclass ternary consistent fn OID.
    pub tri_consistent_fmgr_oid: Oid,
    /// `collation` — the collation to pass when calling the support fn.
    pub collation: Oid,
    /// `strategy` — the operator strategy number.
    pub strategy: u16,
    /// `searchMode` — the GIN search mode.
    pub searchMode: i32,
    /// `attnum` — the index attribute number (1-based).
    pub attnum: u16,
    /// `recheckCurItem` — the recheck flag the consistent fn sets.
    pub recheckCurItem: bool,
}

impl GinScanKey {
    /// Construct a scan key carrying the given `entryRes` array, with everything
    /// else at the defaults `ginInitConsistentFunction` later overwrites. `nentries`
    /// is `entryRes.len()`, matching how the C code allocates `entryRes` with one
    /// slot per scan entry.
    pub fn from_entry_res(entry_res: Vec<GinTernaryValue>) -> Self {
        let nentries = entry_res.len() as u32;
        GinScanKey {
            nentries,
            nuserentries: nentries,
            entryRes: entry_res,
            boolConsistentFn: GinBoolConsistentKind::Shim,
            triConsistentFn: GinTriConsistentKind::Shim,
            consistent_fmgr_oid: types_core::InvalidOid,
            tri_consistent_fmgr_oid: types_core::InvalidOid,
            collation: types_core::InvalidOid,
            strategy: 0,
            searchMode: 0,
            attnum: 1,
            recheckCurItem: false,
        }
    }
}
