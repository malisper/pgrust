//! Data model for the `EEOP_HASHED_SCALARARRAYOP` hash table
//! (`execExprInterp.c:195-235`).
//!
//! C forward-declares `struct ScalarArrayOpExprHashTable *elements_tab` in the
//! `ExprEvalStep` payload (`execExpr.h`) and defines the struct, plus the
//! `lib/simplehash.h` `saophash_*` instantiation, in `execExprInterp.c`. The
//! interpreter is the only owner. Per the repo's "opacity inherited, never
//! introduced" rule, the step carries the real typed table
//! (`Option<Box<ScalarArrayOpExprHashTable>>`) instead of the prior opaque
//! address word.
//!
//! These are the *data* definitions only (pure, no fmgr dependency); the
//! `saophash_create`/`insert`/`lookup`/`grow` algorithms live in the
//! interpreter crate (`backend_executor_execExprInterp::saophash`), which owns
//! the simplehash template instantiation.

use types_core::fmgr::FmgrInfo;
use types_datum::Datum;

/// `ScalarArrayOpExprHashEntry` (execExprInterp.c:195) — one simplehash slot.
#[derive(Clone, Copy, Debug, Default)]
pub struct ScalarArrayOpExprHashEntry {
    /// `Datum key` (`SH_KEY`).
    pub key: Datum,
    /// `uint32 status` — `SH_STATUS_EMPTY` (0) / `SH_STATUS_IN_USE` (1).
    pub status: u32,
    /// `uint32 hash` — cached hash (`SH_STORE_HASH` / `SH_GET_HASH`).
    pub hash: u32,
}

/// `saophash_hash` — the macro-generated open-addressing table header
/// (`SH_TYPE`). The C `ctx`/`private_data` fields are folded into the owning
/// [`ScalarArrayOpExprHashTable`]; this header keeps only the bookkeeping.
#[derive(Debug, Default)]
pub struct SaophashHash {
    /// `uint64 size` — bucket count, always a power of two.
    pub size: u64,
    /// `uint32 members` — live entry count.
    pub members: u32,
    /// `uint32 sizemask` — `size - 1`; `hash & sizemask` is the bucket index.
    pub sizemask: u32,
    /// `uint32 grow_threshold` — grow once `members >= grow_threshold`.
    pub grow_threshold: u32,
    /// `SH_ELEMENT_TYPE *data` — the bucket array (`SH_ALLOCATE`d, zeroed).
    pub data: alloc::vec::Vec<ScalarArrayOpExprHashEntry>,
}

/// `ScalarArrayOpExprHashTable` (execExprInterp.c:217) — the hashed-SAOP table
/// the simplehash's `private_data` back-pointer refers to.
///
/// In C the struct is `palloc0`'d with a flexible
/// `FunctionCallInfoBaseData hash_fcinfo_data` tail (`SizeForFunctionCallInfo(1)`)
/// and a `struct ExprEvalStep *op` back-pointer the equality callback uses to
/// reach `op->d.hashedscalararrayop.fcinfo_data`/`.finfo`. The owned model
/// carries only the data that survives across rows: the underlying table and
/// the resolved hash-function `FmgrInfo`. The call frames are re-derived per
/// dispatch through the fmgr seam (by OID), so the flexible `hash_fcinfo_data`
/// tail and the `op` back-pointer are not materialized.
#[derive(Debug, Default)]
pub struct ScalarArrayOpExprHashTable {
    /// `saophash_hash *hashtab` — the underlying open-addressing table.
    pub hashtab: SaophashHash,
    /// `FmgrInfo hash_finfo` — the hash function's lookup data
    /// (`fmgr_info(saop->hashfuncid)`). The owned model needs only `fn_oid`
    /// (the fmgr seam re-resolves by OID; see the crate's F0 contract).
    pub hash_finfo: FmgrInfo,
}
