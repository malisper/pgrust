//! `utils/funccache.h` — the shared vocabulary of `funccache.c`'s function
//! cache: the per-call hash key, the cache-entry header embedded by every
//! procedural language's compiled-function struct, and the hashtable entry.
//!
//! `funccache.c` keeps a backend-lifetime cache of compiled-function data,
//! keyed by `(function OID, input argument types, trigger / event-trigger
//! context, input collation, optional composite result rowtype)`. A
//! [`CachedFunction`] is the funccache-managed header; the C comment notes it
//! "will typically be embedded in a larger struct containing
//! function-language-specific data" (e.g. `PLpgSQL_function`,
//! `SQLFunctionCache`). The language-specific payload is opaque to funccache —
//! C reaches it through `MemoryContextAllocZero(cacheEntrySize)` placing the
//! header first and the payload after it, then a `void *` cast.
//!
//! That one allocation is aliased three ways in C: it lives in the hash table,
//! is returned to the caller (stashed in `fcinfo->flinfo->fn_extra`), and is
//! back-linked from `function->fn_hashkey`. The owned model carries the payload
//! behind an `Rc<RefCell<dyn CachedFunctionPayload>>` ([`CachedFunctionRef`]) so
//! the same allocation is both cache-resident and handed back to the caller,
//! with the embedded header mutated in place through the
//! [`CachedFunctionPayload`] trait. This crate declares only the
//! funccache-managed fields, exactly as `funccache.h` does.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::rc::Rc;
use core::cell::RefCell;

use ::mcx::PgBox;
use ::types_core::primitive::{Oid, Size, TransactionId};
use ::types_core::FUNC_MAX_ARGS;
use ::types_tuple::heaptuple::{ItemPointerData, TupleDescData};

/// `TupleDesc` as carried inside a cache key (`funccache.h`: the optional
/// `callResultType` for a composite-returning function). It is copied into
/// `TopMemoryContext` at insert time and freed at delete time; here it is an
/// owned [`PgBox`] in the cache context, `None` for a scalar result.
pub type KeyTupleDesc<'mcx> = Option<PgBox<'mcx, TupleDescData<'mcx>>>;

/// `CachedFunctionHashKey` (`funccache.h`). The hash lookup key for functions;
/// it accounts for every aspect of a specific call that might lead to different
/// data types or collations being used within the function.
///
/// The C struct is carefully `memcmp`-able (pad bytes zeroed) so `cfunc_hash` /
/// `cfunc_match` can `hash_any` / `memcmp` it up to `callResultType`. The owned
/// key is not `repr(C)`; `cfunc_hash` / `cfunc_match` hash / compare the same
/// logical fields field-by-field, skipping the unused trailing `argtypes` slots
/// exactly as the C does via `nargs`.
#[derive(Debug)]
pub struct CachedFunctionHashKey<'mcx> {
    /// `Oid funcOid`.
    pub funcOid: Oid,
    /// `bool isTrigger` — called as a DML trigger.
    pub isTrigger: bool,
    /// `bool isEventTrigger` — called as an event trigger.
    pub isEventTrigger: bool,
    /// `Size cacheEntrySize` — the language-specific cache-entry size, part of
    /// the key so two languages sharing this hash table never collide and so
    /// `CREATE OR REPLACE FUNCTION` across languages re-keys correctly.
    pub cacheEntrySize: Size,
    /// `Oid trigOid` — for a DML trigger function, the OID of the trigger; zero
    /// otherwise (and in validation mode).
    pub trigOid: Oid,
    /// `Oid inputCollation` — the input collation, part of the key because
    /// different collations need different `Param` collations in the plan.
    pub inputCollation: Oid,
    /// `int nargs` — number of input arguments (`pronargs`).
    pub nargs: i32,
    /// `TupleDesc callResultType` — the result descriptor for a function
    /// returning composite, when the caller asked for it; `None` otherwise.
    pub callResultType: KeyTupleDesc<'mcx>,
    /// `Oid argtypes[FUNC_MAX_ARGS]` — input argument types with polymorphic
    /// types resolved to actual types. Only the first `nargs` entries are valid.
    pub argtypes: [Oid; FUNC_MAX_ARGS],
}

impl<'mcx> Default for CachedFunctionHashKey<'mcx> {
    fn default() -> Self {
        Self {
            funcOid: 0,
            isTrigger: false,
            isEventTrigger: false,
            cacheEntrySize: 0,
            trigOid: 0,
            inputCollation: 0,
            nargs: 0,
            callResultType: None,
            argtypes: [0; FUNC_MAX_ARGS],
        }
    }
}

/// The funccache-managed header of a compiled function (`CachedFunction` in
/// `funccache.h`). Every procedural language embeds this by value at the head of
/// its larger compiled-function struct; this struct holds just the fields
/// `funccache.c` itself manages.
pub struct CachedFunction {
    /// `CachedFunctionHashKey *fn_hashkey` — back-link to the hashtable entry's
    /// key, or `None` when not in the hash table. The key is owned by the
    /// hashtable entry, so a raw aliasing back-pointer is not expressible; the
    /// owned model records the [`CachedFunctionKeyId`] fingerprint the header is
    /// installed under, which `delete_function` uses to relocate and drop the
    /// entry. `None` == NULL == "not in table".
    pub fn_hashkey: Option<CachedFunctionKeyId>,
    /// `TransactionId fn_xmin` — xmin of the function's `pg_proc` row, used to
    /// detect invalidation.
    pub fn_xmin: TransactionId,
    /// `ItemPointerData fn_tid` — ctid of the function's `pg_proc` row.
    pub fn_tid: ItemPointerData,
    /// `CachedFunctionDeleteCallback dcallback` — the language-specific
    /// subsidiary-storage cleanup, or `None` when there is nothing to free.
    pub dcallback: CachedFunctionDeleteCallback,
    /// `uint64 use_count` — bumped while the function is being used; the cache
    /// must not reclaim subsidiary storage while this is nonzero.
    pub use_count: u64,
}

impl Default for CachedFunction {
    fn default() -> Self {
        Self {
            fn_hashkey: None,
            fn_xmin: 0,
            fn_tid: ItemPointerData::default(),
            dcallback: None,
            use_count: 0,
        }
    }
}

/// The owned-model locator for the hashtable bucket a [`CachedFunction`] is
/// installed in (C's `function->fn_hashkey` back-link): the `cfunc_hash` value of
/// the entry's key. Together with the live key stored in the entry, this lets
/// `delete_function` relocate the entry (find its bucket, match within it)
/// without a raw aliasing pointer — exactly the C dynahash hash-then-match-chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CachedFunctionKeyId(pub u32);

/// `CachedFunctionDeleteCallback` (`funccache.h`): "called when discarding a
/// cache entry. Free any free-able subsidiary data of `cfunc`, but not the
/// struct `CachedFunction` itself." `None` is C's NULL `dcallback`. The owned
/// model passes the language payload (the trait object embedding the header) so
/// the callback can drop its subsidiary storage.
pub type CachedFunctionDeleteCallback = Option<fn(&mut dyn CachedFunctionPayload)>;

/// The language-specific compiled-function payload that embeds a
/// [`CachedFunction`] header by value (C's "larger struct containing
/// function-language-specific data").
///
/// `funccache.c` treats this as opaque (a `void *` after the header obtained
/// from `MemoryContextAllocZero(cacheEntrySize)`); the owned model expresses
/// that opacity as a trait giving funccache access to exactly the header fields
/// it manages, leaving the rest to the owning language. Each procedural
/// language implements this for its compiled-function struct (e.g.
/// `PLpgSQL_function`).
pub trait CachedFunctionPayload {
    /// `&((CachedFunction *) entry)->...` — the embedded funccache header.
    fn cfunc(&self) -> &CachedFunction;
    /// Mutable access to the embedded header (funccache fills `fn_xmin`,
    /// `fn_tid`, `fn_hashkey`, `dcallback`, and `use_count`).
    fn cfunc_mut(&mut self) -> &mut CachedFunction;
}

/// A shared, mutable reference to a compiled-function payload.
///
/// This is the owned model's expression of the single
/// `MemoryContextAllocZero(cacheEntrySize)` allocation that C aliases three
/// ways. `Rc` gives the shared ownership (cache-resident *and* returned to the
/// caller / stashed in `fn_extra`); `RefCell` gives the interior mutability
/// funccache needs to fill the header (`fn_xmin`/`fn_tid`/`use_count`) after the
/// language callback has built the body. Cloning an `Rc` is C's pointer copy.
///
/// The payload's subsidiary storage is allocated in the backend-lifetime cache
/// context (C's `TopMemoryContext`), so the trait object is lifetime-free; only
/// the key's `callResultType` carries the cache context's `'mcx` (see
/// [`CachedFunctionHashEntry`]).
pub type CachedFunctionRef = Rc<RefCell<dyn CachedFunctionPayload>>;

/// `CachedFunctionHashEntry` (`funccache.c`) — one hashtable slot: the live key
/// plus the shared cached payload. The key's `callResultType` lives in the
/// cache's `'mcx` context (C's `TopMemoryContext`).
pub struct CachedFunctionHashEntry<'mcx> {
    /// The hash key (must be first in C; here it co-owns the `callResultType`).
    pub key: CachedFunctionHashKey<'mcx>,
    /// The cached payload, shared with the caller (C's aliased `CachedFunction *`).
    pub function: CachedFunctionRef,
}

/// The `pg_proc` facts `cached_function_compile` needs from the function's
/// catalog row: the input-type signature (for the hash key) plus the row's
/// xmin/ctid (for the up-to-dateness check). This is the funccache projection
/// of `SearchSysCache1(PROCOID)` + `GETSTRUCT`, mirroring the `FastpathProcRow`
/// / `ProcInfo` syscache projections; the syscache owner produces it and
/// funccache reads it across the syscache seam.
#[derive(Debug)]
pub struct ProcCompileInfo<'mcx> {
    /// `procStruct->pronargs`.
    pub pronargs: i16,
    /// `procStruct->proargtypes.values` (length `pronargs`).
    pub proargtypes: ::mcx::PgVec<'mcx, Oid>,
    /// `NameStr(procStruct->proname)` — for the polymorphic-resolution error.
    pub proname: ::mcx::PgString<'mcx>,
    /// `HeapTupleHeaderGetRawXmin(procTup->t_data)`.
    pub xmin: TransactionId,
    /// `procTup->t_self`.
    pub tid: ItemPointerData,
}
