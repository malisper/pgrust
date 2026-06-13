//! Arena-form signature types for `backend-utils-cache-catcache`
//! (`utils/cache/catcache.c` + `utils/catcache.h`).
//!
//! Two distinct representations live here:
//!
//!  * [`CCFastKind`] — the small computational-core tag the hash/equality fast
//!    functions dispatch on (the C `cc_hashfunc[i]`/`cc_fastequal[i]` indirect
//!    function-pointer slots, kept here as a *selection* tag applied at use
//!    time).
//!
//!  * The **arena cache graph** ([`CatCacheArena`], [`ArenaCatCache`],
//!    [`ArenaCatCTup`], [`ArenaCatCList`], the `*Idx` handles, and
//!    [`CatCInProgress`]). `catcache.c` is, at heart, an intrusive,
//!    shared-aliasing data structure: a `CatCTup` is *simultaneously*
//!    bucket-linked (`dlist_node cache_elem`), referenced by a `CatCList`
//!    (`cl->members[i]`), back-references its list (`ct->c_list`) and its cache
//!    (`ct->my_cache`), and is handed to callers as `&ct->tuple`. A
//!    `Box`-of-owned control block cannot host that aliasing graph (a node
//!    cannot be owned by both a bucket and a list). So the *live cache state*
//!    is modelled here as a **free-listed index arena**: a cache owns its
//!    tuples and lists in `Vec<Option<…>>` slots, and every cross-reference
//!    (bucket chains, list membership, `c_list`/`my_cache` back-links) is an
//!    arena index, not a pointer. This expresses the C aliasing graph
//!    faithfully and safely (no `unsafe`, no `Rc`/`RefCell`), and lets the
//!    graph-management logic port 1:1.
//!
//! Intrusive doubly-linked buckets become an ordered `Vec` of arena indices per
//! bucket: `dlist_push_head` is `insert(0, …)`, `dlist_move_head` is
//! remove-then-`insert(0, …)`, `dlist_delete` is remove-by-value, and
//! `dlist_foreach` iterates the `Vec` front-to-back — preserving the exact
//! most-recently-used ordering the C reordering produces.
//!
//! `cc_tupdesc` (which in C lives in `CacheMemoryContext`) is not stored on the
//! arena: the owning crate exposes it through the `with_cache_tupdesc` /
//! `cache_tupdesc_is_valid` seams; the arena records only whether phase-2
//! initialization has loaded it ([`ArenaCatCache::initialized`]).

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use types_core::Oid;
use types_datum::Datum;
use types_scan::scankey::ScanKeyData;

/// Which hard-coded hash/equality pair a catcache key uses. Mirrors the
/// `(CCHashFN, CCFastEqualFN)` pair `GetCCHashEqFuncs` assigns for a key type
/// (`catcache.c`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CCFastKind {
    /// `chareqfast` / `charhashfast` — `BOOLOID`, `CHAROID`.
    Char,
    /// `nameeqfast` / `namehashfast` — `NAMEOID`.
    Name,
    /// `int2eqfast` / `int2hashfast` — `INT2OID`.
    Int2,
    /// `int4eqfast` / `int4hashfast` — `INT4OID` and the OID/`reg*` family.
    Int4,
    /// `texteqfast` / `texthashfast` — `TEXTOID`.
    Text,
    /// `oidvectoreqfast` / `oidvectorhashfast` — `OIDVECTOROID`.
    OidVector,
}

/// `CATCACHE_MAXKEYS` (`catcache.h`).
pub const CATCACHE_MAXKEYS: usize = 4;

/// `CT_MAGIC` — sentinel stored in a tuple entry, checked by `ReleaseCatCache`.
pub const CT_MAGIC: i32 = 0x5726_1502;
/// `CL_MAGIC` — sentinel stored in a list entry, checked by `ReleaseCatCacheList`.
pub const CL_MAGIC: i32 = 0x5276_5103;

/* ===========================================================================
 * Arena handles. Each is a stable index into a cache's slot vector. `NONE`
 * (`usize::MAX`) stands in for the C `NULL` pointer. A slot is never reused
 * while a live handle to it could exist (freed only once refcount + list
 * back-references reach zero, exactly as `pfree` happens in C).
 * ======================================================================== */

/// Arena index of an [`ArenaCatCache`] within a [`CatCacheArena`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CacheIdx(pub usize);

impl CacheIdx {
    /// The null cache handle.
    pub const NONE: CacheIdx = CacheIdx(usize::MAX);
    /// `true` when this stands in for a C `NULL`.
    #[inline]
    pub fn is_none(self) -> bool {
        self.0 == usize::MAX
    }
}

/// Arena index of an [`ArenaCatCTup`] within an [`ArenaCatCache::tuples`] vector.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CtIdx(pub usize);

impl CtIdx {
    /// The null tuple handle (C `CatCTup *` `NULL`).
    pub const NONE: CtIdx = CtIdx(usize::MAX);
    /// `true` when this stands in for a C `NULL`.
    #[inline]
    pub fn is_none(self) -> bool {
        self.0 == usize::MAX
    }
}

/// Arena index of an [`ArenaCatCList`] within an [`ArenaCatCache::lists`] vector.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ClIdx(pub usize);

impl ClIdx {
    /// The null list handle (C `CatCList *` `NULL`).
    pub const NONE: ClIdx = ClIdx(usize::MAX);
    /// `true` when this stands in for a C `NULL`.
    #[inline]
    pub fn is_none(self) -> bool {
        self.0 == usize::MAX
    }
}

/* ===========================================================================
 * Resource-owner reference identities. In C,
 * `ResourceOwnerRememberCatCacheRef` records the borrowed pointer `&ct->tuple`
 * and `…CatCacheListRef` records `cl`. In the arena form the stable identity
 * of a pinned tuple is its `(cache_id, ct_idx)` handle pair and a pinned
 * list's is its `(cache_id, cl_idx)` pair — what the resowner seams carry.
 * ======================================================================== */

/// Resource-owner identity of a pinned catcache tuple — the owned stand-in for
/// C's remembered `CatCTup *` (`ResourceOwnerRememberCatCacheRef`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct CatCacheRef {
    /// `SysCacheIdentifier` of the owning cache (the C `ct->my_cache->id`).
    pub cache_id: i32,
    /// Arena slot index of the pinned tuple within its cache's `tuples` vector.
    pub ct_idx: usize,
}

/// Resource-owner identity of a pinned catcache list — the owned stand-in for
/// C's remembered `CatCList *` (`ResourceOwnerRememberCatCacheListRef`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct CatCacheListRef {
    /// `SysCacheIdentifier` of the owning cache (the C `cl->my_cache->id`).
    pub cache_id: i32,
    /// Arena slot index of the pinned list within its cache's `lists` vector.
    pub cl_idx: usize,
}

/// A packed `ItemPointerData` `(block, offset)`. Block split into the C
/// `bi_hi`/`bi_lo` is unnecessary here; equality of the whole pair is what
/// `ItemPointerEquals` tests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ItemPointer {
    /// 32-bit block number (`ip_blkid` hi/lo combined).
    pub block: u32,
    /// `ip_posid` — line-pointer offset within the block.
    pub offset: u16,
}

/* ===========================================================================
 * `CatCTup` (`struct catctup`) — one cached catalog tuple, arena form.
 * ======================================================================== */

/// `CatCTup` (`struct catctup`) — one cached catalog tuple, arena form.
#[derive(Clone, Debug)]
pub struct ArenaCatCTup {
    /// `ct_magic` — `CT_MAGIC` for a valid entry.
    pub ct_magic: i32,
    /// `hash_value` — hash of the tuple's keys (selects the bucket).
    pub hash_value: u32,
    /// `keys[CATCACHE_MAXKEYS]` — the entry's key datums.
    pub keys: [Datum; CATCACHE_MAXKEYS],
    /// `refcount` — number of active references (callers + lists).
    pub refcount: i32,
    /// `dead` — set when invalidated while still referenced.
    pub dead: bool,
    /// `negative` — a negative cache entry (key present, no tuple).
    pub negative: bool,
    /// `tuple.t_len` — length of the cached tuple data.
    pub t_len: u32,
    /// `tuple.t_self` — the item pointer of the source tuple; `t_self` equality
    /// drives list de-dup.
    pub t_self: ItemPointer,
    /// `tuple.t_tableOid`.
    pub t_tableoid: Oid,
    /// The cached tuple's MAXALIGNed data bytes (`memcpy` of `dtp->t_data`).
    pub t_data: Vec<u8>,
    /// `c_list` — the list this tuple belongs to, if any.
    pub c_list: ClIdx,
    /// `my_cache` — back-reference to the owning cache.
    pub my_cache: CacheIdx,
}

/* ===========================================================================
 * `CatCList` (`struct catclist`) — partial-key list result, arena form.
 * ======================================================================== */

/// `CatCList` (`struct catclist`) — result of a partial-key list search, arena
/// form. `members` is the owned vector of member tuple handles (the C
/// `members[FLEXIBLE_ARRAY_MEMBER]` of borrowed `CatCTup *`).
#[derive(Clone, Debug)]
pub struct ArenaCatCList {
    /// `cl_magic` — `CL_MAGIC` for a valid list.
    pub cl_magic: i32,
    /// `hash_value` — hash of the partial key.
    pub hash_value: u32,
    /// `keys[CATCACHE_MAXKEYS]` — the partial key datums.
    pub keys: [Datum; CATCACHE_MAXKEYS],
    /// `refcount`.
    pub refcount: i32,
    /// `dead`.
    pub dead: bool,
    /// `ordered` — list came from an ordered (index) scan.
    pub ordered: bool,
    /// `nkeys` — number of partial keys searched on.
    pub nkeys: i16,
    /// `n_members`.
    pub n_members: i32,
    /// `my_cache` — back-reference to the owning cache.
    pub my_cache: CacheIdx,
    /// `members` — the member tuple handles, in scan order.
    pub members: Vec<CtIdx>,
}

/* ===========================================================================
 * `CatCache` (`struct catcache`) — per-cache control block, arena form.
 * ======================================================================== */

/// `CatCache` (`struct catcache`) — per-cache control block, arena form.
#[derive(Clone, Debug)]
pub struct ArenaCatCache {
    /// `id` — the syscache identifier (`SysCacheIdentifier`).
    pub id: i32,
    /// `cc_relname` — catalog name (`"(not known yet)"` until phase 2).
    pub cc_relname: String,
    /// `cc_reloid` — OID of the catalog relation.
    pub cc_reloid: Oid,
    /// `cc_indexoid` — OID of the index used for scans.
    pub cc_indexoid: Oid,
    /// `cc_relisshared`.
    pub cc_relisshared: bool,
    /// `cc_ntup` — number of tuples (positive + negative) currently cached.
    pub cc_ntup: i32,
    /// `cc_nlist` — number of lists currently cached.
    pub cc_nlist: i32,
    /// `cc_nbuckets` — number of tuple hash buckets (power of two).
    pub cc_nbuckets: i32,
    /// `cc_nlbuckets` — number of list hash buckets (0 until first list search).
    pub cc_nlbuckets: i32,
    /// `cc_nkeys` — number of key columns.
    pub cc_nkeys: i32,
    /// `cc_keyno[CATCACHE_MAXKEYS]` — attribute numbers of the key columns.
    pub cc_keyno: [i32; CATCACHE_MAXKEYS],
    /// Per-key fast hash/equality selection (set in phase 2 by
    /// `GetCCHashEqFuncs`); `None` until the cache is initialized.
    pub cc_fastkind: [Option<CCFastKind>; CATCACHE_MAXKEYS],
    /// `cc_skey[CATCACHE_MAXKEYS]` — the scankey template (the genam scan owner
    /// re-resolves the comparison procedure with `fmgr_info`); `None` per slot
    /// until phase 2 builds it.
    pub cc_skey: [Option<ScanKeyData>; CATCACHE_MAXKEYS],
    /// Whether `CatalogCacheInitializeCache` has run (the C `cc_tupdesc !=
    /// NULL` guard of `ConditionalCatalogCacheInitializeCache`). The descriptor
    /// itself is reached via the `with_cache_tupdesc` seam, not stored here.
    pub initialized: bool,
    /// `cc_bucket[cc_nbuckets]` — tuple buckets, each an ordered list of tuple
    /// indices (front = most-recently-used).
    pub cc_bucket: Vec<Vec<CtIdx>>,
    /// `cc_lbucket[cc_nlbuckets]` — list buckets. Empty until the first list
    /// search allocates it.
    pub cc_lbucket: Vec<Vec<ClIdx>>,
    /// Owned tuple slots (a `None` slot is free for reuse — see `ct_freelist`).
    pub tuples: Vec<Option<ArenaCatCTup>>,
    /// Free tuple-slot indices available for reuse.
    pub ct_freelist: Vec<usize>,
    /// Owned list slots (a `None` slot is free — see `cl_freelist`).
    pub lists: Vec<Option<ArenaCatCList>>,
    /// Free list-slot indices available for reuse.
    pub cl_freelist: Vec<usize>,
}

/// A catalog tuple fetched by the substrate scan seam, carrying everything the
/// catcache needs to build a positive [`ArenaCatCTup`]: a detoasted/flattened
/// `HeapTuple` plus the key datums extracted from it.
///
/// In C, `SearchCatCacheMiss` runs `table_open` + `systable_beginscan` +
/// `systable_getnext`, detoasts each tuple, and extracts the key columns via
/// `cc_tupdesc`. That whole fetch+flatten+extract step crosses the substrate
/// scan seam, which returns these carriers; the catcache keeps ownership of the
/// caching decisions (bucket placement, refcounting, negative entries, lists).
#[derive(Clone, Debug)]
pub struct FetchedCatalogTuple {
    /// `tuple.t_len`.
    pub t_len: u32,
    /// `tuple.t_self` — source item pointer.
    pub t_self: ItemPointer,
    /// `tuple.t_tableOid`.
    pub t_tableoid: Oid,
    /// The flattened tuple data bytes (post-detoast).
    pub t_data: Vec<u8>,
    /// The tuple's key datums, extracted via the cache's `cc_keyno` + tupdesc.
    pub keys: [Datum; CATCACHE_MAXKEYS],
}

/* ===========================================================================
 * `CatCInProgress` — the create-in-progress stack node (`catcache.c`).
 * ======================================================================== */

/// `CatCInProgress` (`catcache.c`) — one entry of the create-in-progress stack.
#[derive(Clone, Copy, Debug)]
pub struct CatCInProgress {
    /// `cache` — the cache the in-progress entry belongs to.
    pub cache: CacheIdx,
    /// `hash_value` — hash of the entry (ignored for lists).
    pub hash_value: u32,
    /// `list` — `true` if it's a list entry.
    pub list: bool,
    /// `dead` — set when the entry is invalidated mid-build.
    pub dead: bool,
}

/* ===========================================================================
 * `CatCacheArena` — the whole catcache subsystem state (`CacheHdr` + caches +
 * in-progress stack), owned by the catcache crate.
 * ======================================================================== */

/// The catcache subsystem's owned state. Equivalent to the file-scope
/// `CacheHdr` (`CatCacheHeader`) plus the `SysCache[]` array of `CatCache *`
/// plus `catcache_in_progress_stack`.
#[derive(Clone, Debug, Default)]
pub struct CatCacheArena {
    /// All caches, in registration order (the C `slist ch_caches`).
    pub caches: Vec<ArenaCatCache>,
    /// `CacheHdr->ch_ntup` — total tuples across all caches.
    pub ch_ntup: i32,
    /// `catcache_in_progress_stack` — the create-in-progress stack (top last).
    pub in_progress: Vec<CatCInProgress>,
}

impl Default for ArenaCatCTup {
    fn default() -> Self {
        ArenaCatCTup {
            ct_magic: CT_MAGIC,
            hash_value: 0,
            keys: [Datum::null(); CATCACHE_MAXKEYS],
            refcount: 0,
            dead: false,
            negative: false,
            t_len: 0,
            t_self: ItemPointer::default(),
            t_tableoid: Oid::default(),
            t_data: Vec::new(),
            c_list: ClIdx::NONE,
            my_cache: CacheIdx::NONE,
        }
    }
}

impl Default for ArenaCatCList {
    fn default() -> Self {
        ArenaCatCList {
            cl_magic: CL_MAGIC,
            hash_value: 0,
            keys: [Datum::null(); CATCACHE_MAXKEYS],
            refcount: 0,
            dead: false,
            ordered: false,
            nkeys: 0,
            n_members: 0,
            my_cache: CacheIdx::NONE,
            members: Vec::new(),
        }
    }
}
