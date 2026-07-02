#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod compute;
mod graph;
mod init;
mod inval;
mod list;
mod search;
#[cfg(test)]
mod tests;

use core::cell::UnsafeCell;
use core::mem::ManuallyDrop;
use core::ptr::NonNull;

use datum::Datum;
use mcx::{bind, Mcx, McxOwned, MemoryContext, PgString, PgVec};
use types_core::Oid;
use types_tuple::{ItemPointerData, TupleDescData};

pub use compute::{CatCKey, CCFastKind, CATCACHE_MAXKEYS};
pub use graph::{
    CatCacheInvalidate, CatalogCacheFlushCatalog, InitCatCache, ResetCatalogCaches,
    ResetCatalogCachesExt,
};
pub use init::{cache_nkeys, cache_relisshared, cache_tupdesc, InitCatCachePhase2};
pub use inval::PrepareToInvalidateCacheTuple;
pub use list::{CatCList as CatCListRef, ReleaseCatCacheList, SearchCatCacheList};
pub use search::{
    CatCTuple, GetCatCacheHashValue, ReleaseCatCache, SearchCatCache, SearchCatCache1,
    SearchCatCache2, SearchCatCache3, SearchCatCache4,
};

pub(crate) const NONE: u32 = u32::MAX;

/// `CatCTup`. `keys[i]` is C's bare `Datum keys[]`: the scalar word for
/// by-value kinds; for by-reference kinds a packed `(off << 32) | len` into
/// `payload` (positive: the tuple image; negative: the copied key buffer) —
/// C's pointer-into-the-cached-tuple, made realloc-proof.
pub(crate) struct CatCTup {
    pub hash_value: u32,
    pub refcount: i32,
    pub dead: bool,
    pub negative: bool,
    pub next: u32,
    pub prev: u32,
    pub c_list: u32,
    pub keys: [Datum; CATCACHE_MAXKEYS],
    pub t_len: u32,
    pub t_self: ItemPointerData,
    pub t_tableoid: Oid,
    /// Stable allocation in the cache context; entries move on slot-vec
    /// growth, this never does (hit borrows point here).
    pub payload: *mut u8,
    pub payload_len: u32,
}

const _: () = assert!(core::mem::size_of::<CatCTup>() <= 128);

pub(crate) struct CatCList<'mcx> {
    pub hash_value: u32,
    pub refcount: i32,
    pub dead: bool,
    pub ordered: bool,
    pub nkeys: i16,
    pub next: u32,
    pub prev: u32,
    pub keys: [Datum; CATCACHE_MAXKEYS],
    pub payload: *mut u8,
    pub payload_len: u32,
    pub members: PgVec<'mcx, u32>,
}

pub(crate) struct CatCache<'mcx> {
    pub id: i32,
    pub cc_reloid: Oid,
    pub cc_indexoid: Oid,
    pub cc_relisshared: bool,
    pub initialized: bool,
    pub cc_ntup: i32,
    pub cc_nlist: i32,
    pub cc_nbuckets: u32,
    pub cc_nlbuckets: u32,
    pub cc_nkeys: i32,
    pub cc_keyno: [i32; CATCACHE_MAXKEYS],
    pub cc_kind: [CCFastKind; CATCACHE_MAXKEYS],
    pub cc_eqfunc: [Oid; CATCACHE_MAXKEYS],
    pub cc_tupdesc: Option<&'static TupleDescData<'static>>,
    pub cc_relname: Option<PgString<'mcx>>,
    pub cc_bucket: PgVec<'mcx, u32>,
    pub cc_lbucket: PgVec<'mcx, u32>,
    pub tuples: PgVec<'mcx, CatCTup>,
    pub ct_free: u32,
    pub lists: PgVec<'mcx, CatCList<'mcx>>,
    pub cl_free: u32,
}

#[derive(Clone, Copy)]
pub(crate) struct CatCInProgress {
    pub cache_id: i32,
    pub hash_value: u32,
    pub list: bool,
    pub dead: bool,
}

/// `CacheHdr` + `SysCache[]` + `catcache_in_progress_stack`. `caches` is
/// indexed by syscache id (C's `SysCache[cacheId]`).
pub(crate) struct CatCacheState<'mcx> {
    pub mcx: Mcx<'mcx>,
    pub caches: PgVec<'mcx, Option<CatCache<'mcx>>>,
    pub ch_ntup: i32,
    pub in_progress: PgVec<'mcx, CatCInProgress>,
}

bind!(pub(crate) CatCacheStateTy => CatCacheState<'mcx>);

thread_local! {
    // UnsafeCell, not RefCell: SearchCatCache1 runs on every catalog lookup
    // and the borrow-flag traffic is per-access overhead C does not pay
    // (fabled #292, ~12% suite-wide). ManuallyDrop keeps the payload
    // !needs_drop; the state lives for the backend's life like C's
    // CacheMemoryContext statics.
    static STATE: UnsafeCell<Option<ManuallyDrop<McxOwned<CatCacheStateTy>>>> =
        const { UnsafeCell::new(None) };
}

#[cfg(debug_assertions)]
thread_local! {
    static BORROW_DEPTH: core::cell::Cell<u8> = const { core::cell::Cell::new(0) };
}

#[cfg(debug_assertions)]
struct BorrowGuard;

#[cfg(debug_assertions)]
impl Drop for BorrowGuard {
    fn drop(&mut self) {
        BORROW_DEPTH.with(|d| d.set(d.get() - 1));
    }
}

#[cold]
fn state_init(slot: &mut Option<ManuallyDrop<McxOwned<CatCacheStateTy>>>) {
    let owned = McxOwned::<CatCacheStateTy>::try_new(
        MemoryContext::new("CacheMemoryContext"),
        |mcx| {
            Ok(CatCacheState {
                mcx,
                caches: PgVec::new_in(mcx),
                ch_ntup: 0,
                in_progress: PgVec::new_in(mcx),
            })
        },
    )
    .expect("CacheMemoryContext allocation");
    *slot = Some(ManuallyDrop::new(owned));
}

/// Run `f` with `&mut` state — the borrow-flag-free analog of C reaching
/// `CacheHdr`/`SysCache[]` through bare pointers.
///
/// # Safety
///
/// The `&mut` must not be re-entered while live: one single-threaded backend
/// owns the thread-local, and every catcache operation confines its borrow
/// to one `f` and drops it before any call that can re-enter the catcache
/// (cache init, the miss scan, syscache callbacks, inval). A pure hit calls
/// no seam and no foreign code inside `f`. The debug/Miri guard turns any
/// violation into a panic.
#[inline(always)]
pub(crate) fn with_state<R>(f: impl for<'mcx> FnOnce(&mut CatCacheState<'mcx>) -> R) -> R {
    STATE.with(|cell| {
        #[cfg(debug_assertions)]
        let _guard = {
            BORROW_DEPTH.with(|d| {
                assert_eq!(d.get(), 0, "catcache state re-entered while a borrow is live");
                d.set(1);
            });
            BorrowGuard
        };
        // SAFETY: single-statement, non-reentrant borrow (see above).
        let slot = unsafe { &mut *cell.get() };
        if slot.is_none() {
            state_init(slot);
        }
        slot.as_mut().unwrap().with_mut(f)
    })
}

impl<'mcx> CatCacheState<'mcx> {
    #[inline]
    pub(crate) fn cache(&self, id: i32) -> &CatCache<'mcx> {
        self.caches
            .get(id as usize)
            .and_then(|c| c.as_ref())
            .unwrap_or_else(|| panic!("catcache: cache id {id} not registered"))
    }

    #[inline]
    pub(crate) fn cache_mut(&mut self, id: i32) -> &mut CatCache<'mcx> {
        self.caches
            .get_mut(id as usize)
            .and_then(|c| c.as_mut())
            .unwrap_or_else(|| panic!("catcache: cache id {id} not registered"))
    }
}

#[inline]
pub(crate) fn pack_ref(off: u32, len: u32) -> Datum {
    Datum::from_usize(((off as usize) << 32) | len as usize)
}

/// Borrow a stored by-reference key's payload slice.
///
/// # Safety
/// `key` was written by `pack_ref` against this entry's live `payload`
/// allocation (insert-time invariant: `off + len <= payload_len`).
#[inline]
pub(crate) unsafe fn stored_bytes<'a>(payload: *const u8, key: Datum) -> &'a [u8] {
    let w = key.as_usize();
    let off = (w >> 32) as u32;
    let len = (w & 0xFFFF_FFFF) as u32;
    unsafe { core::slice::from_raw_parts(payload.add(off as usize), len as usize) }
}

/// `cc_fastequal[i](cachekeys[i], searchkeys[i])`, de-fmgr'd.
#[inline]
pub(crate) fn eq_stored(kind: CCFastKind, stored: Datum, payload: *const u8, probe: &CatCKey<'_>) -> bool {
    match kind {
        CCFastKind::Char => stored.as_char() == probe.word().as_char(),
        CCFastKind::Int2 => stored.as_i16() == probe.word().as_i16(),
        CCFastKind::Int4 => stored.as_i32() == probe.word().as_i32(),
        // SAFETY: stored by-ref keys always pack a live in-payload slice.
        CCFastKind::Name => compute::name_eq(unsafe { stored_bytes(payload, stored) }, probe.bytes()),
        // SAFETY: as above.
        CCFastKind::Text | CCFastKind::OidVector => {
            unsafe { stored_bytes(payload, stored) } == probe.bytes()
        }
    }
}

/// `CatalogCacheCompareTuple`.
#[inline]
pub(crate) fn compare_tuple(
    kinds: &[CCFastKind; 4],
    nkeys: i32,
    ct: &CatCTup,
    probes: &[CatCKey<'_>; 4],
) -> bool {
    for i in 0..nkeys as usize {
        if !eq_stored(kinds[i], ct.keys[i], ct.payload, &probes[i]) {
            return false;
        }
    }
    true
}

/// Allocate a stable payload buffer in the cache context (C's single palloc
/// of the CatCTup + tuple image).
pub(crate) fn payload_alloc(mcx: Mcx<'_>, len: usize) -> NonNull<u8> {
    use mcx::Allocator;
    let layout = core::alloc::Layout::from_size_align(len.max(1), 8).unwrap();
    mcx.allocate(layout)
        .unwrap_or_else(|_| panic!("{}", mcx.oom(len)))
        .cast()
}

pub(crate) fn payload_free(mcx: Mcx<'_>, ptr: *mut u8, len: u32) {
    use mcx::Allocator;
    if ptr.is_null() {
        return;
    }
    let layout = core::alloc::Layout::from_size_align((len as usize).max(1), 8).unwrap();
    // SAFETY: `ptr` came from `payload_alloc(mcx, len)` and is freed once
    // (CatCacheRemoveCTup/CList is the only caller and clears the slot).
    unsafe { mcx.deallocate(NonNull::new_unchecked(ptr), layout) };
}

pub fn init_seams() {
    catcache_seams::prepare_to_invalidate_cache_tuple::set(inval::PrepareToInvalidateCacheTuple);
    catcache_seams::catalog_cache_flush_catalog::set(graph::CatalogCacheFlushCatalog);
    catcache_seams::reset_catalog_caches_ext::set(graph::ResetCatalogCachesExt);
}
