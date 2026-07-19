#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod compute;
mod graph;
mod init;
mod inval;
mod list;
mod search;
pub mod testing;
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
    inval_epoch, CatCacheInvalidate, CatalogCacheFlushCatalog, InitCatCache, ResetCatalogCaches,
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

/// `CatCTup`; a by-ref key slot packs `(off << 32) | len` into `payload`.
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
    /// Stable allocation (entries move on slot-vec growth, this never
    /// does); positive entries: `IMG_PREFIX` header, then the image.
    pub payload: *mut u8,
    pub payload_len: u32,
}

/// C's in-entry HeapTupleData: t_self +0, t_tableoid +8, t_len +12, image +16.
pub(crate) const IMG_PREFIX: usize = 16;

impl CatCTup {
    /// Tuple image pointer of a live POSITIVE entry.
    #[inline(always)]
    pub(crate) fn image_ptr(&self) -> *mut u8 {
        debug_assert!(!self.negative && !self.payload.is_null());
        // SAFETY: positive-entry payloads are IMG_PREFIX + t_len bytes.
        unsafe { self.payload.add(IMG_PREFIX) }
    }
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

/// `CacheHdr` + `SysCache[]` + in-progress stack; indexed by syscache id.
pub(crate) struct CatCacheState<'mcx> {
    pub mcx: Mcx<'mcx>,
    pub caches: PgVec<'mcx, Option<CatCache<'mcx>>>,
    pub ch_ntup: i32,
    pub in_progress: PgVec<'mcx, CatCInProgress>,
}

bind!(pub(crate) CatCacheStateTy => CatCacheState<'mcx>);

thread_local! {
    // UnsafeCell, not RefCell: borrow-flag traffic on every catalog lookup
    // is overhead C does not pay (fabled #292, ~12% suite-wide).
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
    // Session-memory teardown (FPBUDGET-1): C frees the whole catcache with
    // the backend process; the thread model frees it here or every session
    // leaks its catalog-cache estate into the shared process.
    ::mcx::register_session_cleanup(Box::new(|| {
        STATE.with(|cell| {
            // SAFETY: task-end teardown, outside any catcache borrow.
            if let Some(owned) = unsafe { &mut *cell.get() }.take() {
                drop(ManuallyDrop::into_inner(owned));
            }
        });
    }));
}

/// Run `f` with `&mut` state (C reaches `SysCache[]` through bare pointers).
///
/// # Safety
///
/// Never re-entered while a borrow is live: one single-threaded backend owns
/// the thread-local; every operation confines its borrow to one `f` and
/// drops it before any call that can re-enter (init, miss scan, callbacks).
/// A pure hit calls no foreign code inside `f`. The debug/Miri guard turns
/// any violation into a panic.
#[inline(always)]
pub(crate) fn with_state<R>(f: impl for<'mcx> FnOnce(&mut CatCacheState<'mcx>) -> R) -> R {
    // Tiny closure: LocalKey::with outlines big ones (an extra call frame).
    let cell = STATE.with(|cell| cell as *const UnsafeCell<Option<ManuallyDrop<McxOwned<CatCacheStateTy>>>>);
    #[cfg(debug_assertions)]
    let _guard = {
        BORROW_DEPTH.with(|d| {
            assert_eq!(d.get(), 0, "catcache state re-entered while a borrow is live");
            d.set(1);
        });
        BorrowGuard
    };
    // SAFETY: `cell` is this thread's own TLS slot, used only within this
    // call; single-statement, non-reentrant borrow (see above).
    let slot = unsafe { &mut *(*cell).get() };
    if slot.is_none() {
        state_init(slot);
    }
    slot.as_mut().unwrap().with_mut(f)
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
    // Full 8-byte Datum word (u64, not usize: a usize shift truncates on
    // 32-bit wasm — ILP32 Datum-word audit). Identical on 64-bit targets.
    Datum::from_u64(((off as u64) << 32) | len as u64)
}

/// # Safety
/// `key` was written by `pack_ref` against this entry's live `payload`
/// allocation (insert-time invariant: `off + len <= payload_len`).
#[inline]
pub(crate) unsafe fn stored_bytes<'a>(payload: *const u8, key: Datum) -> &'a [u8] {
    let w = key.as_u64();
    let off = (w >> 32) as u32;
    let len = (w & 0xFFFF_FFFF) as u32;
    unsafe { core::slice::from_raw_parts(payload.add(off as usize), len as usize) }
}

/// `cc_fastequal[i]`, de-fmgr'd; split like `fast_hash_probe` — canonical
/// word datums are one inline low-32 compare, slice compares one call.
#[inline(always)]
pub(crate) fn eq_stored(kind: CCFastKind, stored: Datum, payload: *const u8, probe: &CatCKey<'_>) -> bool {
    match kind {
        CCFastKind::Char | CCFastKind::Int2 | CCFastKind::Int4 => {
            stored.as_i32() == probe.word().as_i32()
        }
        _ => eq_stored_bytes_outlined(kind, stored, payload, probe),
    }
}

#[inline(never)]
fn eq_stored_bytes_outlined(
    kind: CCFastKind,
    stored: Datum,
    payload: *const u8,
    probe: &CatCKey<'_>,
) -> bool {
    match kind {
        // SAFETY: stored by-ref keys always pack a live in-payload slice.
        CCFastKind::Name => compute::name_eq(unsafe { stored_bytes(payload, stored) }, probe.bytes()),
        CCFastKind::Text | CCFastKind::OidVector => {
            // SAFETY: as above.
            let s = unsafe { stored_bytes(payload, stored) };
            s == probe.bytes()
        }
        _ => unreachable!("word kinds are handled inline"),
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

/// Stable payload buffer in the cache context (C's palloc of the image).
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
