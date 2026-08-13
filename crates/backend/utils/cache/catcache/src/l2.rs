//! D3.2 shared L2 catcache entries (docs/design/connection-scaling.md §D3).
//!
//! A `CatL2Entry` is the immutable, thread-shareable body of one catcache
//! entry: the `IMG_PREFIX`-prefixed tuple image (positive) or the copied
//! by-ref key payload (negative), plus the precomputed key datums. It lives
//! on the global heap inside an `Arc`, published in the process-global
//! `l2cache` map keyed by (cache id, database, hash, catalog generation).
//!
//! An L1 entry installed from L2 aliases the Arc's payload directly (its
//! `CatCTup::payload` points into the shared buffer and `CatCTup::shared`
//! holds the Arc) — the tuple image bytes exist once per process, not once
//! per backend. The L1 hit path is completely unchanged.

use core::ptr::NonNull;
use std::sync::Arc;

use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_tuple::{HeapTupleData, ItemPointerData};

use crate::compute::{CatCKey, CCFastKind};
use crate::graph::tuple_key;
use crate::{eq_stored, pack_ref, stored_bytes, with_state, CATCACHE_MAXKEYS, IMG_PREFIX};

/// 8-aligned (MAXALIGN) owned byte buffer on the global heap: tuple images
/// must be datum-aligned, which `Box<[u8]>` does not guarantee.
pub struct AlignedBytes {
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: uniquely owned global-heap allocation; no thread affinity.
unsafe impl Send for AlignedBytes {}
// SAFETY: contents are never mutated after construction.
unsafe impl Sync for AlignedBytes {}

impl AlignedBytes {
    fn layout(len: usize) -> core::alloc::Layout {
        core::alloc::Layout::from_size_align(len.max(1), 8).unwrap()
    }

    pub fn new_zeroed(len: usize) -> AlignedBytes {
        // SAFETY: non-zero-size layout (len.max(1)).
        let p = unsafe { std::alloc::alloc_zeroed(Self::layout(len)) };
        let Some(ptr) = NonNull::new(p) else {
            std::alloc::handle_alloc_error(Self::layout(len));
        };
        AlignedBytes { ptr, len }
    }

    #[inline]
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
}

impl Drop for AlignedBytes {
    fn drop(&mut self) {
        // SAFETY: allocated with the identical layout in new_zeroed.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), Self::layout(self.len)) };
    }
}

/// Immutable shared body of one catcache entry.
pub struct CatL2Entry {
    /// Key datums: by-value kinds hold the scalar word; by-ref kinds hold
    /// `pack_ref` (offset, len) into `payload` — position-independent.
    pub keys: [Datum; CATCACHE_MAXKEYS],
    pub negative: bool,
    pub t_len: u32,
    pub t_self: ItemPointerData,
    pub t_tableoid: Oid,
    /// Positive: `IMG_PREFIX` header + tuple image. Negative: by-ref key bytes.
    pub payload: AlignedBytes,
}

// SAFETY: immutable after construction; `keys` carry scalar words or
// payload-relative (offset, len) words, never cross-thread pointers.
unsafe impl Send for CatL2Entry {}
// SAFETY: as above — read-only shared state.
unsafe impl Sync for CatL2Entry {}

impl CatL2Entry {
    #[inline]
    pub fn approx_bytes(&self) -> usize {
        core::mem::size_of::<CatL2Entry>() + self.payload.len()
    }

    /// Borrow this entry's logical keys as probe keys (for prune matching).
    pub(crate) fn probe_keys(&self, kinds: &[CCFastKind; 4], nkeys: i32) -> [CatCKey<'_>; 4] {
        let mut out = [CatCKey::UNUSED; 4];
        for i in 0..nkeys as usize {
            out[i] = match kinds[i] {
                CCFastKind::Char | CCFastKind::Int2 | CCFastKind::Int4 => {
                    CatCKey::Value(self.keys[i])
                }
                // SAFETY: by-ref keys were packed against this payload.
                _ => CatCKey::Bytes(unsafe { stored_bytes(self.payload.as_ptr(), self.keys[i]) }),
            };
        }
        out
    }
}

/// Full logical-key comparison for L2 lookup/prune (hash collisions share a
/// map bucket, exactly like an L1 bucket walk).
pub(crate) fn entry_matches(
    any: &(dyn core::any::Any + Send + Sync),
    kinds: &[CCFastKind; 4],
    nkeys: i32,
    probes: &[CatCKey<'_>; 4],
) -> bool {
    let Some(e) = any.downcast_ref::<CatL2Entry>() else {
        return false;
    };
    for i in 0..nkeys as usize {
        if !eq_stored(kinds[i], e.keys[i], e.payload.as_ptr(), &probes[i]) {
            return false;
        }
    }
    true
}

/// `CatalogCacheCreateEntry` (positive), L2 shape: identical image copy and
/// key computation as `create_entry_positive`, but into a shareable buffer.
pub(crate) fn build_positive(cache_id: i32, ntp: &HeapTupleData<'_>) -> PgResult<Arc<CatL2Entry>> {
    debug_assert!(!ntp.has_external(), "caller flattens via l2_entry_from_scan");
    let (tupdesc, nkeys, kinds, keyno) = with_state(|st| {
        let c = st.cache(cache_id);
        (c.cc_tupdesc, c.cc_nkeys, c.cc_kind, c.cc_keyno)
    });
    let tupdesc = tupdesc.expect("catcache: entry created before phase-2 init");

    let t_len = ntp.t_len;
    let buf = AlignedBytes::new_zeroed(IMG_PREFIX + t_len as usize);
    // SAFETY: fresh IMG_PREFIX + t_len bytes; source image live for t_len.
    let image = unsafe {
        let p = buf.as_ptr();
        core::ptr::write(p.cast::<ItemPointerData>(), ntp.t_self);
        core::ptr::write(p.add(8).cast::<Oid>(), ntp.t_tableOid);
        core::ptr::write(p.add(12).cast::<u32>(), t_len);
        let image = p.add(IMG_PREFIX);
        core::ptr::copy_nonoverlapping(ntp.header_ptr(), image, t_len as usize);
        image
    };
    // SAFETY: `image` now holds a valid tuple image (verbatim copy).
    let cached_view: HeapTupleData<'_> =
        unsafe { HeapTupleData::from_raw_parts(image, t_len, ntp.t_self, ntp.t_tableOid) };

    let mut keys = [Datum::null(); CATCACHE_MAXKEYS];
    for i in 0..nkeys as usize {
        keys[i] = match tuple_key(kinds[i], &cached_view, keyno[i], tupdesc) {
            CatCKey::Value(d) => d,
            CatCKey::Bytes(b) => {
                let off = b.as_ptr() as usize - buf.as_ptr() as usize;
                pack_ref(off as u32, b.len() as u32)
            }
            CatCKey::Str(_) => unreachable!(),
        };
    }

    Ok(Arc::new(CatL2Entry {
        keys,
        negative: false,
        t_len,
        t_self: ntp.t_self,
        t_tableoid: ntp.t_tableOid,
        payload: buf,
    }))
}

/// `CatalogCacheCreateEntry` (negative), L2 shape (`CatCacheCopyKeys`).
pub(crate) fn build_negative(cache_id: i32, probes: &[CatCKey<'_>; 4]) -> Arc<CatL2Entry> {
    let (nkeys, kinds) = with_state(|st| {
        let c = st.cache(cache_id);
        (c.cc_nkeys, c.cc_kind)
    });
    let mut byref_len = 0usize;
    for i in 0..nkeys as usize {
        if matches!(kinds[i], CCFastKind::Name | CCFastKind::Text | CCFastKind::OidVector) {
            byref_len += probes[i].bytes().len();
        }
    }
    let buf = AlignedBytes::new_zeroed(byref_len);
    let mut keys = [Datum::null(); CATCACHE_MAXKEYS];
    let mut off = 0usize;
    for i in 0..nkeys as usize {
        keys[i] = match kinds[i] {
            CCFastKind::Char | CCFastKind::Int2 | CCFastKind::Int4 => probes[i].word(),
            _ => {
                let b = probes[i].bytes();
                // SAFETY: `buf` has room for all by-ref payloads (summed above).
                unsafe {
                    core::ptr::copy_nonoverlapping(b.as_ptr(), buf.as_ptr().add(off), b.len());
                }
                let k = pack_ref(off as u32, b.len() as u32);
                off += b.len();
                k
            }
        };
    }
    Arc::new(CatL2Entry {
        keys,
        negative: true,
        t_len: 0,
        t_self: ItemPointerData::invalid(),
        t_tableoid: 0,
        payload: buf,
    })
}
