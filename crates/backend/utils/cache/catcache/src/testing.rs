//! Fixture builders for tests and the paired C microbench: build a warm
//! in-memory cache without the relcache/genam substrate (phase-2 init and
//! the miss scan are bypassed; the HIT path under test is the real one).
#![doc(hidden)]

use datum::Datum;
use types_tuple::TupleDescData;

use crate::compute::{compute_hash_value, hash_index, CatCKey, CCFastKind};
use crate::graph::create_entry_negative;
use crate::{pack_ref, payload_alloc, with_state, CatCTup, CATCACHE_MAXKEYS, NONE};

pub fn init_cache_bare(
    id: i32,
    nkeys: i32,
    kinds: [CCFastKind; CATCACHE_MAXKEYS],
    nbuckets: i32,
    tupdesc: Option<&'static TupleDescData<'static>>,
) {
    let keyno = [1, 2, 3, 4];
    crate::graph::InitCatCache(id, 1, 2, nkeys, &keyno, nbuckets).unwrap();
    with_state(|st| {
        let c = st.cache_mut(id);
        c.cc_kind = kinds;
        c.cc_tupdesc = tupdesc;
        c.initialized = true;
    });
}

/// Insert a positive entry (payload = image ++ owned by-ref key copies).
pub fn insert_positive(cache_id: i32, keys: &[CatCKey<'_>; 4], image: &[u8]) {
    with_state(|st| {
        let cache = st.cache(cache_id);
        let (nkeys, kinds) = (cache.cc_nkeys, cache.cc_kind);
        let hash_value = compute_hash_value(&kinds, nkeys, keys);

        let mut byref_len = 0usize;
        for i in 0..nkeys as usize {
            if matches!(kinds[i], CCFastKind::Name | CCFastKind::Text | CCFastKind::OidVector) {
                byref_len += keys[i].bytes().len();
            }
        }
        let t_self = types_tuple::ItemPointerData::new(0, 1);
        let t_tableoid: u32 = 1;
        let total = crate::IMG_PREFIX + image.len() + byref_len;
        let buf = payload_alloc(st.mcx, total);
        // SAFETY: fresh `total`-byte buffer; prefix layout per IMG_PREFIX.
        unsafe {
            let p = buf.as_ptr();
            core::ptr::write_bytes(p, 0, crate::IMG_PREFIX);
            core::ptr::write(p.cast::<types_tuple::ItemPointerData>(), t_self);
            core::ptr::write(p.add(8).cast::<u32>(), t_tableoid);
            core::ptr::write(p.add(12).cast::<u32>(), image.len() as u32);
            core::ptr::copy_nonoverlapping(image.as_ptr(), p.add(crate::IMG_PREFIX), image.len());
        }
        let mut off = crate::IMG_PREFIX + image.len();
        let mut key_words = [Datum::null(); CATCACHE_MAXKEYS];
        for i in 0..nkeys as usize {
            key_words[i] = match kinds[i] {
                CCFastKind::Char | CCFastKind::Int2 | CCFastKind::Int4 => keys[i].word(),
                _ => {
                    let b = keys[i].bytes();
                    // SAFETY: buf sized image + summed by-ref payloads.
                    unsafe {
                        core::ptr::copy_nonoverlapping(b.as_ptr(), buf.as_ptr().add(off), b.len());
                    }
                    let k = pack_ref(off as u32, b.len() as u32);
                    off += b.len();
                    k
                }
            };
        }

        let ct = CatCTup {
            hash_value,
            refcount: 0,
            dead: false,
            negative: false,
            next: NONE,
            prev: NONE,
            c_list: NONE,
            keys: key_words,
            t_len: image.len() as u32,
            t_self,
            t_tableoid,
            payload: buf.as_ptr(),
            payload_len: total as u32,
        };
        let cache = st.cache_mut(cache_id);
        let slot = cache.ct_alloc(ct);
        let bi = hash_index(hash_value, cache.cc_nbuckets);
        cache.ct_push_head(bi, slot);
        cache.cc_ntup += 1;
        st.ch_ntup += 1;
    });
}

pub fn insert_negative(cache_id: i32, keys: &[CatCKey<'_>; 4]) {
    with_state(|st| {
        let cache = st.cache(cache_id);
        let hash_value = compute_hash_value(&cache.cc_kind, cache.cc_nkeys, keys);
        create_entry_negative(st, cache_id, keys, hash_value).unwrap();
    });
}

pub fn cache_ntup(cache_id: i32) -> i32 {
    with_state(|st| st.cache(cache_id).cc_ntup)
}

/// Phase-2 bypass for caches registered through the real table.
pub fn force_initialized(id: i32, kinds: [CCFastKind; CATCACHE_MAXKEYS]) {
    with_state(|st| {
        let c = st.cache_mut(id);
        c.cc_kind = kinds;
        c.initialized = true;
    });
}

pub fn set_tupdesc(id: i32, tupdesc: &'static TupleDescData<'static>) {
    with_state(|st| {
        st.cache_mut(id).cc_tupdesc = Some(tupdesc);
    });
}
