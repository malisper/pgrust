use datum::Datum;
use mcx::PgVec;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_DATATYPE_MISMATCH};
use types_tuple::varatt;
use types_tuple::{HeapTupleData, TupleDescData};

use crate::compute::{compute_hash_value, hash_index, CatCKey, CCFastKind, NAMEDATALEN};
use crate::{
    pack_ref, payload_alloc, payload_free, with_state, CatCInProgress, CatCTup, CatCache,
    CatCacheState, CATCACHE_MAXKEYS, NONE,
};

impl<'mcx> CatCache<'mcx> {
    #[inline]
    pub(crate) fn ct_push_head(&mut self, bucket: usize, slot: u32) {
        let head = self.cc_bucket[bucket];
        {
            let ct = &mut self.tuples[slot as usize];
            ct.prev = NONE;
            ct.next = head;
        }
        if head != NONE {
            self.tuples[head as usize].prev = slot;
        }
        self.cc_bucket[bucket] = slot;
    }

    #[inline]
    pub(crate) fn ct_unlink(&mut self, bucket: usize, slot: u32) {
        let (prev, next) = {
            let ct = &self.tuples[slot as usize];
            (ct.prev, ct.next)
        };
        if prev == NONE {
            self.cc_bucket[bucket] = next;
        } else {
            self.tuples[prev as usize].next = next;
        }
        if next != NONE {
            self.tuples[next as usize].prev = prev;
        }
    }

    /// Hit-path `dlist_move_head`, inline and unchecked like C's.
    ///
    /// # Safety
    /// `bucket` is the masked index of the bucket `slot` was walked from.
    #[inline(always)]
    pub(crate) unsafe fn ct_move_head_hot(&mut self, bucket: usize, slot: u32) {
        unsafe {
            let head = *self.cc_bucket.get_unchecked(bucket);
            if head == slot {
                return;
            }
            // SAFETY: slot != head implies prev != NONE and head != NONE.
            let (prev, next) = {
                let ct = self.tuples.get_unchecked(slot as usize);
                (ct.prev, ct.next)
            };
            self.tuples.get_unchecked_mut(prev as usize).next = next;
            if next != NONE {
                self.tuples.get_unchecked_mut(next as usize).prev = prev;
            }
            {
                let ct = self.tuples.get_unchecked_mut(slot as usize);
                ct.prev = NONE;
                ct.next = head;
            }
            self.tuples.get_unchecked_mut(head as usize).prev = slot;
            *self.cc_bucket.get_unchecked_mut(bucket) = slot;
        }
    }

    pub(crate) fn ct_alloc(&mut self, ct: CatCTup) -> u32 {
        if self.ct_free != NONE {
            let slot = self.ct_free;
            self.ct_free = self.tuples[slot as usize].next;
            self.tuples[slot as usize] = ct;
            slot
        } else {
            self.tuples.push(ct);
            (self.tuples.len() - 1) as u32
        }
    }

    pub(crate) fn ct_slot_free(&mut self, slot: u32) {
        let ct = &mut self.tuples[slot as usize];
        ct.payload = core::ptr::null_mut();
        ct.payload_len = 0;
        ct.shared = None;
        ct.next = self.ct_free;
        ct.prev = NONE;
        ct.refcount = 0;
        self.ct_free = slot;
    }

    #[inline]
    pub(crate) fn cl_push_head(&mut self, bucket: usize, slot: u32) {
        let head = self.cc_lbucket[bucket];
        {
            let cl = &mut self.lists[slot as usize];
            cl.prev = NONE;
            cl.next = head;
        }
        if head != NONE {
            self.lists[head as usize].prev = slot;
        }
        self.cc_lbucket[bucket] = slot;
    }

    pub(crate) fn cl_unlink(&mut self, bucket: usize, slot: u32) {
        let (prev, next) = {
            let cl = &self.lists[slot as usize];
            (cl.prev, cl.next)
        };
        if prev == NONE {
            self.cc_lbucket[bucket] = next;
        } else {
            self.lists[prev as usize].next = next;
        }
        if next != NONE {
            self.lists[next as usize].prev = prev;
        }
    }

    pub(crate) fn cl_move_head(&mut self, bucket: usize, slot: u32) {
        if self.cc_lbucket[bucket] == slot {
            return;
        }
        self.cl_unlink(bucket, slot);
        self.cl_push_head(bucket, slot);
    }

    pub(crate) fn cl_alloc(&mut self, cl: crate::CatCList<'mcx>) -> u32 {
        if self.cl_free != NONE {
            let slot = self.cl_free;
            self.cl_free = self.lists[slot as usize].next;
            self.lists[slot as usize] = cl;
            slot
        } else {
            self.lists.push(cl);
            (self.lists.len() - 1) as u32
        }
    }

    pub(crate) fn cl_slot_free(&mut self, slot: u32) {
        let cl = &mut self.lists[slot as usize];
        cl.payload = core::ptr::null_mut();
        cl.payload_len = 0;
        cl.members.clear();
        cl.next = self.cl_free;
        cl.prev = NONE;
        cl.refcount = 0;
        self.cl_free = slot;
    }
}

pub fn InitCatCache(
    id: i32,
    reloid: Oid,
    indexoid: Oid,
    nkeys: i32,
    key: &[i32],
    nbuckets: i32,
) -> PgResult<()> {
    debug_assert!(nbuckets > 0 && (nbuckets & (nbuckets - 1)) == 0);
    let mut cc_keyno = [0i32; CATCACHE_MAXKEYS];
    for i in 0..nkeys as usize {
        debug_assert!(key[i] != 0);
        cc_keyno[i] = key[i];
    }

    with_state(|st| {
        let mcx = st.mcx;
        let mut bucket = PgVec::new_in(mcx);
        bucket.resize(nbuckets as usize, NONE);
        let cache = CatCache {
            id,
            cc_reloid: reloid,
            cc_indexoid: indexoid,
            cc_relisshared: false,
            initialized: false,
            cc_ntup: 0,
            cc_nlist: 0,
            cc_nbuckets: nbuckets as u32,
            cc_nlbuckets: 0,
            cc_nkeys: nkeys,
            cc_keyno,
            cc_kind: [CCFastKind::Int4; CATCACHE_MAXKEYS],
            cc_eqfunc: [0; CATCACHE_MAXKEYS],
            cc_tupdesc: None,
            cc_relname: None,
            cc_bucket: bucket,
            cc_lbucket: PgVec::new_in(mcx),
            tuples: PgVec::new_in(mcx),
            ct_free: NONE,
            lists: PgVec::new_in(mcx),
            cl_free: NONE,
        };
        let idx = id as usize;
        if st.caches.len() <= idx {
            st.caches.resize_with(idx + 1, || None);
        }
        assert!(st.caches[idx].is_none(), "catcache: cache id {id} registered twice");
        st.caches[idx] = Some(cache);
    });
    Ok(())
}

pub(crate) fn rehash_cat_cache<'mcx>(mcx: mcx::Mcx<'mcx>, cache: &mut CatCache<'mcx>) {
    let newn = cache.cc_nbuckets * 2;
    let mut newbucket: PgVec<'mcx, u32> = PgVec::new_in(mcx);
    newbucket.resize(newn as usize, NONE);
    let old = core::mem::replace(&mut cache.cc_bucket, newbucket);
    cache.cc_nbuckets = newn;
    for i in 0..old.len() {
        let mut cur = old[i];
        while cur != NONE {
            let (next, hv) = {
                let ct = &cache.tuples[cur as usize];
                (ct.next, ct.hash_value)
            };
            let bi = hash_index(hv, newn);
            cache.ct_push_head(bi, cur);
            cur = next;
        }
    }
}

pub(crate) fn rehash_cat_cache_lists<'mcx>(mcx: mcx::Mcx<'mcx>, cache: &mut CatCache<'mcx>) {
    let newn = cache.cc_nlbuckets * 2;
    let mut newbucket: PgVec<'mcx, u32> = PgVec::new_in(mcx);
    newbucket.resize(newn as usize, NONE);
    let old = core::mem::replace(&mut cache.cc_lbucket, newbucket);
    cache.cc_nlbuckets = newn;
    for i in 0..old.len() {
        let mut cur = old[i];
        while cur != NONE {
            let (next, hv) = {
                let cl = &cache.lists[cur as usize];
                (cl.next, cl.hash_value)
            };
            let bi = hash_index(hv, newn);
            cache.cl_push_head(bi, cur);
            cur = next;
        }
    }
}

/// `CatCacheRemoveCTup`.
pub(crate) fn remove_ct(st: &mut CatCacheState<'_>, cache_id: i32, slot: u32) {
    let (c_list, hv, payload, payload_len, is_shared) = {
        let cache = st.cache(cache_id);
        let ct = &cache.tuples[slot as usize];
        debug_assert_eq!(ct.refcount, 0);
        (ct.c_list, ct.hash_value, ct.payload, ct.payload_len, ct.shared.is_some())
    };
    if c_list != NONE {
        st.cache_mut(cache_id).tuples[slot as usize].dead = true;
        remove_cl(st, cache_id, c_list);
        return;
    }
    let mcx = st.mcx;
    let cache = st.cache_mut(cache_id);
    let bi = hash_index(hv, cache.cc_nbuckets);
    cache.ct_unlink(bi, slot);
    if !is_shared {
        payload_free(mcx, payload, payload_len);
    }
    // Shared payloads: ct_slot_free drops the Arc (the buffer lives while any
    // thread's L1 or the L2 map still references it).
    cache.ct_slot_free(slot);
    cache.cc_ntup -= 1;
    st.ch_ntup -= 1;
}

/// `CatCacheRemoveCList`.
pub(crate) fn remove_cl(st: &mut CatCacheState<'_>, cache_id: i32, slot: u32) {
    let (n, hv, payload, payload_len) = {
        let cl = &st.cache(cache_id).lists[slot as usize];
        debug_assert_eq!(cl.refcount, 0);
        (cl.members.len(), cl.hash_value, cl.payload, cl.payload_len)
    };
    for i in (0..n).rev() {
        let m = st.cache(cache_id).lists[slot as usize].members[i];
        let (dead, refcount) = {
            let ct = &mut st.cache_mut(cache_id).tuples[m as usize];
            debug_assert_eq!(ct.c_list, slot);
            ct.c_list = NONE;
            (ct.dead, ct.refcount)
        };
        if dead && refcount == 0 {
            remove_ct(st, cache_id, m);
        }
    }
    let mcx = st.mcx;
    let cache = st.cache_mut(cache_id);
    let bi = hash_index(hv, cache.cc_nlbuckets);
    cache.cl_unlink(bi, slot);
    payload_free(mcx, payload, payload_len);
    cache.cl_slot_free(slot);
    cache.cc_nlist -= 1;
}

// `CatCacheInvalidate(SysCache[cacheId], hashValue)`.
// Bumped on every path that can change what a syscache probe returns;
// downstream decode-once memos (cache_syscache shape carriers) key on it.
thread_local! {
    static INVAL_EPOCH: core::cell::Cell<u64> = const { core::cell::Cell::new(0) };
}

pub fn inval_epoch() -> u64 {
    INVAL_EPOCH.get()
}

fn bump_inval_epoch() {
    INVAL_EPOCH.set(INVAL_EPOCH.get() + 1);
}

pub fn CatCacheInvalidate(cache_id: i32, hash_value: u32) {
    bump_inval_epoch();
    with_state(|st| {
        if st.caches.get(cache_id as usize).map(|c| c.is_none()).unwrap_or(true) {
            return;
        }
        invalidate_one(st, cache_id, hash_value);
    });
}

fn list_refcount(cache: &CatCache<'_>, cl: u32) -> i32 {
    if cl == NONE {
        0
    } else {
        cache.lists[cl as usize].refcount
    }
}

pub(crate) fn invalidate_one(st: &mut CatCacheState<'_>, cache_id: i32, hash_value: u32) {
    /* Invalidate *all* CatCLists in this cache */
    let nlbuckets = st.cache(cache_id).cc_nlbuckets;
    for bi in 0..nlbuckets as usize {
        let mut cur = st.cache(cache_id).cc_lbucket[bi];
        while cur != NONE {
            let (next, refcount) = {
                let cl = &st.cache(cache_id).lists[cur as usize];
                (cl.next, cl.refcount)
            };
            if refcount > 0 {
                st.cache_mut(cache_id).lists[cur as usize].dead = true;
            } else {
                remove_cl(st, cache_id, cur);
            }
            cur = next;
        }
    }

    let bi = hash_index(hash_value, st.cache(cache_id).cc_nbuckets);
    let mut cur = st.cache(cache_id).cc_bucket[bi];
    while cur != NONE {
        let (next, hv, refcount, c_list) = {
            let ct = &st.cache(cache_id).tuples[cur as usize];
            (ct.next, ct.hash_value, ct.refcount, ct.c_list)
        };
        if hv == hash_value {
            if refcount > 0 || list_refcount(st.cache(cache_id), c_list) > 0 {
                st.cache_mut(cache_id).tuples[cur as usize].dead = true;
            } else {
                remove_ct(st, cache_id, cur);
            }
            /* could be multiple matches, so keep looking! */
        }
        cur = next;
    }

    for e in st.in_progress.iter_mut() {
        if e.cache_id == cache_id && (e.list || e.hash_value == hash_value) {
            e.dead = true;
        }
    }
}

pub(crate) fn reset_catalog_cache(st: &mut CatCacheState<'_>, cache_id: i32, debug_discard: bool) {
    let nlbuckets = st.cache(cache_id).cc_nlbuckets;
    for bi in 0..nlbuckets as usize {
        let mut cur = st.cache(cache_id).cc_lbucket[bi];
        while cur != NONE {
            let (next, refcount) = {
                let cl = &st.cache(cache_id).lists[cur as usize];
                (cl.next, cl.refcount)
            };
            if refcount > 0 {
                st.cache_mut(cache_id).lists[cur as usize].dead = true;
            } else {
                remove_cl(st, cache_id, cur);
            }
            cur = next;
        }
    }
    let nbuckets = st.cache(cache_id).cc_nbuckets;
    for bi in 0..nbuckets as usize {
        let mut cur = st.cache(cache_id).cc_bucket[bi];
        while cur != NONE {
            let (next, refcount, c_list) = {
                let ct = &st.cache(cache_id).tuples[cur as usize];
                (ct.next, ct.refcount, ct.c_list)
            };
            if refcount > 0 || list_refcount(st.cache(cache_id), c_list) > 0 {
                st.cache_mut(cache_id).tuples[cur as usize].dead = true;
            } else {
                remove_ct(st, cache_id, cur);
            }
            cur = next;
        }
    }
    if !debug_discard {
        for e in st.in_progress.iter_mut() {
            if e.cache_id == cache_id {
                e.dead = true;
            }
        }
    }
}

pub fn ResetCatalogCaches() -> PgResult<()> {
    ResetCatalogCachesExt(false)
}

pub fn ResetCatalogCachesExt(debug_discard: bool) -> PgResult<()> {
    bump_inval_epoch();
    with_state(|st| {
        for id in 0..st.caches.len() {
            if st.caches[id].is_some() {
                reset_catalog_cache(st, id as i32, debug_discard);
            }
        }
    });
    Ok(())
}

pub(crate) const MAX_CACHES: usize = 96;

/// `CatalogCacheFlushCatalog(catId)`.
pub fn CatalogCacheFlushCatalog(cat_id: Oid) -> PgResult<()> {
    bump_inval_epoch();
    let mut targets = [0i32; MAX_CACHES];
    let n = with_state(|st| {
        let mut n = 0;
        for c in st.caches.iter().flatten() {
            if c.cc_reloid == cat_id {
                targets[n] = c.id;
                n += 1;
            }
        }
        n
    });
    for &id in &targets[..n] {
        with_state(|st| reset_catalog_cache(st, id, false));
        // Callbacks re-enter arbitrarily; no state borrow held.
        inval::invalidate::CallSyscacheCallbacks(id, 0)?;
    }
    Ok(())
}

/// # Safety
/// `p` points at a live inline (short or 4-byte header) varlena image;
/// externals are rejected before entry creation.
pub(crate) unsafe fn varlena_payload(p: *const u8) -> (*const u8, usize) {
    let b = unsafe { *p };
    if b != 0x01 && (b & 0x01) == 0x01 {
        let total = ((b >> 1) & 0x7F) as usize;
        (unsafe { p.add(1) }, total.saturating_sub(varatt::VARHDRSZ_SHORT))
    } else {
        let word = unsafe { core::ptr::read_unaligned(p.cast::<u32>()) };
        let total = varatt::varsize_4b_word(word) as usize;
        (unsafe { p.add(varatt::VARHDRSZ) }, total.saturating_sub(varatt::VARHDRSZ))
    }
}

/// # Safety
/// `p` points at a live, plain-storage (4-byte header) oidvector image;
/// `hashoidvector` hashes `values, dim1 * 4` bytes.
///
/// The on-image `dim1` (offset 16) is attacker-controllable in crafted catalog
/// pages: a negative value sign-extends and `* 4` wraps (release builds have no
/// overflow checks), and an inflated value drives the resulting `from_raw_parts`
/// values slice past the image (OOB read / UB). C's `hashoidvector` /
/// `oidvectoreq` gate on `check_valid_oidvector`; we re-derive the element-count
/// bound from the datum's own VARSIZE (the header is at offset 0 for a 4B-U
/// plain-storage oidvector) with `array::vector_dim1_fits`, exactly as the
/// adt/nbtree oidvector paths do, and surface the same
/// "array is not a valid oidvector" error instead of walking out of bounds.
pub(crate) unsafe fn oidvector_elements(p: *const u8) -> PgResult<(*const u8, usize)> {
    let dim1 = unsafe { core::ptr::read_unaligned(p.add(16).cast::<i32>()) };
    // SAFETY: 4B-U plain-storage oidvector datum; header readable for VARSIZE.
    let varsize = unsafe { datum::varlena::VarlenaRef::from_ptr(p) }.varsize();
    if !array::vector_dim1_fits(varsize, dim1, core::mem::size_of::<Oid>()) {
        return Err(Box::new(
            PgError::error("array is not a valid oidvector")
                .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
        ));
    }
    // `vector_dim1_fits` treats a negative dim1 as an empty vector; clamp the
    // element count the same way (matching the adt/nbtree `dim1.max(0)` slicing)
    // so `* 4` can never wrap a sign-extended negative into a huge length.
    Ok((unsafe { p.add(24) }, dim1.max(0) as usize * 4))
}

/// # Safety
/// `p` points at a live NameData / NUL-terminated key image.
pub(crate) unsafe fn name_payload(p: *const u8) -> (*const u8, usize) {
    let mut len = 0usize;
    while len < NAMEDATALEN && unsafe { *p.add(len) } != 0 {
        len += 1;
    }
    (p, len)
}

/// One key column of `tuple` as a borrowed probe key.
pub(crate) fn tuple_key<'a>(
    kind: CCFastKind,
    tuple: &HeapTupleData<'a>,
    attnum: i32,
    tupdesc: &TupleDescData<'_>,
) -> PgResult<CatCKey<'a>> {
    let mut isnull = false;
    // SAFETY: catcache key columns are user columns of the cache's own
    // catalog descriptor; NULL keys are impossible (NOT NULL catalog keys).
    let d = unsafe { types_tuple::heap_getattr(tuple, attnum, tupdesc, &mut isnull) };
    debug_assert!(!isnull);
    Ok(match kind {
        CCFastKind::Char | CCFastKind::Int2 | CCFastKind::Int4 => CatCKey::Value(d),
        CCFastKind::Name => {
            // SAFETY: by-ref datum points into the live tuple image.
            let (p, len) = unsafe { name_payload(d.as_usize() as *const u8) };
            CatCKey::Bytes(unsafe { core::slice::from_raw_parts(p, len) })
        }
        CCFastKind::Text => {
            // SAFETY: as above; catalog text keys are inline post-flatten.
            let (p, len) = unsafe { varlena_payload(d.as_usize() as *const u8) };
            CatCKey::Bytes(unsafe { core::slice::from_raw_parts(p, len) })
        }
        CCFastKind::OidVector => {
            // SAFETY: as above; oidvector is plain storage (4-byte header).
            // `oidvector_elements` validates dim1 against VARSIZE before the
            // slice is formed, rejecting a crafted image instead of over-reading.
            let (p, len) = unsafe { oidvector_elements(d.as_usize() as *const u8) }?;
            CatCKey::Bytes(unsafe { core::slice::from_raw_parts(p, len) })
        }
    })
}

/// `CatalogCacheComputeTupleHashValue`.
pub(crate) fn compute_tuple_hash_value(
    kinds: &[CCFastKind; 4],
    nkeys: i32,
    keyno: &[i32; 4],
    tupdesc: &TupleDescData<'_>,
    tuple: &HeapTupleData<'_>,
) -> PgResult<u32> {
    let mut keys = [CatCKey::UNUSED; 4];
    for i in 0..nkeys as usize {
        keys[i] = tuple_key(kinds[i], tuple, keyno[i], tupdesc)?;
    }
    Ok(compute_hash_value(kinds, nkeys, &keys))
}

/// `CatalogCacheCreateEntry` (positive): the entry slot is linked at its
/// bucket head with refcount 0; keys point into the copied image.
pub(crate) fn create_entry_positive(
    st: &mut CatCacheState<'_>,
    cache_id: i32,
    ntp: &HeapTupleData<'_>,
    hash_value: u32,
) -> PgResult<u32> {
    debug_assert!(!ntp.has_external(), "caller flattens via create_entry_from_scan");
    let mcx = st.mcx;
    let t_len = ntp.t_len;
    let payload_len = crate::IMG_PREFIX + t_len as usize;
    let buf = payload_alloc(mcx, payload_len);
    // SAFETY: fresh IMG_PREFIX + t_len bytes; source image live for t_len.
    let image = unsafe {
        let p = buf.as_ptr();
        core::ptr::write_bytes(p, 0, crate::IMG_PREFIX);
        core::ptr::write(p.cast::<types_tuple::ItemPointerData>(), ntp.t_self);
        core::ptr::write(p.add(8).cast::<Oid>(), ntp.t_tableOid);
        core::ptr::write(p.add(12).cast::<u32>(), t_len);
        let image = p.add(crate::IMG_PREFIX);
        core::ptr::copy_nonoverlapping(ntp.header_ptr(), image, t_len as usize);
        image
    };
    // SAFETY: `image` now holds a valid tuple image (verbatim copy).
    let cached_view: HeapTupleData<'_> = unsafe {
        HeapTupleData::from_raw_parts(image, t_len, ntp.t_self, ntp.t_tableOid)
    };

    let cache = st.cache(cache_id);
    let tupdesc = cache.cc_tupdesc.expect("catcache: entry created before phase-2 init");
    let (nkeys, kinds, keyno) = (cache.cc_nkeys, cache.cc_kind, cache.cc_keyno);
    let mut keys = [Datum::null(); CATCACHE_MAXKEYS];
    for i in 0..nkeys as usize {
        keys[i] = match tuple_key(kinds[i], &cached_view, keyno[i], tupdesc)? {
            CatCKey::Value(d) => d,
            CatCKey::Bytes(b) => {
                // Offsets are payload-relative (stored_bytes contract).
                let off = b.as_ptr() as usize - buf.as_ptr() as usize;
                pack_ref(off as u32, b.len() as u32)
            }
            CatCKey::Str(_) => unreachable!(),
        };
    }

    let ct = CatCTup {
        hash_value,
        refcount: 0,
        dead: false,
        negative: false,
        hot: true,
        next: NONE,
        prev: NONE,
        c_list: NONE,
        keys,
        t_len,
        t_self: ntp.t_self,
        t_tableoid: ntp.t_tableOid,
        payload: buf.as_ptr(),
        payload_len: payload_len as u32,
        shared: None,
    };
    let cache = st.cache_mut(cache_id);
    let slot = cache.ct_alloc(ct);
    let bi = hash_index(hash_value, cache.cc_nbuckets);
    cache.ct_push_head(bi, slot);
    cache.cc_ntup += 1;
    st.ch_ntup += 1;
    maybe_rehash(st, cache_id);
    enforce_cap(st, cache_id, slot);
    Ok(slot)
}

/// The `HeapTupleHasExternal` arm of `CatalogCacheCreateEntry`; toast access
/// runs outside the state borrow (detoast re-enters the caches). `None` is
/// C's NULL return: the entry went stale mid-flatten and the caller rescans.
pub(crate) fn create_entry_from_scan(
    cache_id: i32,
    ntp: &HeapTupleData<'_>,
    hash_value: u32,
) -> PgResult<Option<u32>> {
    if !ntp.has_external() {
        return with_state(|st| create_entry_positive(st, cache_id, ntp, hash_value)).map(Some);
    }
    let tupdesc = with_state(|st| st.cache(cache_id).cc_tupdesc)
        .expect("catcache: entry created before phase-2 init");
    with_state(|st| push_in_progress(st, cache_id, hash_value, false));
    let scratch = mcx::MemoryContext::new("catcache toast_flatten_tuple");
    let flat = heaptoast::toast_flatten_tuple(scratch.mcx(), ntp, tupdesc);
    let dead = with_state(pop_in_progress);
    let flat = flat?;
    if dead {
        return Ok(None);
    }
    with_state(|st| create_entry_positive(st, cache_id, flat.as_tuple(), hash_value)).map(Some)
}

/// D3.2: the `HeapTupleHasExternal` arm for L2-shaped builds; same
/// in-progress/stale contract as `create_entry_from_scan`, but the result is
/// a shareable Arc, not an L1 slot.
pub(crate) fn l2_entry_from_scan(
    cache_id: i32,
    ntp: &HeapTupleData<'_>,
    hash_value: u32,
) -> PgResult<Option<std::sync::Arc<crate::l2::CatL2Entry>>> {
    if !ntp.has_external() {
        return crate::l2::build_positive(cache_id, ntp).map(Some);
    }
    let tupdesc = with_state(|st| st.cache(cache_id).cc_tupdesc)
        .expect("catcache: entry created before phase-2 init");
    with_state(|st| push_in_progress(st, cache_id, hash_value, false));
    let scratch = mcx::MemoryContext::new("catcache toast_flatten_tuple");
    let flat = heaptoast::toast_flatten_tuple(scratch.mcx(), ntp, tupdesc);
    let dead = with_state(pop_in_progress);
    let flat = flat?;
    if dead {
        return Ok(None);
    }
    crate::l2::build_positive(cache_id, flat.as_tuple()).map(Some)
}

/// D3.2: install an L1 entry whose payload aliases a shared L2 body. The slot
/// is linked at its bucket head with refcount 0, exactly like
/// `CatalogCacheCreateEntry`; only the payload ownership differs.
pub(crate) fn install_from_l2(
    st: &mut CatCacheState<'_>,
    cache_id: i32,
    hash_value: u32,
    ent: &std::sync::Arc<crate::l2::CatL2Entry>,
) -> u32 {
    let ct = CatCTup {
        hash_value,
        refcount: 0,
        dead: false,
        negative: ent.negative,
        hot: true,
        next: NONE,
        prev: NONE,
        c_list: NONE,
        keys: ent.keys,
        t_len: ent.t_len,
        t_self: ent.t_self,
        t_tableoid: ent.t_tableoid,
        payload: ent.payload.as_ptr(),
        payload_len: ent.payload.len() as u32,
        shared: Some(std::sync::Arc::clone(ent)),
    };
    let cache = st.cache_mut(cache_id);
    let slot = cache.ct_alloc(ct);
    let bi = hash_index(hash_value, cache.cc_nbuckets);
    cache.ct_push_head(bi, slot);
    cache.cc_ntup += 1;
    st.ch_ntup += 1;
    maybe_rehash(st, cache_id);
    enforce_cap(st, cache_id, slot);
    slot
}

/// `CatalogCacheCreateEntry` (negative): `CatCacheCopyKeys` into `payload`.
pub(crate) fn create_entry_negative(
    st: &mut CatCacheState<'_>,
    cache_id: i32,
    probes: &[CatCKey<'_>; 4],
    hash_value: u32,
) -> PgResult<u32> {
    let cache = st.cache(cache_id);
    let (nkeys, kinds) = (cache.cc_nkeys, cache.cc_kind);
    let mut byref_len = 0usize;
    for i in 0..nkeys as usize {
        if matches!(kinds[i], CCFastKind::Name | CCFastKind::Text | CCFastKind::OidVector) {
            byref_len += probes[i].bytes().len();
        }
    }
    let mcx = st.mcx;
    let buf = payload_alloc(mcx, byref_len);
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

    let ct = CatCTup {
        hash_value,
        refcount: 0,
        dead: false,
        negative: true,
        hot: true,
        next: NONE,
        prev: NONE,
        c_list: NONE,
        keys,
        t_len: 0,
        t_self: types_tuple::ItemPointerData::invalid(),
        t_tableoid: 0,
        payload: buf.as_ptr(),
        payload_len: byref_len as u32,
        shared: None,
    };
    let cache = st.cache_mut(cache_id);
    let slot = cache.ct_alloc(ct);
    let bi = hash_index(hash_value, cache.cc_nbuckets);
    cache.ct_push_head(bi, slot);
    cache.cc_ntup += 1;
    st.ch_ntup += 1;
    maybe_rehash(st, cache_id);
    enforce_cap(st, cache_id, slot);
    Ok(slot)
}

/// D3.1 cap enforcement: evict-on-insert-above-cap with a clock sweep over
/// the slot arenas of every cache (SLRU-style; the reference bit is
/// `CatCTup::hot`, set by every hit). Eviction of an unpinned entry is
/// exactly what `CatCacheInvalidate` does to a refcount==0 entry — remove it
/// and let the next probe rescan the catalog — so no new semantics exist
/// here; the cap only changes WHEN that removal happens.
///
/// Exemptions (never evicted): pinned entries (refcount > 0), members of a
/// live CatCList (c_list != NONE — the list's lifetime pins its members,
/// mirroring C's list-member protection in CatCacheRemoveCTup), dead entries
/// awaiting their last unpin, and the just-created `protect` slot (its
/// caller's refcount++ happens after creation returns).
pub(crate) fn enforce_cap(st: &mut CatCacheState<'_>, protect_cache: i32, protect_slot: u32) {
    enforce_cap_at(st, crate::catcache_cap(), protect_cache, protect_slot)
}

pub(crate) fn enforce_cap_at(
    st: &mut CatCacheState<'_>,
    cap: i32,
    protect_cache: i32,
    protect_slot: u32,
) {
    if cap <= 0 || st.ch_ntup <= cap {
        return;
    }
    // Two full revolutions max: pass 1 may only clear hot bits; if pass 2
    // finds nothing evictable everything live is pinned/hot — give up rather
    // than spin (the cache runs above cap until entries unpin).
    let total_slots: usize = st.caches.iter().flatten().map(|c| c.tuples.len()).sum();
    let mut budget = 2 * (total_slots + st.caches.len() + 1);
    while st.ch_ntup > cap && budget > 0 {
        budget -= 1;
        if st.clock_cache >= st.caches.len() {
            st.clock_cache = 0;
            st.clock_slot = 0;
        }
        let (advance_cache, evict) = match &mut st.caches[st.clock_cache] {
            Some(cache) if st.clock_slot < cache.tuples.len() => {
                let slot = st.clock_slot as u32;
                st.clock_slot += 1;
                let ct = &mut cache.tuples[slot as usize];
                let live = !ct.payload.is_null();
                let exempt = ct.refcount > 0
                    || ct.c_list != NONE
                    || ct.dead
                    || (cache.id == protect_cache && slot == protect_slot);
                if live && !exempt {
                    if ct.hot {
                        ct.hot = false;
                        (false, None)
                    } else {
                        (false, Some((cache.id, slot)))
                    }
                } else {
                    (false, None)
                }
            }
            _ => (true, None),
        };
        if advance_cache {
            st.clock_cache += 1;
            st.clock_slot = 0;
            continue;
        }
        if let Some((cache_id, slot)) = evict {
            remove_ct(st, cache_id, slot);
        }
    }
}

pub(crate) fn maybe_rehash(st: &mut CatCacheState<'_>, cache_id: i32) {
    let mcx = st.mcx;
    let cache = st.cache_mut(cache_id);
    if cache.cc_ntup > (cache.cc_nbuckets * 2) as i32 {
        rehash_cat_cache(mcx, cache);
    }
}

pub(crate) fn push_in_progress(st: &mut CatCacheState<'_>, cache_id: i32, hash_value: u32, list: bool) {
    st.in_progress.push(CatCInProgress { cache_id, hash_value, list, dead: false });
}

pub(crate) fn pop_in_progress(st: &mut CatCacheState<'_>) -> bool {
    st.in_progress.pop().expect("catcache: empty in-progress stack").dead
}

#[cfg(test)]
mod oidvector_key_tests {
    use super::oidvector_elements;

    /// Build a plain-storage (4B-U header) oidvector image with the given
    /// declared VARSIZE and on-image dim1. The header word is `varsize << 2`
    /// (little-endian 4B-U form that `VarlenaRef::varsize` decodes).
    fn image(varsize: u32, dim1: i32, buf_len: usize) -> Vec<u8> {
        let mut b = vec![0u8; buf_len];
        b[0..4].copy_from_slice(&(varsize << 2).to_le_bytes());
        // ndim(4) at 4, dataoffset(8), elemtype(12) left 0/unused by this walk.
        b[16..20].copy_from_slice(&dim1.to_le_bytes());
        b
    }

    #[test]
    fn valid_oidvector_reports_dim1_times_four() {
        // 3 Oids: header(24) + 3*4 = 36 bytes.
        let img = image(36, 3, 36);
        // SAFETY: `img` is a live 4B-U oidvector image for its full VARSIZE.
        let (_p, len) = unsafe { oidvector_elements(img.as_ptr()) }.unwrap();
        assert_eq!(len, 12);
    }

    #[test]
    fn empty_oidvector_is_zero_length() {
        let img = image(24, 0, 24);
        // SAFETY: as above.
        let (_p, len) = unsafe { oidvector_elements(img.as_ptr()) }.unwrap();
        assert_eq!(len, 0);
    }

    #[test]
    fn inflated_dim1_is_rejected_not_walked() {
        // dim1 claims 1000 Oids but the image is only header-sized: OOB if trusted.
        let img = image(24, 1000, 24);
        // SAFETY: as above; the crafted dim1 must be rejected before any walk.
        let err = unsafe { oidvector_elements(img.as_ptr()) }.err().unwrap();
        assert!(err.message().contains("not a valid oidvector"));
    }

    #[test]
    fn negative_dim1_does_not_wrap() {
        // A negative dim1 must not sign-extend into a huge `* 4` length; it is
        // treated as an empty vector (same as the adt `dim1.max(0)` slicing).
        let img = image(24, -1, 24);
        // SAFETY: as above.
        let (_p, len) = unsafe { oidvector_elements(img.as_ptr()) }.unwrap();
        assert_eq!(len, 0);
    }
}
