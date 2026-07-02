use core::ptr::NonNull;

use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_scan::scankey::ScanKeyData;
use types_tuple::{HeapTupleData, ItemPointerData};

use crate::compute::{compute_hash_value, hash_index, int4_hash, CatCKey, CCFastKind};
use crate::graph::{create_entry_negative, create_entry_positive, remove_ct};
use crate::{compare_tuple, init, with_state, NONE};

/// A pinned positive cache entry — C's returned `HeapTuple` (`&ct->tuple`)
/// plus the `ct->refcount++` it carries. Release with [`ReleaseCatCache`];
/// the entry's image cannot be freed while pinned (invalidation only marks
/// it dead), so [`tuple`](CatCTuple::tuple) borrows are stable.
#[must_use]
pub struct CatCTuple {
    pub(crate) cache_id: i32,
    pub(crate) slot: u32,
    image: NonNull<u8>,
    t_len: u32,
    t_self: ItemPointerData,
    t_tableoid: Oid,
}

impl CatCTuple {
    #[inline]
    pub fn tuple(&self) -> HeapTupleData<'_> {
        // SAFETY: `image` is the entry's live tuple image; the pin (refcount)
        // keeps it allocated and nothing writes it after creation.
        unsafe {
            HeapTupleData::from_raw_parts(self.image.as_ptr(), self.t_len, self.t_self, self.t_tableoid)
        }
    }

    #[inline]
    pub fn cache_id(&self) -> i32 {
        self.cache_id
    }
}

enum Probe {
    Hit(CatCTuple),
    NegativeHit,
    Miss { hash_value: u32 },
    NeedsInit,
}

#[inline]
fn pin_entry(cache_id: i32, slot: u32, ct: &crate::CatCTup) -> CatCTuple {
    CatCTuple {
        cache_id,
        slot,
        // SAFETY: positive entries always carry a non-null image.
        image: unsafe { NonNull::new_unchecked(ct.payload) },
        t_len: ct.t_len,
        t_self: ct.t_self,
        t_tableoid: ct.t_tableoid,
    }
}

/// The bucket probe (`SearchCatCacheInternal` up to the miss tail): ONE
/// non-reentrant state borrow does hash → bucket walk → compare →
/// move-to-front → refcount bump. No seam, no allocation.
#[inline]
fn probe(cache_id: i32, mut nkeys: i32, keys: &[CatCKey<'_>; 4]) -> Probe {
    with_state(|st| {
        let cache = st.cache(cache_id);
        if !cache.initialized {
            return Probe::NeedsInit;
        }
        if nkeys == 0 {
            nkeys = cache.cc_nkeys;
        }
        debug_assert_eq!(cache.cc_nkeys, nkeys);
        let kinds = cache.cc_kind;

        // Monomorphized single-Oid-key lane (RELOID/TYPEOID/PROCOID/... —
        // the dominant catalog probe): no per-key dispatch at all.
        if nkeys == 1 {
            if let (CCFastKind::Int4, CatCKey::Value(w)) = (kinds[0], &keys[0]) {
                return probe_1_int4(st, cache_id, *w);
            }
        }

        let hash_value = compute_hash_value(&kinds, nkeys, keys);
        let bi = hash_index(hash_value, cache.cc_nbuckets);
        let mut cur = cache.cc_bucket[bi];
        while cur != NONE {
            let ct = &cache.tuples[cur as usize];
            if !ct.dead
                && ct.hash_value == hash_value
                && compare_tuple(&kinds, nkeys, ct, keys)
            {
                return found(st, cache_id, bi, cur);
            }
            cur = ct.next;
        }
        Probe::Miss { hash_value }
    })
}

#[inline]
fn probe_1_int4(st: &mut crate::CatCacheState<'_>, cache_id: i32, w: Datum) -> Probe {
    let cache = st.cache(cache_id);
    let hash_value = int4_hash(w);
    let bi = hash_index(hash_value, cache.cc_nbuckets);
    let key = w.as_i32();
    let mut cur = cache.cc_bucket[bi];
    while cur != NONE {
        let ct = &cache.tuples[cur as usize];
        if !ct.dead && ct.hash_value == hash_value && ct.keys[0].as_i32() == key {
            return found(st, cache_id, bi, cur);
        }
        cur = ct.next;
    }
    Probe::Miss { hash_value }
}

#[inline]
fn found(st: &mut crate::CatCacheState<'_>, cache_id: i32, bucket: usize, slot: u32) -> Probe {
    let cache = st.cache_mut(cache_id);
    cache.ct_move_head(bucket, slot);
    let ct = &mut cache.tuples[slot as usize];
    if ct.negative {
        Probe::NegativeHit
    } else {
        // C: ResourceOwnerEnlarge + ct->refcount++ + RememberCatCacheRef.
        // The pin is the guard; resowner integration follows the xact unit.
        ct.refcount += 1;
        Probe::Hit(pin_entry(cache_id, slot, ct))
    }
}

fn search_internal(cache_id: i32, nkeys: i32, keys: &[CatCKey<'_>; 4]) -> PgResult<Option<CatCTuple>> {
    loop {
        match probe(cache_id, nkeys, keys) {
            Probe::Hit(t) => return Ok(Some(t)),
            Probe::NegativeHit => return Ok(None),
            Probe::Miss { hash_value } => return search_miss(cache_id, hash_value, keys),
            Probe::NeedsInit => {
                // Init opens the catalog relation (re-enters the catcache via
                // the relcache path); runs with no state borrow, then retry.
                init::catalog_cache_initialize_cache(cache_id)?;
            }
        }
    }
}

/// `SearchCatCacheMiss` — scan the catalog through the genam seam, insert a
/// positive entry (or a negative one), return the pinned copy.
#[cold]
fn search_miss(cache_id: i32, hash_value: u32, keys: &[CatCKey<'_>; 4]) -> PgResult<Option<CatCTuple>> {
    let (reloid, indexoid, nkeys) = with_state(|st| {
        let c = st.cache(cache_id);
        (c.cc_reloid, c.cc_indexoid, c.cc_nkeys)
    });

    let scratch = mcx::MemoryContext::new("SearchCatCacheMiss");
    let scan_mcx = scratch.mcx();
    let cur_skey = build_scan_keys(scan_mcx, cache_id, nkeys, keys)?;

    let relation = table::table_open(scan_mcx, reloid, types_storage::lock::AccessShareLock)?;
    let index_ok = init::IndexScanOK(cache_id);

    let mut slot: Option<u32> = None;
    let mut create_err: Option<Box<types_error::PgError>> = None;
    genam_seams::systable_scan_catalog::call(
        &relation,
        indexoid,
        index_ok,
        &cur_skey[..nkeys as usize],
        &mut |ntp| {
            match with_state(|st| create_entry_positive(st, cache_id, ntp, hash_value)) {
                Ok(s) => {
                    slot = Some(s);
                    Ok(false) /* break: assume only one match */
                }
                Err(e) => {
                    create_err = Some(e);
                    Ok(false)
                }
            }
        },
    )?;
    table::table_close(relation, types_storage::lock::AccessShareLock)?;
    drop(scratch);
    if let Some(e) = create_err {
        return Err(e);
    }

    if let Some(slot) = slot {
        return Ok(Some(with_state(|st| {
            let cache = st.cache_mut(cache_id);
            let ct = &mut cache.tuples[slot as usize];
            ct.refcount += 1;
            pin_entry(cache_id, slot, ct)
        })));
    }

    // Negative entry, unless bootstrap (inval can't clear it there).
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(None);
    }
    with_state(|st| create_entry_negative(st, cache_id, keys, hash_value))?;
    Ok(None)
}

/// `memcpy(cur_skey, cache->cc_skey, ...)` + `sk_argument = v1..vN`. By-ref
/// arguments are framed into the on-disk image the index comparator reads
/// (NameData buffer / 4-byte-header varlena / oidvector), in `scan_mcx`.
fn build_scan_keys<'mcx>(
    scan_mcx: mcx::Mcx<'mcx>,
    cache_id: i32,
    nkeys: i32,
    keys: &[CatCKey<'_>; 4],
) -> PgResult<[ScanKeyData; 4]> {
    use types_scan::scankey::BTEqualStrategyNumber;
    let (keyno, kinds, eqfunc) = with_state(|st| {
        let c = st.cache(cache_id);
        (c.cc_keyno, c.cc_kind, c.cc_eqfunc)
    });
    let mut out: [ScanKeyData; 4] = core::array::from_fn(|_| ScanKeyData::empty());
    for i in 0..nkeys as usize {
        let sk = &mut out[i];
        sk.sk_attno = keyno[i] as types_core::AttrNumber;
        sk.sk_strategy = BTEqualStrategyNumber;
        sk.sk_subtype = 0;
        sk.sk_collation = types_core::catalog::C_COLLATION_OID;
        sk.sk_func = types_fmgr::FmgrInfo {
            fn_oid: eqfunc[i],
            ..types_fmgr::FmgrInfo::unresolved()
        };
        sk.sk_argument = frame_scan_arg(scan_mcx, kinds[i], &keys[i])?;
    }
    Ok(out)
}

fn frame_scan_arg(mcx: mcx::Mcx<'_>, kind: CCFastKind, key: &CatCKey<'_>) -> PgResult<Datum> {
    use types_tuple::varatt::VARHDRSZ;
    Ok(match kind {
        CCFastKind::Char | CCFastKind::Int2 | CCFastKind::Int4 => key.word(),
        CCFastKind::Name => {
            let b = key.bytes();
            let n = b.len().min(crate::compute::NAMEDATALEN - 1);
            let buf = crate::payload_alloc(mcx, crate::compute::NAMEDATALEN);
            // SAFETY: fresh NAMEDATALEN-byte buffer; n < NAMEDATALEN.
            unsafe {
                core::ptr::write_bytes(buf.as_ptr(), 0, crate::compute::NAMEDATALEN);
                core::ptr::copy_nonoverlapping(b.as_ptr(), buf.as_ptr(), n);
            }
            Datum::from_usize(buf.as_ptr() as usize)
        }
        CCFastKind::Text => {
            let b = key.bytes();
            let total = b.len() + VARHDRSZ;
            let buf = crate::payload_alloc(mcx, total);
            // SAFETY: fresh `total`-byte buffer.
            unsafe {
                let word = types_tuple::varatt::set_varsize_4b_word(total as u32);
                core::ptr::copy_nonoverlapping(word.to_ne_bytes().as_ptr(), buf.as_ptr(), 4);
                core::ptr::copy_nonoverlapping(b.as_ptr(), buf.as_ptr().add(4), b.len());
            }
            Datum::from_usize(buf.as_ptr() as usize)
        }
        CCFastKind::OidVector => {
            // Rebuild the oidvector image (buildoidvector): 24-byte ArrayType
            // header {vl_len, ndim=1, dataoffset=0, elemtype=OIDOID, dim1,
            // lbound1=0} + element words.
            let b = key.bytes();
            let dim1 = (b.len() / 4) as i32;
            let total = 24 + b.len();
            let buf = crate::payload_alloc(mcx, total);
            // SAFETY: fresh `total`-byte, 8-aligned buffer.
            unsafe {
                let p = buf.as_ptr();
                let word = types_tuple::varatt::set_varsize_4b_word(total as u32);
                core::ptr::copy_nonoverlapping(word.to_ne_bytes().as_ptr(), p, 4);
                core::ptr::write_unaligned(p.add(4).cast::<i32>(), 1);
                core::ptr::write_unaligned(p.add(8).cast::<i32>(), 0);
                core::ptr::write_unaligned(p.add(12).cast::<u32>(), 26 /* OIDOID */);
                core::ptr::write_unaligned(p.add(16).cast::<i32>(), dim1);
                core::ptr::write_unaligned(p.add(20).cast::<i32>(), 0);
                core::ptr::copy_nonoverlapping(b.as_ptr(), p.add(24), b.len());
            }
            Datum::from_usize(buf.as_ptr() as usize)
        }
    })
}

pub fn SearchCatCache(
    cache_id: i32,
    v1: CatCKey<'_>,
    v2: CatCKey<'_>,
    v3: CatCKey<'_>,
    v4: CatCKey<'_>,
) -> PgResult<Option<CatCTuple>> {
    search_internal(cache_id, 0, &[v1, v2, v3, v4])
}

pub fn SearchCatCache1(cache_id: i32, v1: CatCKey<'_>) -> PgResult<Option<CatCTuple>> {
    search_internal(cache_id, 1, &[v1, CatCKey::UNUSED, CatCKey::UNUSED, CatCKey::UNUSED])
}

pub fn SearchCatCache2(cache_id: i32, v1: CatCKey<'_>, v2: CatCKey<'_>) -> PgResult<Option<CatCTuple>> {
    search_internal(cache_id, 2, &[v1, v2, CatCKey::UNUSED, CatCKey::UNUSED])
}

pub fn SearchCatCache3(
    cache_id: i32,
    v1: CatCKey<'_>,
    v2: CatCKey<'_>,
    v3: CatCKey<'_>,
) -> PgResult<Option<CatCTuple>> {
    search_internal(cache_id, 3, &[v1, v2, v3, CatCKey::UNUSED])
}

pub fn SearchCatCache4(
    cache_id: i32,
    v1: CatCKey<'_>,
    v2: CatCKey<'_>,
    v3: CatCKey<'_>,
    v4: CatCKey<'_>,
) -> PgResult<Option<CatCTuple>> {
    search_internal(cache_id, 4, &[v1, v2, v3, v4])
}

/// `ReleaseCatCache(tuple)`.
pub fn ReleaseCatCache(tuple: CatCTuple) {
    with_state(|st| {
        let cache = st.cache_mut(tuple.cache_id);
        let ct = &mut cache.tuples[tuple.slot as usize];
        debug_assert!(ct.refcount > 0);
        ct.refcount -= 1;
        if ct.dead && ct.refcount == 0 && ct.c_list == NONE {
            remove_ct(st, tuple.cache_id, tuple.slot);
        }
    });
}

/// `GetCatCacheHashValue(cache, v1..v4)`.
pub fn GetCatCacheHashValue(
    cache_id: i32,
    v1: CatCKey<'_>,
    v2: CatCKey<'_>,
    v3: CatCKey<'_>,
    v4: CatCKey<'_>,
) -> PgResult<u32> {
    if !with_state(|st| st.cache(cache_id).initialized) {
        init::catalog_cache_initialize_cache(cache_id)?;
    }
    Ok(with_state(|st| {
        let c = st.cache(cache_id);
        compute_hash_value(&c.cc_kind, c.cc_nkeys, &[v1, v2, v3, v4])
    }))
}
