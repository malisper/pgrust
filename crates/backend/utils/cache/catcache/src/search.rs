use core::ptr::NonNull;

use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_scan::scankey::ScanKeyData;
use types_tuple::{HeapTupleData, ItemPointerData};

use crate::compute::{fast_hash_probe, hash_index, int4_hash, CatCKey, CCFastKind};
use crate::graph::{create_entry_from_scan, create_entry_negative, remove_ct};
use crate::{eq_stored, init, with_state, NONE};

/// A pinned positive entry (C's `&ct->tuple` + `ct->refcount++`); release
/// with [`ReleaseCatCache`]. The `IMG_PREFIX`-prefixed image outlives the pin.
#[must_use]
pub struct CatCTuple {
    pub(crate) cache_id: i32,
    pub(crate) slot: u32,
    image: NonNull<u8>,
}

impl CatCTuple {
    #[inline]
    pub fn tuple(&self) -> HeapTupleData<'_> {
        // SAFETY: live IMG_PREFIX-prefixed image, pinned, written once.
        unsafe {
            let base = self.image.as_ptr().sub(crate::IMG_PREFIX);
            let t_self = core::ptr::read(base.cast::<ItemPointerData>());
            let t_tableoid = core::ptr::read(base.add(8).cast::<Oid>());
            let t_len = core::ptr::read(base.add(12).cast::<u32>());
            HeapTupleData::from_raw_parts(self.image.as_ptr(), t_len, t_self, t_tableoid)
        }
    }

    #[inline]
    pub fn cache_id(&self) -> i32 {
        self.cache_id
    }
}

/// Two-word probe result: returns in registers, not an sret buffer (whose
/// wide reload stalled on V2 store-forwarding — catcache-parity.md).
#[derive(Clone, Copy)]
struct ProbeRet {
    p: *mut u8,
    w: u64,
}

const PROBE_NEG: usize = 1;
const PROBE_MISS: usize = 2;
const PROBE_INIT: usize = 3;

impl ProbeRet {
    #[inline(always)]
    fn hit(image: *mut u8, slot: u32) -> Self {
        ProbeRet { p: image, w: slot as u64 }
    }
    #[inline(always)]
    fn negative() -> Self {
        ProbeRet { p: core::ptr::without_provenance_mut(PROBE_NEG), w: 0 }
    }
    #[inline(always)]
    fn miss(hash_value: u32) -> Self {
        ProbeRet { p: core::ptr::without_provenance_mut(PROBE_MISS), w: hash_value as u64 }
    }
    #[inline(always)]
    fn needs_init() -> Self {
        ProbeRet { p: core::ptr::without_provenance_mut(PROBE_INIT), w: 0 }
    }
}

#[inline]
fn pin_entry(cache_id: i32, slot: u32, ct: &crate::CatCTup) -> CatCTuple {
    CatCTuple {
        cache_id,
        slot,
        // SAFETY: positive entries always carry a non-null prefixed image.
        image: unsafe { NonNull::new_unchecked(ct.image_ptr()) },
    }
}

/// Search-key carrier, monomorphized per arity: constant `nkeys`, no
/// key-array materialization. `NKEYS == -1` reads the cache's `cc_nkeys`.
pub(crate) trait ProbeKeys {
    const NKEYS: i32;
    fn slot(&self, i: usize) -> CatCKey<'_>;
    fn to_array(&self) -> [CatCKey<'_>; 4];
}

pub(crate) struct K1<'a>(pub CatCKey<'a>);
pub(crate) struct K2<'a>(pub CatCKey<'a>, pub CatCKey<'a>);
pub(crate) struct K3<'a>(pub CatCKey<'a>, pub CatCKey<'a>, pub CatCKey<'a>);
pub(crate) struct K4<'a>(pub [CatCKey<'a>; 4]);
pub(crate) struct KDyn<'a>(pub [CatCKey<'a>; 4]);

impl ProbeKeys for K1<'_> {
    const NKEYS: i32 = 1;
    #[inline(always)]
    fn slot(&self, i: usize) -> CatCKey<'_> {
        debug_assert_eq!(i, 0);
        self.0
    }
    fn to_array(&self) -> [CatCKey<'_>; 4] {
        [self.0, CatCKey::UNUSED, CatCKey::UNUSED, CatCKey::UNUSED]
    }
}

impl ProbeKeys for K2<'_> {
    const NKEYS: i32 = 2;
    #[inline(always)]
    fn slot(&self, i: usize) -> CatCKey<'_> {
        if i == 0 {
            self.0
        } else {
            self.1
        }
    }
    fn to_array(&self) -> [CatCKey<'_>; 4] {
        [self.0, self.1, CatCKey::UNUSED, CatCKey::UNUSED]
    }
}

impl ProbeKeys for K3<'_> {
    const NKEYS: i32 = 3;
    #[inline(always)]
    fn slot(&self, i: usize) -> CatCKey<'_> {
        match i {
            0 => self.0,
            1 => self.1,
            _ => self.2,
        }
    }
    fn to_array(&self) -> [CatCKey<'_>; 4] {
        [self.0, self.1, self.2, CatCKey::UNUSED]
    }
}

impl ProbeKeys for K4<'_> {
    const NKEYS: i32 = 4;
    #[inline(always)]
    fn slot(&self, i: usize) -> CatCKey<'_> {
        self.0[i]
    }
    fn to_array(&self) -> [CatCKey<'_>; 4] {
        self.0
    }
}

impl ProbeKeys for KDyn<'_> {
    const NKEYS: i32 = -1;
    #[inline(always)]
    fn slot(&self, i: usize) -> CatCKey<'_> {
        self.0[i]
    }
    fn to_array(&self) -> [CatCKey<'_>; 4] {
        self.0
    }
}

/// `CatalogCacheComputeHashValue`.
#[inline(always)]
fn hash_keys<K: ProbeKeys>(kinds: &[CCFastKind; 4], nkeys: i32, keys: &K) -> u32 {
    let mut hash: u32 = 0;
    if nkeys == 4 {
        hash ^= fast_hash_probe(kinds[3], &keys.slot(3)).rotate_left(24);
    }
    if nkeys >= 3 {
        hash ^= fast_hash_probe(kinds[2], &keys.slot(2)).rotate_left(16);
    }
    if nkeys >= 2 {
        hash ^= fast_hash_probe(kinds[1], &keys.slot(1)).rotate_left(8);
    }
    hash ^ fast_hash_probe(kinds[0], &keys.slot(0))
}

/// `CatalogCacheCompareTuple`.
#[inline(always)]
fn keys_match<K: ProbeKeys>(
    kinds: &[CCFastKind; 4],
    nkeys: i32,
    ct: &crate::CatCTup,
    keys: &K,
) -> bool {
    for i in 0..nkeys as usize {
        if !eq_stored(kinds[i], ct.keys[i], ct.payload, &keys.slot(i)) {
            return false;
        }
    }
    true
}

/// `SearchCatCacheInternal` up to the miss tail, under ONE borrow.
#[inline(always)]
fn probe<K: ProbeKeys>(cache_id: i32, keys: &K) -> ProbeRet {
    with_state(|st| {
        let cache = st.cache_mut(cache_id);
        if !cache.initialized {
            return ProbeRet::needs_init();
        }
        let nkeys = if K::NKEYS > 0 { K::NKEYS } else { cache.cc_nkeys };
        debug_assert!(K::NKEYS < 0 || cache.cc_nkeys == nkeys);
        let kinds = cache.cc_kind;


        if K::NKEYS == 1 {
            if let (CCFastKind::Int4, CatCKey::Value(w)) = (kinds[0], keys.slot(0)) {
                return probe_1_int4(cache, w);
            }
            return probe_walk_outlined(cache, nkeys, &kinds, keys);
        }

        probe_walk(cache, nkeys, &kinds, keys)
    })
}

#[inline(always)]
fn probe_walk<K: ProbeKeys>(
    cache: &mut crate::CatCache<'_>,
    nkeys: i32,
    kinds: &[CCFastKind; 4],
    keys: &K,
) -> ProbeRet {
    let hash_value = hash_keys(kinds, nkeys, keys);
    let bi = hash_index(hash_value, cache.cc_nbuckets);
    // SAFETY: bi is masked below cc_bucket.len() (power-of-two invariant).
    let mut cur = unsafe { *cache.cc_bucket.get_unchecked(bi) };
    while cur != NONE {
        // SAFETY: bucket links reference live slots (arena invariant).
        let ct = unsafe { cache.tuples.get_unchecked(cur as usize) };
        if !ct.dead && ct.hash_value == hash_value && keys_match(kinds, nkeys, ct, keys) {
            return found(cache, bi, cur);
        }
        cur = ct.next;
    }
    ProbeRet::miss(hash_value)
}

#[inline(never)]
fn probe_walk_outlined<K: ProbeKeys>(
    cache: &mut crate::CatCache<'_>,
    nkeys: i32,
    kinds: &[CCFastKind; 4],
    keys: &K,
) -> ProbeRet {
    probe_walk(cache, nkeys, kinds, keys)
}

#[inline(always)]
fn probe_1_int4(cache: &mut crate::CatCache<'_>, w: Datum) -> ProbeRet {
    let hash_value = int4_hash(w);
    let bi = hash_index(hash_value, cache.cc_nbuckets);
    let key = w.as_i32();
    // SAFETY: bi is masked below cc_bucket.len() (power-of-two invariant).
    let mut cur = unsafe { *cache.cc_bucket.get_unchecked(bi) };
    while cur != NONE {
        // SAFETY: bucket links reference live slots (arena invariant).
        let ct = unsafe { cache.tuples.get_unchecked(cur as usize) };
        if !ct.dead && ct.hash_value == hash_value && ct.keys[0].as_i32() == key {
            return found(cache, bi, cur);
        }
        cur = ct.next;
    }
    ProbeRet::miss(hash_value)
}

#[inline(always)]
fn found(cache: &mut crate::CatCache<'_>, bucket: usize, slot: u32) -> ProbeRet {
    // SAFETY: `bucket` is the masked bucket `slot` was walked from.
    unsafe { cache.ct_move_head_hot(bucket, slot) };
    // SAFETY: `slot` came off the bucket walk (live slot).
    let ct = unsafe { cache.tuples.get_unchecked_mut(slot as usize) };
    if ct.negative {
        ProbeRet::negative()
    } else {
        // C also RememberCatCacheRef's; resowner integration follows xact.
        ct.refcount += 1;
        ProbeRet::hit(ct.image_ptr(), slot)
    }
}

#[inline(always)]
fn search_internal<K: ProbeKeys>(cache_id: i32, keys: &K) -> PgResult<Option<CatCTuple>> {
    loop {
        let r = probe(cache_id, keys);
        match r.p.addr() {
            PROBE_NEG => return Ok(None),
            PROBE_MISS => return search_miss(cache_id, r.w as u32, &keys.to_array()),
            PROBE_INIT => {
                /* init re-enters the catcache; no borrow held, then retry */
                init::catalog_cache_initialize_cache(cache_id)?;
            }
            _ => {
                return Ok(Some(CatCTuple {
                    cache_id,
                    slot: r.w as u32,
                    // SAFETY: non-sentinel p is the pin's live image.
                    image: unsafe { NonNull::new_unchecked(r.p) },
                }));
            }
        }
    }
}

/// `SearchCatCacheMiss`.
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
    /* C's do-while(stale): a mid-flatten invalidation restarts the scan. */
    loop {
        let mut stale = false;
        genam_seams::systable_scan_catalog::call(
            &relation,
            indexoid,
            index_ok,
            &cur_skey[..nkeys as usize],
            &mut |ntp| {
                match create_entry_from_scan(cache_id, ntp, hash_value) {
                    Ok(Some(s)) => slot = Some(s),
                    Ok(None) => stale = true,
                    Err(e) => create_err = Some(e),
                }
                Ok(false) /* break: assume only one match */
            },
        )?;
        if !stale || create_err.is_some() {
            break;
        }
    }
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

/// `memcpy(cur_skey, cc_skey, ...)` + `sk_argument = v1..vN`.
pub(crate) fn build_scan_keys<'mcx>(
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
        // C resolves cc_skey[i].sk_func at cache init; heap-fallback scans
        // (pre-critical-relcache misses) invoke it via heap_key_test.
        sk.sk_func = fmgr_seams::fmgr_info::call(eqfunc[i])?;
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
            // buildoidvector: 24-byte ArrayType header + element words.
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
    search_internal(cache_id, &KDyn([v1, v2, v3, v4]))
}

#[inline]
pub fn SearchCatCache1(cache_id: i32, v1: CatCKey<'_>) -> PgResult<Option<CatCTuple>> {
    search_internal(cache_id, &K1(v1))
}

#[inline]
pub fn SearchCatCache2(cache_id: i32, v1: CatCKey<'_>, v2: CatCKey<'_>) -> PgResult<Option<CatCTuple>> {
    search_internal(cache_id, &K2(v1, v2))
}

pub fn SearchCatCache3(
    cache_id: i32,
    v1: CatCKey<'_>,
    v2: CatCKey<'_>,
    v3: CatCKey<'_>,
) -> PgResult<Option<CatCTuple>> {
    search_internal(cache_id, &K3(v1, v2, v3))
}

pub fn SearchCatCache4(
    cache_id: i32,
    v1: CatCKey<'_>,
    v2: CatCKey<'_>,
    v3: CatCKey<'_>,
    v4: CatCKey<'_>,
) -> PgResult<Option<CatCTuple>> {
    search_internal(cache_id, &K4([v1, v2, v3, v4]))
}

/// `ReleaseCatCache(tuple)`.
#[inline]
pub fn ReleaseCatCache(tuple: CatCTuple) {
    with_state(|st| {
        let cache = st.cache_mut(tuple.cache_id);
        // SAFETY: pins are minted over live slots, never freed while pinned.
        let ct = unsafe { cache.tuples.get_unchecked_mut(tuple.slot as usize) };
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
        hash_keys(&c.cc_kind, c.cc_nkeys, &KDyn([v1, v2, v3, v4]))
    }))
}
