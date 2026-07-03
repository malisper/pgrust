use core::ptr::NonNull;

use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_tuple::{HeapTupleData, ItemPointerData};

use crate::compute::{compute_hash_value, hash_index, CatCKey, CCFastKind};
use crate::graph::{
    compute_tuple_hash_value, create_entry_from_scan, pop_in_progress, push_in_progress,
    remove_cl, remove_ct, rehash_cat_cache_lists,
};
use crate::search::build_scan_keys;
use crate::{pack_ref, payload_alloc, with_state, CATCACHE_MAXKEYS, NONE};

#[must_use]
pub struct CatCList {
    cache_id: i32,
    slot: u32,
    n_members: i32,
    pub ordered: bool,
}

pub struct MemberTuple<'a> {
    image: NonNull<u8>,
    t_len: u32,
    t_self: ItemPointerData,
    t_tableoid: Oid,
    _pin: core::marker::PhantomData<&'a CatCList>,
}

impl MemberTuple<'_> {
    #[inline]
    pub fn tuple(&self) -> HeapTupleData<'_> {
        // SAFETY: the pinned list holds each member's refcount-protected
        // image alive; images are write-once.
        unsafe {
            HeapTupleData::from_raw_parts(self.image.as_ptr(), self.t_len, self.t_self, self.t_tableoid)
        }
    }
}

impl CatCList {
    pub fn n_members(&self) -> i32 {
        self.n_members
    }

    pub fn member(&self, i: usize) -> MemberTuple<'_> {
        with_state(|st| {
            let cache = st.cache(self.cache_id);
            let m = cache.lists[self.slot as usize].members[i];
            let ct = &cache.tuples[m as usize];
            MemberTuple {
                // SAFETY: list members are positive entries (non-null image).
                image: unsafe { NonNull::new_unchecked(ct.image_ptr()) },
                t_len: ct.t_len,
                t_self: ct.t_self,
                t_tableoid: ct.t_tableoid,
                _pin: core::marker::PhantomData,
            }
        })
    }
}

/// `SearchCatCacheList(cache, nkeys, v1, v2, v3)`.
pub fn SearchCatCacheList(
    cache_id: i32,
    nkeys: i32,
    v1: CatCKey<'_>,
    v2: CatCKey<'_>,
    v3: CatCKey<'_>,
) -> PgResult<CatCList> {
    let keys = [v1, v2, v3, CatCKey::UNUSED];

    if !with_state(|st| st.cache(cache_id).initialized) {
        crate::init::catalog_cache_initialize_cache(cache_id)?;
    }

    let hit = with_state(|st| {
        {
            let mcx = st.mcx;
            let cache = st.cache_mut(cache_id);
            assert!(nkeys > 0 && nkeys < cache.cc_nkeys);
            if cache.cc_nlbuckets == 0 {
                cache.cc_nlbuckets = 16;
                cache.cc_lbucket.resize(16, NONE);
            } else if cache.cc_nlist > (cache.cc_nlbuckets * 2) as i32 {
                rehash_cat_cache_lists(mcx, cache);
            }
        }
        let cache = st.cache(cache_id);
        let kinds = cache.cc_kind;
        let l_hash = compute_hash_value(&kinds, nkeys, &keys);
        let bi = hash_index(l_hash, cache.cc_nlbuckets);
        let mut cur = cache.cc_lbucket[bi];
        while cur != NONE {
            let cl = &cache.lists[cur as usize];
            if !cl.dead
                && cl.hash_value == l_hash
                && cl.nkeys as i32 == nkeys
                && compare_list_keys(&kinds, nkeys, cl, &keys)
            {
                let n_members = cl.members.len() as i32;
                let ordered = cl.ordered;
                let cache = st.cache_mut(cache_id);
                cache.cl_move_head(bi, cur);
                cache.lists[cur as usize].refcount += 1;
                return Ok((l_hash, Some((cur, n_members, ordered))));
            }
            cur = cl.next;
        }
        Ok::<_, Box<types_error::PgError>>((l_hash, None))
    })?;

    let (l_hash, found) = hit;
    if let Some((slot, n_members, ordered)) = found {
        return Ok(CatCList { cache_id, slot, n_members, ordered });
    }

    build_list(cache_id, nkeys, l_hash, &keys)
}

#[inline]
fn compare_list_keys(
    kinds: &[CCFastKind; 4],
    nkeys: i32,
    cl: &crate::CatCList<'_>,
    probes: &[CatCKey<'_>; 4],
) -> bool {
    for i in 0..nkeys as usize {
        if !crate::eq_stored(kinds[i], cl.keys[i], cl.payload, &probes[i]) {
            return false;
        }
    }
    true
}

/// The list-miss build (the C PG_TRY body + finalize).
#[cold]
fn build_list(
    cache_id: i32,
    nkeys: i32,
    l_hash: u32,
    keys: &[CatCKey<'_>; 4],
) -> PgResult<CatCList> {
    let (reloid, indexoid) = with_state(|st| {
        let c = st.cache(cache_id);
        (c.cc_reloid, c.cc_indexoid)
    });

    let members_ctx = mcx::MemoryContext::new("SearchCatCacheList members");
    let mut members: mcx::PgVec<'_, u32> = mcx::PgVec::new_in(members_ctx.mcx());

    with_state(|st| push_in_progress(st, cache_id, l_hash, true));
    let result = build_list_scan(cache_id, nkeys, reloid, indexoid, keys, &mut members);
    let in_progress_dead = with_state(pop_in_progress);

    let ordered = match result {
        Ok(v) => v,
        Err(e) => return Err(e),
    };
    debug_assert!(!in_progress_dead, "list build retry loop exited dead");

    let (slot, n_members) = with_state(|st| -> PgResult<(u32, i32)> {
        let mcx = st.mcx;
        let cache = st.cache(cache_id);
        let kinds = cache.cc_kind;

        /* CatCacheCopyKeys */
        let mut byref_len = 0usize;
        for i in 0..nkeys as usize {
            if matches!(kinds[i], CCFastKind::Name | CCFastKind::Text | CCFastKind::OidVector) {
                byref_len += keys[i].bytes().len();
            }
        }
        let buf = payload_alloc(mcx, byref_len);
        let mut cl_keys = [Datum::null(); CATCACHE_MAXKEYS];
        let mut off = 0usize;
        for i in 0..nkeys as usize {
            cl_keys[i] = match kinds[i] {
                CCFastKind::Char | CCFastKind::Int2 | CCFastKind::Int4 => keys[i].word(),
                _ => {
                    let b = keys[i].bytes();
                    // SAFETY: `buf` was sized as the sum of the payloads.
                    unsafe {
                        core::ptr::copy_nonoverlapping(b.as_ptr(), buf.as_ptr().add(off), b.len());
                    }
                    let k = pack_ref(off as u32, b.len() as u32);
                    off += b.len();
                    k
                }
            };
        }

        let mut member_vec = mcx::PgVec::new_in(mcx);
        member_vec.extend_from_slice(&members);

        let n_members = members.len() as i32;
        let mut dead = false;
        let cl = crate::CatCList {
            hash_value: l_hash,
            refcount: 1,
            dead: false,
            ordered,
            nkeys: nkeys as i16,
            next: NONE,
            prev: NONE,
            keys: cl_keys,
            payload: buf.as_ptr(),
            payload_len: byref_len as u32,
            members: member_vec,
        };
        let cache = st.cache_mut(cache_id);
        let slot = cache.cl_alloc(cl);
        for i in 0..members.len() {
            let m = members[i];
            let ct = &mut cache.tuples[m as usize];
            debug_assert!(ct.c_list == NONE);
            ct.c_list = slot;
            ct.refcount -= 1; /* drop the temp ref taken during the scan */
            if ct.dead {
                dead = true;
            }
        }
        if dead {
            cache.lists[slot as usize].dead = true;
        }
        let bi = hash_index(l_hash, cache.cc_nlbuckets);
        cache.cl_push_head(bi, slot);
        cache.cc_nlist += 1;
        Ok((slot, n_members))
    })?;

    Ok(CatCList { cache_id, slot, n_members, ordered })
}

/// The `do { scan } while (in_progress_ent.dead)` retry loop.
fn build_list_scan(
    cache_id: i32,
    nkeys: i32,
    reloid: Oid,
    indexoid: Oid,
    keys: &[CatCKey<'_>; 4],
    members: &mut mcx::PgVec<'_, u32>,
) -> PgResult<bool> {
    loop {
        with_state(|st| {
            st.in_progress.last_mut().expect("in-progress underflow").dead = false;
        });
        members.clear();

        let scratch = mcx::MemoryContext::new("SearchCatCacheList");
        let scan_mcx = scratch.mcx();
        let cur_skey = build_scan_keys(scan_mcx, cache_id, nkeys, keys)?;
        let relation = table::table_open(scan_mcx, reloid, types_storage::lock::AccessShareLock)?;
        let index_ok = crate::init::IndexScanOK(cache_id);

        let mut inner_err: Option<Box<types_error::PgError>> = None;
        let ordered = genam_seams::systable_scan_catalog::call(
            &relation,
            indexoid,
            index_ok,
            &cur_skey[..nkeys as usize],
            &mut |ntp| match reuse_or_create_member(cache_id, ntp) {
                Ok(Some(slot)) => {
                    members.push(slot);
                    Ok(true)
                }
                Ok(None) => {
                    /* C: member create failed stale — mark the list build dead. */
                    with_state(|st| {
                        st.in_progress.last_mut().expect("in-progress underflow").dead = true;
                    });
                    Ok(false)
                }
                Err(e) => {
                    inner_err = Some(e);
                    Ok(false)
                }
            },
        );
        table::table_close(relation, types_storage::lock::AccessShareLock)?;
        drop(scratch);

        let failed = ordered.is_err() || inner_err.is_some();
        let retry =
            !failed && with_state(|st| st.in_progress.last().expect("in-progress underflow").dead);
        if failed || retry {
            // PG_CATCH / stale retry: undo the temp member refs.
            with_state(|st| {
                for &m in members.iter() {
                    let ct = &mut st.cache_mut(cache_id).tuples[m as usize];
                    ct.refcount -= 1;
                    let (dead, refcount, c_list) = (ct.dead, ct.refcount, ct.c_list);
                    if dead && refcount == 0 && c_list == NONE {
                        remove_ct(st, cache_id, m);
                    }
                }
            });
            if let Some(e) = inner_err {
                return Err(e);
            }
            ordered?;
            continue;
        }
        return ordered;
    }
}

/// Reuse a usable existing entry for `ntp` or create one; temp refcount++.
/// `None`: the new entry went stale mid-flatten (caller restarts the scan).
fn reuse_or_create_member(cache_id: i32, ntp: &HeapTupleData<'_>) -> PgResult<Option<u32>> {
    let found = with_state(|st| {
        let cache = st.cache(cache_id);
        let tupdesc = cache.cc_tupdesc.expect("list build before phase-2 init");
        let hv = compute_tuple_hash_value(&cache.cc_kind, cache.cc_nkeys, &cache.cc_keyno, tupdesc, ntp);
        let bi = hash_index(hv, cache.cc_nbuckets);
        let mut cur = cache.cc_bucket[bi];
        while cur != NONE {
            let ct = &cache.tuples[cur as usize];
            if !ct.dead
                && !ct.negative
                && ct.hash_value == hv
                && ct.t_self == ntp.t_self
                && ct.c_list == NONE
            {
                return (hv, Some(cur));
            }
            cur = ct.next;
        }
        (hv, None)
    });
    let slot = match found {
        (_, Some(slot)) => slot,
        (hv, None) => match create_entry_from_scan(cache_id, ntp, hv)? {
            Some(slot) => slot,
            None => return Ok(None),
        },
    };
    with_state(|st| st.cache_mut(cache_id).tuples[slot as usize].refcount += 1);
    Ok(Some(slot))
}

/// `ReleaseCatCacheList(list)`.
pub fn ReleaseCatCacheList(list: CatCList) {
    with_state(|st| {
        let cl = &mut st.cache_mut(list.cache_id).lists[list.slot as usize];
        debug_assert!(cl.refcount > 0);
        cl.refcount -= 1;
        if cl.dead && cl.refcount == 0 {
            remove_cl(st, list.cache_id, list.slot);
        }
    });
}
