//! Invalidation support for inval.c (`catcache.c`):
//! `PrepareToInvalidateCacheTuple`.
//!
//! For each cache on `RelationGetRelid(relation)`, compute the tuple's hash
//! value(s) and emit one [`types_storage::PrepareToInvalidateCacheTuple`]
//! request per `(*function)` invocation the C code makes (one for the old
//! tuple's keys, plus one for the new tuple's keys on an update when the hash
//! differs). The key columns are deformed from the real `HeapTupleData` via the
//! cache's `cc_tupdesc` (read through the catcache's own descriptor) and
//! `heap_getattr` (genam/heaptuple substrate); `dbId` is
//! `cc_relisshared ? InvalidOid : MyDatabaseId`.

use alloc::vec::Vec;

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_core::primitive::InvalidOid;
use types_core::Oid;
use types_cache::backend_utils_cache_catcache::{CCFastKind, CacheIdx, CatKey};
// Bare-word machine-word `Datum` (`types_datum::Datum`), aliased `ScalarWord`:
// `CatalogCacheComputeHashValue` consumes the by-value scalar key word (and the
// `PointerGetDatum` of a detoasted by-reference payload). Pass-by-value scalar
// keys stay the audited bare word, not the canonical `types_tuple::Datum<'mcx>`
// enum (which carries deformed tuple values).
use types_datum::Datum as ScalarWord;
use types_error::PgResult;
use types_rel::RelationData;
use types_storage::PrepareToInvalidateCacheTuple;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{HeapTupleData, HeapTupleHeaderGetNatts, TupleDescData};

use backend_access_common_heaptuple::{getmissingattr, heap_attisnull, nocachegetattr};
use backend_utils_init_small_seams as init_small_seams;

use crate::core_compute::CatalogCacheComputeHashValue;
use crate::{init_meta, with_arena};

/// The metadata `PrepareToInvalidateCacheTuple` needs about one matching cache,
/// snapshotted under the arena borrow so the hash computation (which re-enters
/// the arena through the `cc_tupdesc` seam) runs after the borrow is dropped.
/// Mirrors the C reads of `ccp->id`, `ccp->cc_nkeys`, `ccp->cc_keyno`,
/// `ccp->cc_fastkind`, and `ccp->cc_relisshared`.
struct CacheProbe {
    /// `ccp->id`.
    id: i32,
    /// `ccp->cc_nkeys`.
    cc_nkeys: i32,
    /// `ccp->cc_keyno[CATCACHE_MAXKEYS]`.
    cc_keyno: [i32; 4],
    /// Per-key fast hash/equality selection (`GetCCHashEqFuncs` result).
    cc_fastkind: [CCFastKind; 4],
    /// `ccp->cc_relisshared`.
    cc_relisshared: bool,
}

/// `heap_getattr(tup, attnum, tupleDesc, &isnull)` (`access/htup_details.h`),
/// composed from the heaptuple unit's pieces: attributes past the tuple's natts
/// via `getmissingattr`, NULLs via the bitmap check, everything else via
/// `nocachegetattr`. The catcache key columns are always `attnum > 0`, so the
/// `heap_getsysattr` arm of the general macro is not reachable here.
///
/// `data` is the tuple's user-data area (`td + t_hoff .. td + t_len`).
fn heap_getattr<'mcx>(
    mcx: Mcx<'mcx>,
    tuple: &HeapTupleData<'_>,
    attnum: i32,
    tupdesc: &TupleDescData<'_>,
    data: &[u8],
) -> PgResult<(Datum<'mcx>, bool)> {
    debug_assert!(attnum > 0, "heap_getattr: catcache keys are user columns");
    let header = tuple
        .t_data
        .as_ref()
        .expect("heap_getattr: tuple has no t_data");
    if attnum > HeapTupleHeaderGetNatts(header) as i32 {
        return getmissingattr(mcx, tupdesc, attnum);
    }
    if heap_attisnull(tuple, attnum, Some(tupdesc)) {
        return Ok((Datum::ByVal(0), true));
    }
    Ok((nocachegetattr(mcx, tuple, attnum, tupdesc, data)?, false))
}

/// `CatalogCacheComputeTupleHashValue(cache, nkeys, tuple)` (catcache.c) —
/// extract the cache's key columns from `tuple` (the C `fastgetattr` cascade
/// with the `case 4..1` fall-through) and feed them to
/// [`CatalogCacheComputeHashValue`]. An out-of-range `nkeys` is the C
/// `elog(FATAL, "wrong number of hash keys")`, which `CatalogCacheComputeHashValue`
/// raises.
fn catalog_cache_compute_tuple_hash_value(
    mcx: Mcx<'_>,
    cache_id: i32,
    probe: &CacheProbe,
    tuple: &HeapTupleData<'_>,
    tuple_data: &[u8],
) -> PgResult<u32> {
    let nkeys = probe.cc_nkeys;
    let cc_keyno = &probe.cc_keyno;

    // The C reads of `v1..v4` start zeroed and the `case 4..1` fall-through
    // fills only the used slots.
    let mut v: [CatKey; 4] = core::array::from_fn(|_| CatKey::scalar_null());

    // Borrow the cache's tuple descriptor (lives in CacheMemoryContext) and
    // deform the key columns against it, exactly as the C reads
    // `cache->cc_tupdesc`. `init_meta::with_cache_tupdesc` re-enters the arena,
    // so this runs outside the iteration's arena borrow.
    let mut deform: PgResult<()> = Ok(());
    init_meta::with_cache_tupdesc(cache_id, &mut |tupdesc| {
        // `tp = (char *) tup->t_data + tup->t_data->t_hoff;` — the tuple's
        // user-data area, threaded in from the caller's `FormedTuple.data`.
        let data = tuple_data;

        // Now extract key fields from tuple, insert into scankey
        // (the C `switch (nkeys) { case 4: ... case 1: ... }` cascade).
        for i in (0..nkeys as usize).rev() {
            let (value, is_null) = match heap_getattr(mcx, tuple, cc_keyno[i], tupdesc, data) {
                Ok(r) => r,
                Err(e) => {
                    deform = Err(e);
                    return;
                }
            };
            debug_assert!(!is_null, "catcache key column unexpectedly NULL");
            // The key value fed to `CatalogCacheComputeHashValue` is a by-value
            // scalar word for by-value kinds and the resolved payload bytes for a
            // by-reference key (`name`/`text`/`oidvector`) — the same `CatKey`
            // shape the search/build paths produce (`CatCacheCopyKeys`).
            v[i] = match &value {
                Datum::ByVal(d) => CatKey::Scalar(ScalarWord::from_usize(*d)),
                // The deformed key column is the full on-disk varlena image; the
                // hash core consumes the canonical header-LESS payload (the same
                // bytes the search/build paths use and a by-name search key
                // carries). Without this, a `text`-keyed inval request's hash
                // never matches the negative entry left by an earlier by-name
                // miss, so the stale negative entry is never cleared.
                Datum::ByRef(b) => CatKey::ByRef(
                    crate::core_compute::canonicalize_byref_key(probe.cc_fastkind[i], b),
                ),
                Datum::Cstring(_) | Datum::Composite(_) | Datum::Expanded(_) | Datum::Internal(_) => panic!(
                    "catcache::inval_support: a Cstring/Composite/Expanded/Internal key value \
                     is not a catcache key type (GetCCHashEqFuncs rejects them)"
                ),
            };
        }
    });
    deform?;

    let kinds = &probe.cc_fastkind[..nkeys as usize];
    CatalogCacheComputeHashValue(kinds, nkeys, &v)
}

/// `PrepareToInvalidateCacheTuple(relation, tuple, newtuple, function, context)`.
///
/// `tuple_data` / `newtuple_data` are the respective tuples' user-data areas
/// (`(char *) t_data + t_hoff`), threaded in from the caller's
/// [`FormedTuple::data`] so the cache-key columns can be deformed.
pub fn prepare_to_invalidate_cache_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &RelationData<'_>,
    tuple: &HeapTupleData<'_>,
    tuple_data: &[u8],
    newtuple: Option<&HeapTupleData<'_>>,
    newtuple_data: Option<&[u8]>,
) -> PgResult<PgVec<'mcx, PrepareToInvalidateCacheTuple>> {
    // CACHE_elog(DEBUG2, "PrepareToInvalidateCacheTuple: called");
    //
    // sanity checks: RelationIsValid / HeapTupleIsValid / PointerIsValid /
    // CacheHdr != NULL are all enforced by the typed `&` arguments and the
    // always-present arena.

    let reloid = relation.rd_id; // RelationGetRelid(relation)

    // ----------------
    //  for each cache
    //     if the cache contains tuples from the specified relation
    //         compute the tuple's hash value(s) in this cache,
    //         and call the passed function to register the information.
    // ----------------
    //
    // `slist_foreach(iter, &CacheHdr->ch_caches)` — collect the matching caches
    // (running `ConditionalCatalogCacheInitializeCache` on each, as the C does
    // mid-iteration) under a single arena borrow, then drop it before computing
    // the hash values, which re-enter the arena through the `cc_tupdesc` seam.
    // Collect the matching caches in registration order (the C `ch_caches`
    // slist), then run ConditionalCatalogCacheInitializeCache on each WITHOUT
    // the arena borrow held (init opens the catalog relation and re-enters the
    // catcache), then build the probes under a fresh borrow.
    let matching: Vec<CacheIdx> = with_arena(|arena| {
        (0..arena.caches.len())
            .filter(|&idx| arena.caches[idx].cc_reloid == reloid)
            .map(CacheIdx)
            .collect()
    });
    for &cache_idx in &matching {
        // Just in case cache hasn't finished initialization yet...
        init_meta::conditional_initialize(cache_idx)?;
    }

    let probes: Vec<(CacheIdx, CacheProbe)> = with_arena(|arena| {
        let mut probes = Vec::new();
        for &cache_idx in &matching {
            let idx = cache_idx.0;
            let ccp = &arena.caches[idx];
            // After initialization the per-key fast-kind selection is set;
            // `GetCCHashEqFuncs` assigns every key column a kind.
            let mut cc_fastkind = [CCFastKind::Int4; 4];
            for k in 0..ccp.cc_nkeys as usize {
                cc_fastkind[k] = ccp.cc_fastkind[k]
                    .expect("PrepareToInvalidateCacheTuple: cache key fast-kind unset after init");
            }
            probes.push((
                cache_idx,
                CacheProbe {
                    id: ccp.id,
                    cc_nkeys: ccp.cc_nkeys,
                    cc_keyno: ccp.cc_keyno,
                    cc_fastkind,
                    cc_relisshared: ccp.cc_relisshared,
                },
            ));
        }
        Ok::<_, types_error::PgError>(probes)
    })?;

    // Up to two requests per matching cache (old + new on a hash-changing
    // update); reserve fallibly, then the fills are infallible.
    let mut requests: PgVec<'mcx, PrepareToInvalidateCacheTuple> =
        vec_with_capacity_in(mcx, probes.len() * 2)?;

    for (_cache_idx, probe) in &probes {
        let hashvalue =
            catalog_cache_compute_tuple_hash_value(mcx, probe.id, probe, tuple, tuple_data)?;
        let dbid: Oid = if probe.cc_relisshared {
            InvalidOid
        } else {
            init_small_seams::my_database_id::call()
        };

        // (*function) (ccp->id, hashvalue, dbid, context);
        requests.push(PrepareToInvalidateCacheTuple {
            cache_id: probe.id,
            hash_value: hashvalue,
            db_id: dbid,
        });

        if let Some(newtuple) = newtuple {
            let newtuple_data = newtuple_data
                .expect("PrepareToInvalidateCacheTuple: newtuple present without its data area");
            let newhashvalue = catalog_cache_compute_tuple_hash_value(
                mcx,
                probe.id,
                probe,
                newtuple,
                newtuple_data,
            )?;
            if newhashvalue != hashvalue {
                requests.push(PrepareToInvalidateCacheTuple {
                    cache_id: probe.id,
                    hash_value: newhashvalue,
                    db_id: dbid,
                });
            }
        }
    }

    Ok(requests)
}
