//! Port of `src/backend/access/hash/hashutil.c` (PostgreSQL 18.3): utility code
//! for the hash access method — hashkey computation, bucket mapping, splitpoint
//! bit math, page sanity checks, tuple conversion, in-page binary search, and
//! the LP_DEAD killer (`_hash_kill_items`).
//!
//! ## Seam-and-panic paths (unported callees in other crates)
//! * `_hash_datum2hashkey` / `_hash_datum2hashkey_type`: the index's hash
//!   support proc is reached through `backend-utils-fmgr-fmgr-seams`
//!   (`function_call1_coll`) + `backend-utils-cache-relcache-seams`
//!   (`index_getprocinfo` / `rd_indcollation`) + `backend-utils-cache-lsyscache-seams`
//!   (`get_opfamily_proc`) — all panic until those owners land.
//! * `hashoptions`: `build_reloptions` lives in
//!   `backend-access-common-reloptions-seams`.
//! * `_hash_checkpage` reads the page through the buffer manager
//!   (`with_buffer_page`/`page_is_new`/`buffer_get_block_number`) — bufmgr-seams.
//! * `_hash_kill_items` locks/unlocks buffers and marks them dirty-hint through
//!   bufmgr-seams.

use types_core::primitive::{
    BlockNumber, OffsetNumber, Oid,
};
use types_core::InvalidOid;
use types_error::{PgError, PgResult, ERROR};
use hash::hashpage::{
    Bucket, HASH_MAGIC, HASH_VERSION, HASH_METAPAGE,
    HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE, HASH_SPLITPOINT_PHASE_BITS,
    HASH_SPLITPOINT_PHASE_MASK, LH_META_PAGE,
    LH_OVERFLOW_PAGE, LH_PAGE_HAS_DEAD_TUPLES,
};
use hash::hash::{HASHSTANDARD_PROC};
use rel::Relation;
use types_tuple::heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

use bufmgr_seams as bufmgr;
use page::{
    ItemIdMarkDead, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageMut, PageRef,
};
use lsyscache_seams as lsyscache;
use relcache_seams as relcache;
use indexam_seams as indexam;
use fmgr_seams as fmgr;

use crate::hashpage::{_hash_relbuf, _hash_getbuf};
use crate::pagebytes::{
    hasho_flag, set_hasho_flag,
    INDEX_INFO_HEADER_SIZE, SIZEOF_HASH_PAGE_OPAQUE_DATA, maxalign,
};
use crate::HashScan;

// ===========================================================================
// CALC_NEW_BUCKET (hashutil.c).
// ===========================================================================

/// `CALC_NEW_BUCKET(old_bucket, lowmask)` — `old_bucket | (lowmask + 1)`.
#[inline]
fn calc_new_bucket(old_bucket: Bucket, lowmask: u32) -> Bucket {
    old_bucket | (lowmask + 1)
}

// ===========================================================================
// _hash_checkqual (hashutil.c:30)
// ===========================================================================

/// `_hash_checkqual(scan, itup)` — does the index tuple satisfy the scan
/// conditions? The body is guarded by `#ifdef NOT_USED`; in production we always
/// return true (hashgettuple set the recheck flag).
pub(crate) fn _hash_checkqual(_scan: &HashScan, _itup: &[u8]) -> bool {
    true
}

// ===========================================================================
// _hash_datum2hashkey (hashutil.c:81)
// ===========================================================================

/// `_hash_datum2hashkey(rel, key)` — given a Datum, call the index's primary
/// hash function and return the 32-bit hash key.
pub fn _hash_datum2hashkey<'m, 'r>(
    mcx: mcx::Mcx<'m>,
    rel: &Relation<'r>,
    key: &Datum<'r>,
) -> PgResult<u32> {
    // XXX assumes index has only one attribute
    let procinfo = indexam::index_getprocinfo::call(rel, 1, HASHSTANDARD_PROC)?;
    let collation = relcache::rd_indcollation::call(rel, 1)?;

    // DatumGetUInt32(FunctionCall1Coll(procinfo, collation, key)).
    //
    // `key` may be a by-reference value (a fixed-but-by-ref type such as
    // `uuid`/`macaddr`, or a varlena); it must cross the fmgr boundary on the
    // by-reference lane, not be flattened into a scalar word. The `_datum`
    // variant of the seam marshals both by-value and by-reference args.
    let res = fmgr::function_call1_coll_datum::call(
        mcx,
        procinfo.fn_oid,
        collation,
        key.clone_in(mcx)?,
    )?;
    Ok(res.as_u32())
}

// ===========================================================================
// _hash_datum2hashkey_type (hashutil.c:101)
// ===========================================================================

/// `_hash_datum2hashkey_type(rel, key, keytype)` — hash a Datum of a specified
/// type compatibly with this index (cross-type case).
pub fn _hash_datum2hashkey_type<'m, 'r>(
    mcx: mcx::Mcx<'m>,
    rel: &Relation<'r>,
    key: &Datum<'r>,
    keytype: Oid,
) -> PgResult<u32> {
    // XXX assumes index has only one attribute
    let opfamily = relcache::rd_opfamily::call(rel, 1)?;
    let hash_proc = lsyscache::get_opfamily_proc::call(opfamily, keytype, keytype, HASHSTANDARD_PROC as i16)?;
    if hash_proc == InvalidOid {
        return Err(PgError::new(
            ERROR,
            "missing support function for hash index",
        ));
    }
    let collation = relcache::rd_indcollation::call(rel, 1)?;

    // DatumGetUInt32(OidFunctionCall1Coll(hash_proc, collation, key)).
    // `key` may be by-reference — cross it on the by-reference lane (see
    // `_hash_datum2hashkey`).
    let res = fmgr::function_call1_coll_datum::call(
        mcx,
        hash_proc,
        collation,
        key.clone_in(mcx)?,
    )?;
    Ok(res.as_u32())
}

// ===========================================================================
// _hash_hashkey2bucket (hashutil.c:124)
// ===========================================================================

/// `_hash_hashkey2bucket(hashkey, maxbucket, highmask, lowmask)` — determine
/// which bucket the hashkey maps to.
pub fn _hash_hashkey2bucket(hashkey: u32, maxbucket: u32, highmask: u32, lowmask: u32) -> Bucket {
    let mut bucket = hashkey & highmask;
    if bucket > maxbucket {
        bucket &= lowmask;
    }
    bucket
}

// ===========================================================================
// pg_bitutils.h helpers used by the splitpoint math.
// ===========================================================================

/// `pg_leftmost_one_pos32(word)` (pg_bitutils.h): position of the most
/// significant set bit (0-based), `word != 0`.
pub(crate) fn pg_leftmost_one_pos32(word: u32) -> u32 {
    debug_assert!(word != 0);
    31 - word.leading_zeros()
}

/// `pg_ceil_log2_32(num)` (pg_bitutils.h).
pub(crate) fn pg_ceil_log2_32(num: u32) -> u32 {
    if num <= 1 {
        0
    } else {
        pg_leftmost_one_pos32(num - 1) + 1
    }
}

/// `pg_nextpower2_32(num)` (pg_bitutils.h): smallest power of 2 >= num.
pub(crate) fn pg_nextpower2_32(num: u32) -> u32 {
    debug_assert!(num >= 1);
    if num <= 1 {
        1
    } else {
        1u32 << (pg_leftmost_one_pos32(num - 1) + 1)
    }
}

// ===========================================================================
// _hash_spareindex (hashutil.c:141)
// ===========================================================================

/// `_hash_spareindex(num_bucket)` — returns spare index / global splitpoint
/// phase of the bucket.
pub fn _hash_spareindex(num_bucket: u32) -> u32 {
    let splitpoint_group = pg_ceil_log2_32(num_bucket);

    if splitpoint_group < HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE {
        return splitpoint_group;
    }

    // account for single-phase groups
    let mut splitpoint_phases = HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE;

    // account for multi-phase groups before splitpoint_group
    splitpoint_phases += (splitpoint_group - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE)
        << HASH_SPLITPOINT_PHASE_BITS;

    // account for phases within current group
    splitpoint_phases += ((num_bucket - 1)
        >> (splitpoint_group - (HASH_SPLITPOINT_PHASE_BITS + 1)))
        & HASH_SPLITPOINT_PHASE_MASK;

    splitpoint_phases
}

// ===========================================================================
// _hash_get_totalbuckets (hashutil.c:173)
// ===========================================================================

/// `_hash_get_totalbuckets(splitpoint_phase)` — total number of buckets
/// allocated until the given splitpoint phase.
pub fn _hash_get_totalbuckets(splitpoint_phase: u32) -> u32 {
    if splitpoint_phase < HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE {
        return 1u32 << splitpoint_phase;
    }

    // get splitpoint's group
    let mut splitpoint_group = HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE;
    splitpoint_group +=
        (splitpoint_phase - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE) >> HASH_SPLITPOINT_PHASE_BITS;

    // account for buckets before splitpoint_group
    let mut total_buckets = 1u32 << (splitpoint_group - 1);

    // account for buckets within splitpoint_group
    let phases_within_splitpoint_group = ((splitpoint_phase
        - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE)
        & HASH_SPLITPOINT_PHASE_MASK)
        + 1;
    total_buckets += ((1u32 << (splitpoint_group - 1)) >> HASH_SPLITPOINT_PHASE_BITS)
        * phases_within_splitpoint_group;

    total_buckets
}

// ===========================================================================
// _hash_checkpage (hashutil.c:209)
// ===========================================================================

/// `_hash_checkpage(rel, buf, flags)` — sanity checks on the format of all hash
/// pages. `flags`, if nonzero, is a bitwise OR of the acceptable page types.
pub fn _hash_checkpage<'mcx>(
    _rel: &Relation<'mcx>,
    buf: types_storage::Buffer,
    flags: i32,
) -> PgResult<()> {
    // Defend against the all-zero page case.
    if bufmgr::page_is_new::call(buf)? {
        let blkno = bufmgr::buffer_get_block_number::call(buf);
        return Err(PgError::new(
            ERROR,
            &alloc::format!("index contains unexpected zero page at block {blkno}"),
        ));
    }

    let mut result: PgResult<()> = Ok(());
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        let pref = PageRef::new(page)?;

        // Additionally check that the special area looks sane.
        if page::PageGetSpecialSize(&pref) as usize
            != maxalign(SIZEOF_HASH_PAGE_OPAQUE_DATA)
        {
            result = Err(PgError::new(ERROR, "index contains corrupted page"));
            return Ok(());
        }

        if flags != 0 {
            let opaque_flag = hasho_flag(page);
            if (opaque_flag as i32 & flags) == 0 {
                result = Err(PgError::new(ERROR, "index contains corrupted page"));
                return Ok(());
            }
        }

        // When checking the metapage, also verify magic number and version.
        if flags == LH_META_PAGE as i32 {
            let magic = crate::pagebytes::meta_get_u32(page, crate::pagebytes::META_OFF_MAGIC);
            if magic != HASH_MAGIC {
                result = Err(PgError::new(ERROR, "index is not a hash index"));
                return Ok(());
            }
            let version = crate::pagebytes::meta_get_u32(page, crate::pagebytes::META_OFF_VERSION);
            if version != HASH_VERSION {
                result = Err(PgError::new(ERROR, "index has wrong hash version"));
                return Ok(());
            }
        }
        Ok(())
    })?;
    result
}

// ===========================================================================
// hashoptions (hashutil.c:274)
// ===========================================================================

/// `hashoptions(reloptions, validate)` — parse the hash index reloptions
/// (`fillfactor`). Returns the serialized `HashOptions` bytea (`None` for the C
/// NULL).
pub fn hashoptions(reloptions: Option<&[u8]>, validate: bool) -> PgResult<Option<alloc::vec::Vec<u8>>> {
    reloptions_seams::build_reloptions_hash::call(reloptions, validate)
}

// ===========================================================================
// _hash_get_indextuple_hashkey (hashutil.c:290)
// ===========================================================================

/// `_hash_get_indextuple_hashkey(itup)` — get the hash index tuple's hash key
/// value. `itup` is the on-page item bytes. The hash key is the first attribute
/// (a non-null uint32) at `IndexInfoFindDataOffset(t_info)`.
pub fn _hash_get_indextuple_hashkey(itup: &[u8]) -> u32 {
    let off = INDEX_INFO_HEADER_SIZE; // single non-null attr, no null bitmap
    u32::from_ne_bytes([itup[off], itup[off + 1], itup[off + 2], itup[off + 3]])
}

// ===========================================================================
// _hash_convert_tuple (hashutil.c:317)
// ===========================================================================

/// `_hash_convert_tuple(index, user_values, user_isnull, index_values,
/// index_isnull)` — convert raw index data to hash key. Returns true on
/// success, false if the (single) user value is null. Fills `index_values[0]`
/// with `UInt32GetDatum(hashkey)` and `index_isnull[0]` with false.
pub fn _hash_convert_tuple<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    index: &Relation<'mcx>,
    user_values: &[Datum<'mcx>],
    user_isnull: &[bool],
    index_values: &mut [Datum<'mcx>],
    index_isnull: &mut [bool],
) -> PgResult<bool> {
    // We do not insert null values into hash indexes.
    if user_isnull[0] {
        return Ok(false);
    }

    let hashkey = _hash_datum2hashkey(mcx, index, &user_values[0])?;
    // UInt32GetDatum(hashkey)
    index_values[0] = Datum::ByVal(hashkey as usize);
    index_isnull[0] = false;
    Ok(true)
}

// ===========================================================================
// _hash_binsearch (hashutil.c:349)
// ===========================================================================

/// `_hash_binsearch(page, hash_value)` — offset of the first index entry with
/// `hashkey >= hash_value`, or maxoffset+1 if greater than all.
pub(crate) fn _hash_binsearch(page: &PageRef<'_>, hash_value: u32) -> PgResult<OffsetNumber> {
    // Loop invariant: lower <= desired place <= upper
    let mut upper = PageGetMaxOffsetNumber(page) + 1;
    let mut lower: OffsetNumber = 1; // FirstOffsetNumber

    while upper > lower {
        let off = (upper + lower) / 2;
        let iid = PageGetItemId(page, off)?;
        let itup = PageGetItem(page, &iid)?;
        let hashkey = _hash_get_indextuple_hashkey(itup);
        if hashkey < hash_value {
            lower = off + 1;
        } else {
            upper = off;
        }
    }

    Ok(lower)
}

// ===========================================================================
// _hash_binsearch_last (hashutil.c:387)
// ===========================================================================

/// `_hash_binsearch_last(page, hash_value)` — like [`_hash_binsearch`] but the
/// offset of the last matching item; range 0..maxoffset.
pub(crate) fn _hash_binsearch_last(page: &PageRef<'_>, hash_value: u32) -> PgResult<OffsetNumber> {
    let mut upper = PageGetMaxOffsetNumber(page);
    // FirstOffsetNumber - 1 == 0, on OffsetNumber (u16).
    let mut lower: i32 = 0;

    while (upper as i32) > lower {
        let off = ((upper as i32 + lower + 1) / 2) as OffsetNumber;
        let iid = PageGetItemId(page, off)?;
        let itup = PageGetItem(page, &iid)?;
        let hashkey = _hash_get_indextuple_hashkey(itup);
        if hashkey > hash_value {
            upper = off - 1;
        } else {
            lower = off as i32;
        }
    }

    Ok(lower as OffsetNumber)
}

// ===========================================================================
// _hash_get_oldblock_from_newbucket (hashutil.c:421)
// ===========================================================================

/// `_hash_get_oldblock_from_newbucket(rel, new_bucket)` — block number of the
/// bucket from which the (new) bucket is being split.
pub fn _hash_get_oldblock_from_newbucket<'mcx>(
    rel: &Relation<'mcx>,
    new_bucket: Bucket,
) -> PgResult<BlockNumber> {
    // Masking the most significant bit of new bucket gives us old bucket.
    let mask = (1u32 << pg_leftmost_one_pos32(new_bucket)) - 1;
    let old_bucket = new_bucket & mask;

    let metabuf = _hash_getbuf(rel, HASH_METAPAGE, hash::hashpage::HASH_READ, LH_META_PAGE as i32)?;
    let blkno = crate::hashpage::with_metap(metabuf, |metap| {
        crate::hashpage::bucket_to_blkno(metap, old_bucket)
    })?;
    _hash_relbuf(rel, metabuf);

    Ok(blkno)
}

// ===========================================================================
// _hash_get_newblock_from_oldbucket (hashutil.c:460)
// ===========================================================================

/// `_hash_get_newblock_from_oldbucket(rel, old_bucket)` — block number of the
/// bucket generated after split from the old bucket.
pub fn _hash_get_newblock_from_oldbucket<'mcx>(
    rel: &Relation<'mcx>,
    old_bucket: Bucket,
) -> PgResult<BlockNumber> {
    let metabuf = _hash_getbuf(rel, HASH_METAPAGE, hash::hashpage::HASH_READ, LH_META_PAGE as i32)?;
    let (lowmask, maxbucket) = crate::hashpage::with_metap(metabuf, |metap| {
        (metap.hashm_lowmask, metap.hashm_maxbucket)
    })?;
    let new_bucket = _hash_get_newbucket_from_oldbucket(rel, old_bucket, lowmask, maxbucket)?;
    let blkno = crate::hashpage::with_metap(metabuf, |metap| {
        crate::hashpage::bucket_to_blkno(metap, new_bucket)
    })?;
    _hash_relbuf(rel, metabuf);

    Ok(blkno)
}

// ===========================================================================
// _hash_get_newbucket_from_oldbucket (hashutil.c:493)
// ===========================================================================

/// `_hash_get_newbucket_from_oldbucket(rel, old_bucket, lowmask, maxbucket)` —
/// the new bucket generated after split from the current (old) bucket.
pub fn _hash_get_newbucket_from_oldbucket<'mcx>(
    _rel: &Relation<'mcx>,
    old_bucket: Bucket,
    lowmask: u32,
    maxbucket: u32,
) -> PgResult<Bucket> {
    let mut new_bucket = calc_new_bucket(old_bucket, lowmask);
    if new_bucket > maxbucket {
        let lowmask = lowmask >> 1;
        new_bucket = calc_new_bucket(old_bucket, lowmask);
    }
    Ok(new_bucket)
}

// ===========================================================================
// _hash_kill_items (hashutil.c:535)
// ===========================================================================

/// `_hash_kill_items(scan)` — set LP_DEAD state for items an indexscan caller
/// reported were killed.
pub fn _hash_kill_items<'mcx>(scan: &mut HashScan<'mcx>) -> PgResult<()> {
    let num_killed = scan.opaque.numKilled;
    debug_assert!(num_killed > 0);
    debug_assert!(!scan.opaque.killedItems.is_empty());
    debug_assert!(hash::hashpage::HashScanPosIsValid(&scan.opaque.currPos));

    // Always reset the scan state, so we don't look for same items on other
    // pages.
    scan.opaque.numKilled = 0;

    let blkno = scan.opaque.currPos.currPage;
    let rel = scan.indexRelation.alias();

    let mut have_pin = false;
    let buf;
    if hash::hashpage::HashScanPosIsPinned(&scan.opaque.currPos) {
        // We already have pin on this buffer, so just acquire lock on it.
        have_pin = true;
        buf = scan.opaque.currPos.buf;
        bufmgr::lock_buffer::call(buf, BUFFER_LOCK_SHARE)?;
    } else {
        buf = _hash_getbuf(&rel, blkno, hash::hashpage::HASH_READ, LH_OVERFLOW_PAGE as i32)?;
    }

    let mut killedsomething = false;

    // Collect the killed-item match data so we can mutate the page in one pass.
    let killed: alloc::vec::Vec<(OffsetNumber, ItemPointerData)> = (0..num_killed as usize)
        .map(|i| {
            let item_index = scan.opaque.killedItems[i];
            let curr = &scan.opaque.currPos.items[item_index as usize];
            debug_assert!(
                item_index >= scan.opaque.currPos.firstItem
                    && item_index <= scan.opaque.currPos.lastItem
            );
            (curr.indexOffset, curr.heapTid)
        })
        .collect();

    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        let maxoff = {
            let pref = PageRef::new(page)?;
            PageGetMaxOffsetNumber(&pref)
        };

        for &(start_off, heap_tid) in &killed {
            let mut offnum = start_off;
            while offnum <= maxoff {
                let (iid, matches) = {
                    let pref = PageRef::new(page)?;
                    let iid = PageGetItemId(&pref, offnum)?;
                    let ituple = PageGetItem(&pref, &iid)?;
                    // ItemPointerEquals(&ituple->t_tid, &currItem->heapTid)
                    let t_tid = crate::pagebytes::index_tuple_tid(ituple);
                    (iid, t_tid == heap_tid)
                };
                if matches {
                    // found the item: ItemIdMarkDead(iid)
                    let mut iid = iid;
                    ItemIdMarkDead(&mut iid);
                    let mut pmut = PageMut::new(page)?;
                    page::PageSetItemId(&mut pmut, offnum, iid)?;
                    killedsomething = true;
                    break; // out of inner search loop
                }
                offnum += 1; // OffsetNumberNext
            }
        }

        // Whenever we mark anything LP_DEAD, also set LH_PAGE_HAS_DEAD_TUPLES.
        if killedsomething {
            let flag = hasho_flag(page);
            set_hasho_flag(page, flag | LH_PAGE_HAS_DEAD_TUPLES);
        }
        Ok(())
    })?;

    if killedsomething {
        bufmgr::mark_buffer_dirty_hint::call(buf, true);
    }

    if scan.opaque.hashso_bucket_buf == scan.opaque.currPos.buf || have_pin {
        bufmgr::lock_buffer::call(scan.opaque.currPos.buf, BUFFER_LOCK_UNLOCK)?;
    } else {
        _hash_relbuf(&rel, buf);
    }
    Ok(())
}

// `BUFFER_LOCK_*` (storage/bufmgr.h) — the `mode` values for `LockBuffer`.
pub(crate) const BUFFER_LOCK_UNLOCK: i32 = 0;
pub(crate) const BUFFER_LOCK_SHARE: i32 = 1;
pub(crate) const BUFFER_LOCK_EXCLUSIVE: i32 = 2;

// Re-export bit-math + accessors used across modules.
#[allow(unused_imports)]
pub(crate) use crate::pagebytes::maxalign as _maxalign_reexport;

extern crate alloc;

// silence unused import warning when AttrNumber only used in some configs
