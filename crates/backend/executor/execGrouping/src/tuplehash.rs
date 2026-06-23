//! The `tuplehash` simplehash specialization used by `execGrouping.c`.
//!
//! `execGrouping.c` instantiates `lib/simplehash.h` with:
//!
//! ```c
//! #define SH_PREFIX tuplehash
//! #define SH_ELEMENT_TYPE TupleHashEntryData
//! #define SH_KEY_TYPE MinimalTuple
//! #define SH_KEY firstTuple
//! #define SH_HASH_KEY(tb, key) TupleHashTableHash_internal(tb, key)
//! #define SH_EQUAL(tb, a, b) TupleHashTableMatch(tb, a, b) == 0
//! #define SH_SCOPE extern
//! #define SH_STORE_HASH
//! #define SH_GET_HASH(tb, a) a->hash
//! #define SH_DEFINE
//! ```
//!
//! Ported 1:1 with `lib/simplehash.h`, keeping every branch and growth
//! heuristic. The bucket array is the owned [`TuplehashHash`] / `PgVec` from
//! `types-nodes`. `SH_HASH_KEY`/`SH_EQUAL` call back into the owning
//! `TupleHashTable` (the C `private_data`), so this module is parameterized over
//! a [`TuplehashOps`] trait supplying those two callbacks. Every execGrouping
//! use passes `MinimalTuple::None` as the key (the C "NULL flags reference to
//! inputslot" sentinel); the real tuple reaches the callbacks through the
//! owning table's input slot.

use ::mcx::Mcx;
use ::types_error::PgResult;
use ::nodes::nodeagg::{
    TupleHashEntryData, TuplehashHash, TUPLEHASH_STATUS_EMPTY, TUPLEHASH_STATUS_IN_USE,
};
use ::nodes::EStateData;

/// `SH_MAX_SIZE` = `((uint64) PG_UINT32_MAX) + 1`.
const SH_MAX_SIZE: u64 = (u32::MAX as u64) + 1;
/// `SH_FILLFACTOR` (0.9).
const SH_FILLFACTOR: f64 = 0.9;
/// `SH_MAX_FILLFACTOR` (0.98).
const SH_MAX_FILLFACTOR: f64 = 0.98;
/// `SH_GROW_MAX_DIB` (25).
const SH_GROW_MAX_DIB: u32 = 25;
/// `SH_GROW_MAX_MOVE` (150).
const SH_GROW_MAX_MOVE: i32 = 150;
/// `SH_GROW_MIN_FILLFACTOR` (0.1).
const SH_GROW_MIN_FILLFACTOR: f64 = 0.1;

/// The C `MinimalTuple` key threaded through the simplehash. In every
/// execGrouping use it is `NULL` (referencing the table's input slot), so the
/// only key value that ever flows here is the unit `()` sentinel.
type Key = ();

/// Callbacks supplied by the owning `TupleHashTable` (`SH_HASH_KEY` /
/// `SH_EQUAL`), i.e. `TupleHashTableHash_internal` / `TupleHashTableMatch`,
/// which fetch `tb->private_data` in C.
pub trait TuplehashOps<'mcx> {
    /// `SH_HASH_KEY(tb, key)` -> `TupleHashTableHash_internal(tb, NULL)`.
    fn hash_key(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32>;

    /// `SH_EQUAL(tb, a, b)` -> `TupleHashTableMatch(tb, a, b) == 0`. `a_index`
    /// is the stored table entry's bucket; `b` is the lookup key (always the
    /// input-slot sentinel). Returns whether the two tuples are equal.
    fn equal(
        &mut self,
        tb: &TuplehashHash<'mcx>,
        a_index: usize,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>;
}

/// `tuplehash_iterator` — the `SH_ITERATOR` generated struct.
#[derive(Clone, Copy, Debug)]
pub struct Iter {
    pub(crate) cur: u32,
    pub(crate) end: u32,
    pub(crate) done: bool,
}

/// `SH_COMPUTE_SIZE`: round `newsize` up to a power of two suitable for the
/// bucket array, erroring out if the allocation would overflow `Size`.
fn sh_compute_size(newsize: u64) -> PgResult<u64> {
    // supporting zero sized hashes would complicate matters
    let mut size = newsize.max(2);

    // round up size to the next power of 2, that's how bucketing works
    size = pg_nextpower2_64(size);
    debug_assert!(size <= SH_MAX_SIZE);

    // Verify that allocation of ->data is possible on this platform, without
    // overflowing Size.
    if (core::mem::size_of::<TupleHashEntryData>() as u64).saturating_mul(size)
        >= (usize::MAX as u64) / 2
    {
        return Err(::types_error::PgError::error("hash table too large"));
    }

    Ok(size)
}

/// `pg_nextpower2_64`: next higher power of two at or above `num`.
fn pg_nextpower2_64(num: u64) -> u64 {
    debug_assert!(num > 0 && num <= u64::MAX / 2 + 1);
    if num & (num - 1) == 0 {
        return num; // already power 2
    }
    1u64 << (pg_leftmost_one_pos64(num) + 1)
}

/// `pg_leftmost_one_pos64`: position (0-based) of the most-significant set bit.
fn pg_leftmost_one_pos64(word: u64) -> u32 {
    debug_assert!(word != 0);
    63 - word.leading_zeros()
}

/// `tuplehash_create`: allocate the `tuplehash_hash` with room for `nelements`
/// distinct members.
pub fn create<'mcx>(mcx: Mcx<'mcx>, nelements: u32) -> PgResult<TuplehashHash<'mcx>> {
    // increase nelements by fillfactor, want to store nelements elements
    let size = (SH_MAX_SIZE as f64).min(nelements as f64 / SH_FILLFACTOR);
    let size = sh_compute_size(size as u64)?;

    let data = alloc_zeroed_buckets(mcx, size as usize)?;

    let mut tb = TuplehashHash {
        size: 0,
        members: 0,
        sizemask: 0,
        grow_threshold: 0,
        data,
    };
    update_parameters(&mut tb, size)?;
    Ok(tb)
}

/// Allocate `size` zeroed (EMPTY) buckets, OOM-safe via `try_reserve`.
fn alloc_zeroed_buckets<'mcx>(
    mcx: Mcx<'mcx>,
    size: usize,
) -> PgResult<::mcx::PgVec<'mcx, TupleHashEntryData<'mcx>>> {
    let mut data: ::mcx::PgVec<'mcx, TupleHashEntryData<'mcx>> = ::mcx::PgVec::new_in(mcx);
    data.try_reserve(size).map_err(|_| mcx.oom(size))?;
    for _ in 0..size {
        data.push(TupleHashEntryData::empty(mcx));
    }
    Ok(data)
}

/// `SH_UPDATE_PARAMETERS`: recompute `size`/`sizemask`/`grow_threshold`.
fn update_parameters(tb: &mut TuplehashHash, newsize: u64) -> PgResult<()> {
    let size = sh_compute_size(newsize)?;

    // now set size
    tb.size = size;
    tb.sizemask = (size - 1) as u32;

    // Compute the next threshold at which we need to grow the hash table again.
    if tb.size == SH_MAX_SIZE {
        tb.grow_threshold = (tb.size as f64 * SH_MAX_FILLFACTOR) as u32;
    } else {
        tb.grow_threshold = (tb.size as f64 * SH_FILLFACTOR) as u32;
    }
    Ok(())
}

/// `SH_INITIAL_BUCKET`: the optimal bucket for `hash`.
fn initial_bucket(tb: &TuplehashHash, hash: u32) -> u32 {
    hash & tb.sizemask
}

/// `SH_NEXT`: next bucket after `curelem`, handling wraparound.
fn next(tb: &TuplehashHash, curelem: u32) -> u32 {
    curelem.wrapping_add(1) & tb.sizemask
}

/// `SH_PREV`: bucket before `curelem`, handling wraparound.
fn prev(tb: &TuplehashHash, curelem: u32) -> u32 {
    curelem.wrapping_sub(1) & tb.sizemask
}

/// `SH_DISTANCE_FROM_OPTIMAL`.
fn distance_from_optimal(tb: &TuplehashHash, optimal: u32, bucket: u32) -> u32 {
    if optimal <= bucket {
        bucket - optimal
    } else {
        (tb.size as u32).wrapping_add(bucket).wrapping_sub(optimal)
    }
}

/// `SH_ENTRY_HASH` (with `SH_STORE_HASH`, returns `SH_GET_HASH`).
fn entry_hash(entry: &TupleHashEntryData) -> u32 {
    entry.hash
}

/// `SH_COMPARE_KEYS` (with `SH_STORE_HASH`): `ahash == b->hash && SH_EQUAL`.
fn compare_keys<'mcx>(
    tb: &TuplehashHash<'mcx>,
    ops: &mut dyn TuplehashOps<'mcx>,
    ahash: u32,
    b_index: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if ahash != tb.data[b_index].hash {
        return Ok(false);
    }
    ops.equal(tb, b_index, estate)
}

/// `tuplehash_reset`: empty the bucket array and reset `members`.
pub fn reset(tb: &mut TuplehashHash) {
    for entry in tb.data.iter_mut() {
        entry.firstTuple = None;
        entry.additional.clear();
        entry.status = TUPLEHASH_STATUS_EMPTY;
        entry.hash = 0;
    }
    tb.members = 0;
}

/// `SH_GROW`: grow the bucket array to at least `newsize` buckets.
fn grow<'mcx>(mcx: Mcx<'mcx>, tb: &mut TuplehashHash<'mcx>, newsize: u64) -> PgResult<()> {
    let oldsize = tb.size;

    debug_assert_eq!(oldsize, pg_nextpower2_64(oldsize));
    debug_assert_ne!(oldsize, SH_MAX_SIZE);
    debug_assert!(oldsize < newsize);

    let newsize = sh_compute_size(newsize)?;

    let newdata = alloc_zeroed_buckets(mcx, newsize as usize)?;
    let olddata = core::mem::replace(&mut tb.data, newdata);

    // Update parameters for new table after allocation succeeds to avoid
    // inconsistent state on OOM.
    update_parameters(tb, newsize)?;

    // search for the first element in the hash that's not wrapped around
    let mut startelem: u32 = 0;
    for (i, oldentry) in olddata.iter().enumerate().take(oldsize as usize) {
        if oldentry.status != TUPLEHASH_STATUS_IN_USE {
            startelem = i as u32;
            break;
        }
        let optimal = initial_bucket(tb, entry_hash(oldentry));
        if optimal == i as u32 {
            startelem = i as u32;
            break;
        }
    }

    // Move (not copy) the owned entries out of the old bucket array into the
    // new one.
    let mut olddata = olddata;
    let mut copyelem = startelem as usize;
    for _ in 0..oldsize {
        if olddata[copyelem].status == TUPLEHASH_STATUS_IN_USE {
            let oldentry = core::mem::replace(
                &mut olddata[copyelem],
                TupleHashEntryData::empty(mcx),
            );
            let startelem2 = initial_bucket(tb, entry_hash(&oldentry));
            let mut curelem = startelem2;

            // find empty element to put data into
            while tb.data[curelem as usize].status != TUPLEHASH_STATUS_EMPTY {
                curelem = next(tb, curelem);
            }

            // copy entry to new slot
            tb.data[curelem as usize] = oldentry;
        }

        // can't use SH_NEXT here, would use new size
        copyelem += 1;
        if copyelem >= oldsize as usize {
            copyelem = 0;
        }
    }

    Ok(())
}

/// `tuplehash_insert_hash` / `SH_INSERT_HASH_INTERNAL`: insert the input-slot
/// key using the precomputed `hash`. Returns `(index, found)`.
///
/// `firstTuple` is intentionally *not* set for a freshly-created entry (it stays
/// `None`); the caller (`lookup_internal`) fills it, matching the C division of
/// labor (`SH_INSERT_HASH_INTERNAL` only sets the key, which here is the NULL
/// sentinel, i.e. nothing).
pub fn insert_hash<'mcx>(
    mcx: Mcx<'mcx>,
    tb: &mut TuplehashHash<'mcx>,
    ops: &mut dyn TuplehashOps<'mcx>,
    hash: u32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(usize, bool)> {
    // 'restart:' label in C
    'restart: loop {
        let mut insertdist: u32 = 0;

        // We do the grow check even if the key is actually present.
        if tb.members >= tb.grow_threshold {
            if tb.size == SH_MAX_SIZE {
                return Err(::types_error::PgError::error("hash table size exceeded"));
            }
            let twice = tb.size * 2;
            grow(mcx, tb, twice)?;
        }

        // perform insert, start bucket search at optimal location
        let startelem = initial_bucket(tb, hash);
        let mut curelem = startelem;
        loop {
            // any empty bucket can directly be used
            if tb.data[curelem as usize].status == TUPLEHASH_STATUS_EMPTY {
                tb.members += 1;
                let entry = &mut tb.data[curelem as usize];
                // SH_KEY assignment (firstTuple = key): key is the NULL
                // sentinel, so nothing is stored here.
                entry.hash = hash;
                entry.status = TUPLEHASH_STATUS_IN_USE;
                return Ok((curelem as usize, false));
            }

            // If not empty, either a match (done) or decide skip/move.
            if compare_keys(tb, ops, hash, curelem as usize, estate)? {
                debug_assert_eq!(tb.data[curelem as usize].status, TUPLEHASH_STATUS_IN_USE);
                return Ok((curelem as usize, true));
            }

            let curhash = entry_hash(&tb.data[curelem as usize]);
            let curoptimal = initial_bucket(tb, curhash);
            let curdist = distance_from_optimal(tb, curoptimal, curelem);

            if insertdist > curdist {
                let mut emptyelem = curelem;
                let mut emptydist: i32 = 0;

                // find next empty bucket
                let mut lastentry = loop {
                    emptyelem = next(tb, emptyelem);

                    if tb.data[emptyelem as usize].status == TUPLEHASH_STATUS_EMPTY {
                        break emptyelem;
                    }

                    // To avoid overly imbalanced hashtables, grow if collisions
                    // would require moving a lot of entries. Don't grow if too
                    // empty.
                    emptydist += 1;
                    if emptydist > SH_GROW_MAX_MOVE
                        && (tb.members as f64 / tb.size as f64) >= SH_GROW_MIN_FILLFACTOR
                    {
                        tb.grow_threshold = 0;
                        continue 'restart;
                    }
                };

                // shift forward, starting at last occupied element
                let mut moveelem = emptyelem;
                while moveelem != curelem {
                    moveelem = prev(tb, moveelem);
                    let moveentry = core::mem::replace(
                        &mut tb.data[moveelem as usize],
                        TupleHashEntryData::empty(mcx),
                    );
                    tb.data[lastentry as usize] = moveentry;
                    lastentry = moveelem;
                }

                // and fill the now empty spot
                tb.members += 1;
                let entry = &mut tb.data[curelem as usize];
                entry.hash = hash;
                entry.status = TUPLEHASH_STATUS_IN_USE;
                return Ok((curelem as usize, false));
            }

            curelem = next(tb, curelem);
            insertdist += 1;

            // To avoid overly imbalanced hashtables, grow if collisions lead to
            // large runs. Don't grow if too empty.
            if insertdist > SH_GROW_MAX_DIB
                && (tb.members as f64 / tb.size as f64) >= SH_GROW_MIN_FILLFACTOR
            {
                tb.grow_threshold = 0;
                continue 'restart;
            }
        }
    }
}

/// `tuplehash_lookup_hash` / `SH_LOOKUP_HASH_INTERNAL`: look up the input-slot
/// key with the precomputed `hash`; returns the bucket index, or `None`.
pub fn lookup_hash<'mcx>(
    tb: &mut TuplehashHash<'mcx>,
    ops: &mut dyn TuplehashOps<'mcx>,
    hash: u32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<usize>> {
    let startelem = initial_bucket(tb, hash);
    let mut curelem = startelem;

    loop {
        if tb.data[curelem as usize].status == TUPLEHASH_STATUS_EMPTY {
            return Ok(None);
        }

        debug_assert_eq!(tb.data[curelem as usize].status, TUPLEHASH_STATUS_IN_USE);

        if compare_keys(tb, ops, hash, curelem as usize, estate)? {
            return Ok(Some(curelem as usize));
        }

        curelem = next(tb, curelem);
    }
}

/// `tuplehash_lookup` / `SH_LOOKUP`: compute the hash, then look up.
pub fn lookup<'mcx>(
    tb: &mut TuplehashHash<'mcx>,
    ops: &mut dyn TuplehashOps<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<usize>> {
    let hash = ops.hash_key(estate)?;
    lookup_hash(tb, ops, hash, estate)
}

/// `tuplehash_start_iterate` / `SH_START_ITERATE`.
pub fn start_iterate(tb: &TuplehashHash) -> Iter {
    let mut startelem: u64 = u64::MAX;

    // Search for the first empty element. As deletions during iterations are
    // supported, start/end at an element that cannot be affected by shifting.
    for (i, entry) in tb.data.iter().enumerate().take(tb.size as usize) {
        if entry.status != TUPLEHASH_STATUS_IN_USE {
            startelem = i as u64;
            break;
        }
    }

    // we should have found an empty element
    debug_assert!(startelem < SH_MAX_SIZE);

    // Iterate backwards, allowing the current element to be deleted.
    Iter {
        cur: startelem as u32,
        end: startelem as u32,
        done: false,
    }
}

/// `tuplehash_iterate` / `SH_ITERATE`: return the next occupied bucket index,
/// or `None` if done.
pub fn iterate(tb: &TuplehashHash, iter: &mut Iter) -> Option<usize> {
    debug_assert!((iter.cur as u64) < tb.size);
    debug_assert!((iter.end as u64) < tb.size);

    while !iter.done {
        let elem = iter.cur;

        // next element in backward direction
        iter.cur = iter.cur.wrapping_sub(1) & tb.sizemask;

        if (iter.cur & tb.sizemask) == (iter.end & tb.sizemask) {
            iter.done = true;
        }
        if tb.data[elem as usize].status == TUPLEHASH_STATUS_IN_USE {
            return Some(elem as usize);
        }
    }

    None
}
