//! Port of `src/backend/access/hash/hashsort.c` (PostgreSQL 18.3) — sort
//! tuples for insertion into a new hash index.
//!
//! When building a very large hash index, we pre-sort the tuples by bucket
//! number to improve locality of access, avoiding thrashing. `tuplesort.c` does
//! the sort, reached through `backend-utils-sort-tuplesort-seams`. The genuine
//! own logic here — the bucket-mask arithmetic of [`_h_spoolinit`], the
//! build/insert loop and progress accounting of [`_h_indexbuild`], and the
//! debug-only sorted-order assertion — is ported 1:1. `_hash_doinsert` is the
//! sibling `hash-core` unit (called directly), interrupts/progress are seams.

use mcx::{alloc_in, Mcx, PgBox};
use types_error::PgResult;
use ::nodes::Tuplesortstate;
use rel::Relation;
use types_tuple::heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

use hash_core as core;
use tuplesort_seams as tuplesort;

// `pgstat_progress_update_param` index codes (commands/progress.h).
const PROGRESS_CREATEIDX_TUPLES_DONE: i32 = 9;

/// `TUPLESORT_NONE` (`utils/tuplesort.h`) — no extra sort options.
const TUPLESORT_NONE: i32 = 0;

/// `struct HSpool` — status record for the spooling/sorting phase (hashsort.c).
///
/// We sort the hash keys by the buckets they belong to, then by the hash values
/// themselves, to optimize insertions onto hash pages. The masks below feed
/// [`_hash_hashkey2bucket`](core::_hash_hashkey2bucket) to determine the bucket
/// of a given hash key.
pub struct HSpool<'mcx> {
    /// `Tuplesortstate *sortstate` — state data for tuplesort.c.
    pub sortstate: PgBox<'mcx, Tuplesortstate<'mcx>>,
    /// `Relation index` — the index being built.
    pub index: Relation<'mcx>,
    /// `uint32 high_mask`.
    pub high_mask: u32,
    /// `uint32 low_mask`.
    pub low_mask: u32,
    /// `uint32 max_buckets`.
    pub max_buckets: u32,
}

/// `_h_spoolinit()` — create and initialize a spool structure.
pub fn _h_spoolinit<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    num_buckets: u32,
) -> PgResult<HSpool<'mcx>> {
    // Determine the bitmask for hash code values. Since there are currently
    // num_buckets buckets in the index, the appropriate mask is computed as
    // follows.
    //
    // NOTE: this hash mask calculation should be in sync with the similar
    // calculation in _hash_init_metabuffer.
    let high_mask = pg_nextpower2_32(num_buckets + 1) - 1;
    let low_mask = high_mask >> 1;
    let max_buckets = num_buckets - 1;

    // We size the sort area as maintenance_work_mem rather than work_mem to
    // speed index creation. This is OK since a single backend can't run
    // multiple index creations in parallel.
    let sortstate = tuplesort::tuplesort_begin_index_hash::call(
        mcx,
        heap,
        index,
        high_mask,
        low_mask,
        max_buckets,
        guc_seams::maintenance_work_mem::call(),
        TUPLESORT_NONE,
    )?;

    Ok(HSpool {
        sortstate: alloc_in(mcx, sortstate)?,
        index: index.alias(),
        high_mask,
        low_mask,
        max_buckets,
    })
}

/// `_h_spooldestroy()` — clean up a spool structure and its substructures.
pub fn _h_spooldestroy(hspool: HSpool<'_>) -> PgResult<()> {
    tuplesort::tuplesort_end::call(hspool.sortstate)?;
    // pfree(hspool): the owned HSpool is dropped here.
    Ok(())
}

/// `_h_spool()` — spool an index entry into the sort file.
pub fn _h_spool<'mcx>(
    hspool: &mut HSpool<'mcx>,
    self_tid: ItemPointerData,
    values: &[Datum<'mcx>],
    isnull: &[bool],
) -> PgResult<()> {
    let index = hspool.index.alias();
    tuplesort::tuplesort_putindextuplevalues::call(
        &mut hspool.sortstate,
        &index,
        self_tid,
        values,
        isnull,
    )
}

/// `_h_indexbuild()` — given a spool loaded by successive calls to `_h_spool`,
/// create an entire index. Takes `&mut HSpool` because the mutating
/// `tuplesort_*` calls thread the single owned `Tuplesortstate` (C: the
/// `hspool->sortstate` pointer).
pub fn _h_indexbuild<'mcx>(hspool: &mut HSpool<'mcx>, heap_rel: &Relation<'mcx>) -> PgResult<()> {
    let mut tups_done: i64 = 0;
    #[cfg(debug_assertions)]
    let mut hashkey: u32 = 0;

    let index = hspool.index.alias();

    tuplesort::tuplesort_performsort::call(&mut hspool.sortstate)?;

    while let Some(itup) = tuplesort::tuplesort_getindextuple::call(&mut hspool.sortstate, true)? {
        // Technically, it isn't critical that hash keys be found in sorted
        // order (the sort is only a locality optimization). It still seems good
        // to test tuplesort.c's hash-index sort handling through an assertion.
        #[cfg(debug_assertions)]
        {
            let lasthashkey = hashkey;
            hashkey = core::_hash_hashkey2bucket(
                core::_hash_get_indextuple_hashkey(&itup),
                hspool.max_buckets,
                hspool.high_mask,
                hspool.low_mask,
            );
            debug_assert!(hashkey >= lasthashkey);
        }

        // the tuples are sorted by hashkey, so pass 'sorted' as true
        core::_hash_doinsert(&index, &itup, heap_rel, true)?;

        // allow insertion phase to be interrupted, and track progress
        postgres_seams::check_for_interrupts::call()?;

        tups_done += 1;
        activity_small::backend_progress::pgstat_progress_update_param(
            PROGRESS_CREATEIDX_TUPLES_DONE,
            tups_done,
        );
    }

    Ok(())
}

/// `pg_nextpower2_32(num)` (`port/pg_bitutils.h`) — the smallest power of 2
/// `>= num` (num must be > 0 and the result must fit in 32 bits).
#[inline]
fn pg_nextpower2_32(num: u32) -> u32 {
    debug_assert!(num > 0);
    // 1 << (32 - leading_zeros(num - 1)) for num > 1; 1 for num == 1.
    if num <= 1 {
        return 1;
    }
    1u32 << (32 - (num - 1).leading_zeros())
}
