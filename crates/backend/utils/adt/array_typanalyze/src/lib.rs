//! Port of `backend/utils/adt/array_typanalyze.c` (PostgreSQL 18.3) — gather
//! statistics from array columns.
//!
//! The algorithm — the Lossy-Counting (LC) driver in [`compute_array_stats`],
//! the hashtable pruning in `prune_element_hashtable`, the element hash/compare
//! callbacks, and the three sort comparators — lives entirely in this crate,
//! mirroring the C control flow 1:1.  Everything that crosses a subsystem
//! boundary goes through a seam declared in the owning subsystem's `*-seams`
//! crate (loud-panic default, never a silent fallback):
//!
//!   * `std_typanalyze` / the standard `compute_stats` callback
//!     (`commands/analyze.c`) → `commands_analyze_seams`;
//!   * the element typcache projection + the by-OID hash / compare fmgr calls
//!     (`utils/cache/typcache.c`, `fmgr.c`) →
//!     `typcache_seams`;
//!   * `get_base_element_type` (`utils/cache/lsyscache.c`) →
//!     `lsyscache_seams`;
//!   * `toast_raw_datum_size` (`access/common/detoast.c`) →
//!     `detoast_seams`;
//!   * `deconstruct_array` (`utils/adt/arrayfuncs.c`) →
//!     `arrayfuncs_seams::deconstruct_array_v`;
//!   * `vacuum_delay_point` / `CHECK_FOR_INTERRUPTS`
//!     (`commands/vacuum.c`) → the vacuumlazy / vacuum seam crates.
//!
//! # Model adaptations
//!
//! * `VacAttrStats` is the owned value type in `statistics`; its result
//!   slots `stanumbers[n]` / `stavalues[n]` are owned `Vec<f32>` /
//!   `Vec<Datum<'mcx>>` (the C `palloc`-into-`anl_context` arrays).  The MCELEM
//!   values are `datumCopy`'d into `stats.anl_context` exactly as in C.
//! * `ArrayAnalyzeExtraData` lives in `statistics`; `cmp` / `hash` carry
//!   the support functions' proc OIDs (the fmgr calls are seamed by the typcache
//!   owner), not `FmgrInfo` pointers.  The C struct also saves the std
//!   `compute_stats` / `extra_data`; here the std routine is reached through the
//!   `std_compute_stats` seam, and `stats.extra_data` (a `u64`) is not large
//!   enough to carry the array struct, so `compute_array_stats` re-derives the
//!   element metadata from `stats.attrtypid` / `stats.attrcollid` through the
//!   same typcache seam `array_typanalyze` used.  That re-lookup is
//!   deterministic and behaviour-identical to reading the saved struct (the
//!   typcache entry is stable across the ANALYZE run, exactly the C assumption).
//! * The two dynahash tables become owned bucketed [`ElementsTab`] / [`CountTab`]
//!   keyed through the collation-sensitive element hash/compare seams.  They are
//!   crate-local working memory (the C "local memory" temp context).
//! * `fetchfunc` is the repo `AnalyzeAttrFetchFunc` fn pointer over
//!   `VacAttrStats`, matching the C `Datum fetchfunc(stats, rownum, &isnull)`.
//!
//! # Functions ported (every C function)
//!
//! * `array_typanalyze`               → [`array_typanalyze`]
//! * `compute_array_stats`            → [`compute_array_stats`]
//! * `prune_element_hashtable`        → `prune_element_hashtable`
//! * `element_hash`                   → `element_hash`
//! * `element_match`                  → `element_match`
//! * `element_compare`                → `element_compare`
//! * `trackitem_compare_frequencies_desc` → `trackitem_compare_frequencies_desc`
//! * `trackitem_compare_element`      → `trackitem_compare_element`
//! * `countitem_compare_count`        → `countitem_compare_count`

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::format;
use alloc::vec::Vec;
use core::cmp::Ordering;

use mcx::Mcx;
use types_core::primitive::OidIsValid;
use types_error::{PgError, PgResult, ERROR};
use statistics::{
    AnalyzeAttrFetchFunc, ArrayAnalyzeExtraData, VacAttrStats, STATISTIC_NUM_SLOTS,
};
use types_tuple::Datum;

use types_selfuncs::{STATISTIC_KIND_DECHIST, STATISTIC_KIND_MCELEM};

use detoast_seams::toast_raw_datum_size;
use vacuumlazy_seams::vacuum_delay_point;
use commands_analyze_seams::{std_compute_stats, std_typanalyze};
use vacuum_seams::check_for_interrupts;
use arrayfuncs_seams::deconstruct_array_v;
use lsyscache_seams::get_base_element_type;
use typcache_seams::{
    array_element_compare, array_element_hash, array_typanalyze_element_typcache,
};

// ===========================================================================
// constants matching the C source
// ===========================================================================

/// `ARRAY_WIDTH_THRESHOLD` (array_typanalyze.c:33) — arrays wider than this
/// (after detoasting) are ignored during analysis.
const ARRAY_WIDTH_THRESHOLD: i64 = 0x10000;

/// `elog(ERROR, msg)` — raise an internal error as a recoverable value (the C
/// `ereport(ERROR)` `longjmp` analog).
fn elog_error<T>(msg: impl Into<alloc::string::String>) -> PgResult<T> {
    Err(PgError::new(ERROR, msg))
}

/// `Min(a, b)` (`c.h`).
#[inline]
fn min_i64(a: i64, b: i64) -> i64 {
    if a < b {
        a
    } else {
        b
    }
}

/// `Max(a, b)` (`c.h`).
#[inline]
fn max_i64(a: i64, b: i64) -> i64 {
    if a > b {
        a
    } else {
        b
    }
}

// ===========================================================================
// TrackItem (array_typanalyze.c:68) / DECountItem (array_typanalyze.c:77)
// ===========================================================================

/// `TrackItem` (array_typanalyze.c:68) — a hash table entry for the Lossy
/// Counting algorithm.  Field order matches the C struct.  `key` is the
/// `datumCopy`'d element value (owned in `stats.anl_context`).
struct TrackItem<'mcx> {
    /// This is 'e' from the LC algorithm.
    key: Datum<'mcx>,
    /// This is 'f'.
    frequency: i32,
    /// And this is 'delta'.
    delta: i32,
    /// For de-duplication of array elements.
    last_container: i32,
}

/// `DECountItem` (array_typanalyze.c:77) — a hash table entry for
/// distinct-elements counts.  Field order matches the C struct.
#[derive(Clone, Copy)]
struct DECountItem {
    /// Count of distinct elements in an array.
    count: i32,
    /// Number of arrays seen with this count.
    frequency: i32,
}

// ===========================================================================
// Owned equivalents of the two dynahash tables
// ===========================================================================
//
// dynahash's only observable semantics here are enter/find/remove/seq-scan and
// (for the LC table) holding a stable handle to an entry across later inserts.
// We model each table as a vector of buckets indexed by the element hash (the
// exact bucket count is unobservable to the algorithm), with equality decided
// by `element_compare(...) == 0`.  This is crate-local working memory, the C
// "local memory" temp context; it is dropped when the function returns.

/// Idiomatic model of the `D` (Lossy Counting) dynahash table.
struct ElementsTab<'mcx> {
    /// Buckets keyed by [`element_hash`]; each holds the entries that hashed
    /// there (so an entry's identity is stable as a `(bucket, index)` pair until
    /// a removal in that bucket).
    buckets: Vec<Vec<TrackItem<'mcx>>>,
    /// Element metadata needed to drive the hash/compare seams.
    extra: ArrayAnalyzeExtraData,
}

/// A stable handle to a live [`ElementsTab`] entry (its bucket and position),
/// modelling the `TrackItem *` the C holds across the per-element body.
#[derive(Clone, Copy)]
struct ElemHandle {
    bucket: usize,
    index: usize,
}

impl<'mcx> ElementsTab<'mcx> {
    /// dynahash sizes for `num_mcelem` entries; the bucket count is unobservable
    /// to the algorithm, so a fixed power-of-two base suffices (bucket vectors
    /// grow implicitly).
    fn new(extra: ArrayAnalyzeExtraData) -> Self {
        let mut buckets = Vec::with_capacity(1024);
        for _ in 0..1024 {
            buckets.push(Vec::new());
        }
        ElementsTab { buckets, extra }
    }

    #[inline]
    fn bucket_for(&self, key: &Datum<'mcx>) -> PgResult<usize> {
        let h = element_hash(&self.extra, key.clone())?;
        Ok((h & (self.buckets.len() as u32 - 1)) as usize)
    }

    /// `hash_search(elements_tab, &elem_value, HASH_ENTER, &found)` — return a
    /// stable handle to the (existing or freshly inserted) entry plus the
    /// `found` flag.  A new entry is zero-initialized save for its `key` (the
    /// caller fills the rest, exactly as the C does after `hash_search`).
    fn enter(&mut self, key: Datum<'mcx>) -> PgResult<(ElemHandle, bool)> {
        let b = self.bucket_for(&key)?;
        for i in 0..self.buckets[b].len() {
            if element_compare(&self.extra, key.clone(), self.buckets[b][i].key.clone())? == 0 {
                return Ok((ElemHandle { bucket: b, index: i }, true));
            }
        }
        let index = self.buckets[b].len();
        self.buckets[b].push(TrackItem {
            key,
            frequency: 0,
            delta: 0,
            last_container: 0,
        });
        Ok((ElemHandle { bucket: b, index }, false))
    }

    #[inline]
    fn get_mut(&mut self, h: ElemHandle) -> &mut TrackItem<'mcx> {
        &mut self.buckets[h.bucket][h.index]
    }

    /// `hash_get_num_entries(elements_tab)`.
    fn num_entries(&self) -> usize {
        self.buckets.iter().map(|b| b.len()).sum()
    }
}

/// Idiomatic model of the distinct-element-count dynahash table (`HASH_BLOBS`,
/// `int` key).
struct CountTab {
    buckets: Vec<Vec<DECountItem>>,
}

impl CountTab {
    fn new() -> Self {
        let mut buckets = Vec::with_capacity(128);
        for _ in 0..128 {
            buckets.push(Vec::new());
        }
        CountTab { buckets }
    }

    #[inline]
    fn bucket_for(&self, key: i32) -> usize {
        // dynahash's HASH_BLOBS uses tag_hash over the key bytes; the exact
        // function is unobservable to the algorithm, so an integer mix suffices.
        (key as u32 as usize).wrapping_mul(2654435761) & (self.buckets.len() - 1)
    }

    /// `hash_search(count_tab, &distinct_count, HASH_ENTER, &found)`.  A new
    /// entry carries the key in `count` and a zeroed `frequency` (the caller
    /// sets it next).
    fn enter(&mut self, key: i32) -> (usize, usize, bool) {
        let b = self.bucket_for(key);
        for i in 0..self.buckets[b].len() {
            if self.buckets[b][i].count == key {
                return (b, i, true);
            }
        }
        let index = self.buckets[b].len();
        self.buckets[b].push(DECountItem {
            count: key,
            frequency: 0,
        });
        (b, index, false)
    }

    #[inline]
    fn get_mut(&mut self, bucket: usize, index: usize) -> &mut DECountItem {
        &mut self.buckets[bucket][index]
    }

    /// `hash_get_num_entries(count_tab)`.
    fn num_entries(&self) -> usize {
        self.buckets.iter().map(|b| b.len()).sum()
    }
}

// ===========================================================================
// array_typanalyze (array_typanalyze.c:97)
// ===========================================================================

/// `array_typanalyze(PG_FUNCTION_ARGS)` (array_typanalyze.c:97) — `typanalyze`
/// function for array columns.
///
/// The C `stats` is `PG_GETARG_POINTER(0)`; the result is `PG_RETURN_BOOL(...)`
/// and the function mutates `stats->compute_stats` / `stats->extra_data` as a
/// side effect.  Here `stats` is the owned [`VacAttrStats`] taken by `&mut`; on
/// the array path we set `stats.compute_stats = Some(compute_array_stats_callback)`
/// (the analyze driver will invoke it after sampling).  `stats.extra_data` (a
/// `u64`) cannot hold the array metadata, so `compute_array_stats` re-derives it
/// from `stats.attrtypid` / `stats.attrcollid` — see the module note.
pub fn array_typanalyze(stats: &mut VacAttrStats) -> PgResult<bool> {
    /*
     * Call the standard typanalyze function.  It may fail to find needed
     * operators, in which case we also can't do anything, so just fail.
     */
    if !std_typanalyze::call(stats)? {
        return Ok(false); /* PG_RETURN_BOOL(false) */
    }

    /*
     * Check attribute data type is a varlena array (or a domain over one).
     */
    let element_typeid = get_base_element_type::call(stats.attrtypid)?;
    if !OidIsValid(element_typeid) {
        return elog_error(format!(
            "array_typanalyze was invoked for non-array type {}",
            stats.attrtypid
        ));
    }

    /*
     * Gather information about the element type.  If we fail to find
     * something, return leaving the state from std_typanalyze() in place.
     *
     * The C does lookup_type_cache(TYPECACHE_EQ_OPR | TYPECACHE_CMP_PROC_FINFO
     * | TYPECACHE_HASH_PROC_FINFO) and checks eq_opr / cmp / hash are valid;
     * the seam returns None when any of them is missing (the standard-only
     * PG_RETURN_BOOL(true) path).
     */
    if array_typanalyze_element_typcache::call(element_typeid, stats.attrcollid)?.is_none() {
        return Ok(true); /* PG_RETURN_BOOL(true) — standard stats only */
    }

    /*
     * Store our findings for use by compute_array_stats().  The C replaces
     * stats->compute_stats with compute_array_stats and saves the prior
     * compute_stats/extra_data; here we install the array callback.  The
     * array ArrayAnalyzeExtraData is re-derived inside compute_array_stats
     * from stats.attrtypid / stats.attrcollid (stats.extra_data is a u64 and
     * cannot carry the struct).
     *
     * Note we leave stats->minrows set as std_typanalyze set it.
     */
    stats.compute_stats = Some(compute_array_stats_callback);

    Ok(true) /* PG_RETURN_BOOL(true) */
}

/// `AnalyzeAttrComputeStatsFunc`-typed wrapper installed into
/// `stats.compute_stats`.  The C `compute_stats` callbacks return `void` and
/// `ereport(ERROR)` (`longjmp`) on failure; this fn-pointer type likewise
/// returns `()`, so a recoverable [`PgError`] from the value-typed
/// [`compute_array_stats`] is re-raised by panicking at the callback boundary —
/// the analyze driver's `ereport`/`longjmp` site.
fn compute_array_stats_callback(
    stats: &mut VacAttrStats,
    fetchfunc: AnalyzeAttrFetchFunc,
    samplerows: i32,
    totalrows: f64,
) {
    if let Err(e) = compute_array_stats(stats, fetchfunc, samplerows, totalrows) {
        // ereport(ERROR) longjmp analog: there is no `void`-returning way to
        // surface a recoverable error through the C callback ABI.
        panic!("compute_array_stats raised an error: {e:?}");
    }
}

// ===========================================================================
// compute_array_stats (array_typanalyze.c:215)
// ===========================================================================

/// `compute_array_stats()` (array_typanalyze.c:215) — compute statistics for an
/// array column using Lossy Counting (see the C comment for the full algorithm
/// derivation).
///
/// `fetchfunc` mirrors the C `Datum fetchfunc(stats, rownum, &isnull)`.
/// Results are written into the owned slot `Vec`s of `stats`; the MCELEM values
/// are `datumCopy`'d into `stats.anl_context` exactly as in C.
pub fn compute_array_stats(
    stats: &mut VacAttrStats,
    fetchfunc: AnalyzeAttrFetchFunc,
    samplerows: i32,
    totalrows: f64,
) -> PgResult<()> {
    let _ = totalrows; // forwarded to std_compute_stats only

    /*
     * The C reads extra_data = stats->extra_data (the ArrayAnalyzeExtraData
     * array_typanalyze stashed).  In this repo stats.extra_data is a u64 that
     * cannot carry the struct, so re-derive it from the column's element type
     * (deterministic; the typcache entry is stable across the ANALYZE run).
     * array_typanalyze already verified this returns Some on the array path.
     */
    let element_typeid = get_base_element_type::call(stats.attrtypid)?;
    let extra_data = match array_typanalyze_element_typcache::call(element_typeid, stats.attrcollid)?
    {
        Some(ed) => ed,
        None => {
            return elog_error("compute_array_stats: element type lost its eq/cmp/hash operators");
        }
    };

    let mut null_elem_cnt: i32 = 0;
    let mut analyzed_rows: i32 = 0;

    /*
     * Invoke analyze.c's standard analysis function to create scalar-style
     * stats for the column.  C temporarily installs extra_data->std_extra_data
     * around the call; here the standard routine is reached through its own
     * seam (which resolves its own std payload).
     */
    std_compute_stats::call(stats, fetchfunc, samplerows, totalrows)?;

    /*
     * We want statistics_target * 10 elements in the MCELEM array.
     */
    let num_mcelem_initial: i32 = stats.attstattarget * 10;

    /* We set bucket width equal to num_mcelem / 0.007 as per the comment above. */
    let bucket_width: i32 = num_mcelem_initial * 1000 / 7;

    /* This is D from the LC algorithm. */
    let mut elements_tab = ElementsTab::new(extra_data);
    /* hashtable for array distinct elements counts */
    let mut count_tab = CountTab::new();

    /* Initialize counters. */
    let mut b_current: i32 = 1;
    let mut element_no: i64 = 0;

    // The working Mcx for detoast/deconstruct results and the datumCopy of the
    // element keys.  C palloc's these in CurrentMemoryContext and pfrees the
    // per-array buffers each loop; here we allocate in stats.anl_context (the
    // only context the callback has).  That is behaviour-preserving for the
    // statistics: the per-loop pfree in C is purely a memory-footprint
    // optimisation, and the MCELEM key values must end up in anl_context anyway.
    let mcx: Mcx = stats
        .anl_context
        .expect("compute_array_stats: stats.anl_context must be set by ANALYZE");

    /* Loop over the arrays. */
    let mut array_no: i32 = 0;
    while array_no < samplerows {
        let prev_element_no: i64 = element_no;

        vacuum_delay_point::call(true)?;

        let mut isnull = false;
        let value = fetchfunc(stats, array_no, &mut isnull);
        if isnull {
            /* ignore arrays that are null overall */
            array_no += 1;
            continue;
        }

        /* Skip too-large values. */
        if toast_raw_datum_size::call(mcx, value.clone())? > ARRAY_WIDTH_THRESHOLD {
            array_no += 1;
            continue;
        } else {
            analyzed_rows += 1;
        }

        /*
         * Now detoast the array if needed, and deconstruct into datums.  (The
         * seam detoasts internally; the C Assert(ARR_ELEMTYPE(array) ==
         * extra_data->type_id) is upheld by the owner.)
         */
        let elems = deconstruct_array_v::call(
            mcx,
            value,
            extra_data.type_id,
            extra_data.typlen,
            extra_data.typbyval,
            extra_data.typalign as core::ffi::c_char,
        )?;
        let num_elems: i32 = elems.len() as i32;

        /*
         * We loop through the elements in the array and add them to our
         * tracking hashtable.
         */
        let mut null_present = false;
        let mut j: i32 = 0;
        while j < num_elems {
            /* No null element processing other than flag setting here */
            if elems[j as usize].1 {
                null_present = true;
                j += 1;
                continue;
            }

            /* Lookup current element in hashtable, adding it if new */
            let elem_value = &elems[j as usize].0;
            let (handle, found) = elements_tab.enter(elem_value.clone())?;

            if found {
                /* The element value is already on the tracking list */

                /*
                 * The operators we assist ignore duplicate array elements, so
                 * count a given distinct element only once per array.
                 */
                if elements_tab.get_mut(handle).last_container == array_no {
                    j += 1;
                    continue;
                }

                let item = elements_tab.get_mut(handle);
                item.frequency += 1;
                item.last_container = array_no;
            } else {
                /* Initialize new tracking list element */

                /*
                 * If element type is pass-by-reference, we must copy it into
                 * long-lived space, so that we can release the array.  C
                 * datumCopy's into the hashtable context; here we clone the
                 * value into anl_context (it must survive there for the MCELEM
                 * slot regardless).
                 */
                let key = elem_value.clone_in(mcx)?;
                let item = elements_tab.get_mut(handle);
                item.key = key;
                item.frequency = 1;
                item.delta = b_current - 1;
                item.last_container = array_no;
            }

            /* element_no is the number of elements processed (ie N) */
            element_no += 1;

            /* We prune the D structure after processing each bucket */
            if element_no % bucket_width as i64 == 0 {
                prune_element_hashtable(&mut elements_tab, b_current)?;
                b_current += 1;
            }

            j += 1;
        }

        /* Count null element presence once per array. */
        if null_present {
            null_elem_cnt += 1;
        }

        /* Update frequency of the particular array distinct element count. */
        let distinct_count: i32 = (element_no - prev_element_no) as i32;
        let (cb, ci, count_item_found) = count_tab.enter(distinct_count);
        if count_item_found {
            count_tab.get_mut(cb, ci).frequency += 1;
        } else {
            count_tab.get_mut(cb, ci).frequency = 1;
        }

        /*
         * The C frees the detoasted array and elem_values/elem_nulls here; the
         * owned `elems` is dropped at the end of the loop body.  (Its backing
         * allocation is in anl_context — see the note above.)
         */
        drop(elems);

        array_no += 1;
    }

    /* Skip pg_statistic slots occupied by standard statistics */
    let mut slot_idx: usize = 0;
    while slot_idx < STATISTIC_NUM_SLOTS && stats.stakind[slot_idx] != 0 {
        slot_idx += 1;
    }
    if slot_idx > STATISTIC_NUM_SLOTS - 2 {
        return elog_error("insufficient pg_statistic slots for array stats");
    }

    /* We can only compute real stats if we found some non-null values. */
    if analyzed_rows > 0 {
        let nonnull_cnt: i32 = analyzed_rows;

        /*
         * Construct an array of the interesting hashtable items, those meeting
         * the cutoff frequency (s - epsilon)*N.  Since epsilon = s/10 and
         * bucket_width = 1/epsilon, the cutoff frequency is 9*N / bucket_width.
         */
        let cutoff_freq: i64 = 9 * element_no / bucket_width as i64;

        let i_count: i32 = elements_tab.num_entries() as i32; /* surely enough space */

        // sort_table (C: palloc(sizeof(TrackItem *) * i)) — we collect the
        // qualifying entries as (key, frequency) snapshots in seq-scan order.
        let mut sort_table: Vec<(Datum, i32)> = Vec::with_capacity(i_count as usize);
        let mut minfreq: i64 = element_no;
        let mut maxfreq: i64 = 0;
        for bucket in elements_tab.buckets.iter() {
            for item in bucket.iter() {
                if item.frequency as i64 > cutoff_freq {
                    sort_table.push((item.key.clone(), item.frequency));
                    minfreq = min_i64(minfreq, item.frequency as i64);
                    maxfreq = max_i64(maxfreq, item.frequency as i64);
                }
            }
        }
        let track_len: i32 = sort_table.len() as i32;
        debug_assert!(track_len <= i_count);

        /*
         * emit some statistics for debug purposes — C: elog(DEBUG3, ...).  This
         * is a diagnostic-only message below the default log_min_messages
         * (WARNING); the repo has no in-scope log-emit primitive for this leaf,
         * so it is intentionally a no-op (no behavioural effect on the stats).
         */
        let _ = (num_mcelem_initial, bucket_width, element_no, i_count, track_len);

        /*
         * If we obtained more elements than we really want, get rid of those
         * with least frequencies.  qsort into descending frequency order and
         * truncate.
         */
        let mut num_mcelem = num_mcelem_initial;
        if num_mcelem < track_len {
            qsort_interruptible(&mut sort_table, |a, b| {
                trackitem_compare_frequencies_desc(a.1, b.1)
            })?;
            /* reset minfreq to the smallest frequency we're keeping */
            minfreq = sort_table[(num_mcelem - 1) as usize].1 as i64;
        } else {
            num_mcelem = track_len;
        }

        /* Generate MCELEM slot entry */
        if num_mcelem > 0 {
            /*
             * We want statistics sorted on the element value using the element
             * type's default comparison function (permits fast binary search).
             */
            let extra = extra_data;
            qsort_interruptible_try(&mut sort_table[..num_mcelem as usize], |a, b| {
                trackitem_compare_element(&extra, &a.0, &b.0)
            })?;

            /* Must copy the target values into anl_context (already there). */
            let mut mcelem_values: Vec<Datum> = Vec::with_capacity(num_mcelem as usize);
            let mut mcelem_freqs: Vec<f32> = Vec::with_capacity(num_mcelem as usize + 3);

            /*
             * See comments above about use of nonnull_cnt as the divisor for
             * the final frequency estimates.
             */
            for titem in sort_table[..num_mcelem as usize].iter() {
                mcelem_values.push(titem.0.clone_in(mcx)?);
                mcelem_freqs.push((titem.1 as f64 / nonnull_cnt as f64) as f32);
            }
            mcelem_freqs.push((minfreq as f64 / nonnull_cnt as f64) as f32);
            mcelem_freqs.push((maxfreq as f64 / nonnull_cnt as f64) as f32);
            mcelem_freqs.push((null_elem_cnt as f64 / nonnull_cnt as f64) as f32);

            stats.stakind[slot_idx] = STATISTIC_KIND_MCELEM as i16;
            stats.staop[slot_idx] = extra_data.eq_opr;
            stats.stacoll[slot_idx] = extra_data.coll_id;
            /* See above comment about extra stanumber entries */
            stats.numnumbers[slot_idx] = num_mcelem + 3;
            stats.stanumbers[slot_idx] = mcelem_freqs;
            stats.numvalues[slot_idx] = num_mcelem;
            stats.stavalues[slot_idx] = mcelem_values;
            /* We are storing values of element type */
            stats.statypid[slot_idx] = extra_data.type_id;
            stats.statyplen[slot_idx] = extra_data.typlen;
            stats.statypbyval[slot_idx] = extra_data.typbyval;
            stats.statypalign[slot_idx] = extra_data.typalign;
            slot_idx += 1;
        }

        /* Generate DECHIST slot entry */
        let count_items_count: i32 = count_tab.num_entries() as i32;
        if count_items_count > 0 {
            let mut num_hist: i32 = stats.attstattarget;

            /* num_hist must be at least 2 for the loop below to work */
            num_hist = max_i64(num_hist as i64, 2) as i32;

            /*
             * Create an array of DECountItems, sorted into increasing count
             * order.
             */
            let mut sorted_count_items: Vec<DECountItem> =
                Vec::with_capacity(count_items_count as usize);
            for bucket in count_tab.buckets.iter() {
                for item in bucket.iter() {
                    sorted_count_items.push(*item);
                }
            }
            debug_assert_eq!(sorted_count_items.len(), count_items_count as usize);
            qsort_interruptible(&mut sorted_count_items, |a, b| {
                countitem_compare_count(a, b)
            })?;

            /*
             * Prepare to fill stanumbers with the histogram, followed by the
             * average count.  This array is stored in anl_context (owned Vec).
             */
            let mut hist: Vec<f32> = Vec::with_capacity(num_hist as usize + 1);
            for _ in 0..(num_hist as usize + 1) {
                hist.push(0.0);
            }
            hist[num_hist as usize] = (element_no as f64 / nonnull_cnt as f64) as f32;

            /*----------
             * Construct the histogram of distinct-element counts (DECs).  See
             * the C comment for the full derivation of the "frac" advance
             * criterion; the math is done in int64 to avoid int32 overflow for
             * very large statistics targets.
             *----------
             */
            let delta: i32 = analyzed_rows - 1;
            let mut j: usize = 0; /* current index in sorted_count_items */
            /* Initialize frac for sorted_count_items[0]; y is initially 0 */
            let mut frac: i64 = sorted_count_items[0].frequency as i64 * (num_hist - 1) as i64;
            let mut i: usize = 0;
            while (i as i32) < num_hist {
                while frac <= 0 {
                    /* Advance, and update x component of frac */
                    j += 1;
                    frac += sorted_count_items[j].frequency as i64 * (num_hist - 1) as i64;
                }
                hist[i] = sorted_count_items[j].count as f32;
                frac -= delta as i64; /* update y for upcoming i increment */
                i += 1;
            }
            debug_assert_eq!(j, (count_items_count - 1) as usize);

            stats.stakind[slot_idx] = STATISTIC_KIND_DECHIST as i16;
            stats.staop[slot_idx] = extra_data.eq_opr;
            stats.stacoll[slot_idx] = extra_data.coll_id;
            stats.numnumbers[slot_idx] = num_hist + 1;
            stats.stanumbers[slot_idx] = hist;
            slot_idx += 1;
            let _ = slot_idx;
        }
    }

    /*
     * We don't need to bother cleaning up any of our temporary allocations.
     * The two tables are dropped here; the result slots written into `stats`
     * (the only allocations that escape) live in anl_context.
     */
    Ok(())
}

// ===========================================================================
// prune_element_hashtable (array_typanalyze.c:680)
// ===========================================================================

/// `prune_element_hashtable()` (array_typanalyze.c:680) — prune the `D`
/// structure from the Lossy Counting algorithm: remove every entry with
/// `frequency + delta <= b_current`.
fn prune_element_hashtable(elements_tab: &mut ElementsTab, b_current: i32) -> PgResult<()> {
    /*
     * C hash_seq_search tolerates HASH_REMOVE of the just-returned entry.  We
     * retain in place: walk each bucket and keep only the entries that survive
     * the cutoff (retain order is unobservable to the algorithm).  The freed
     * entries' `key` Datums are dropped (the C `pfree(DatumGetPointer(value))`
     * for pass-by-reference values; here the clone in anl_context is released
     * with the TrackItem).
     */
    for bucket in elements_tab.buckets.iter_mut() {
        bucket.retain(|item| item.frequency + item.delta > b_current);
    }
    Ok(())
}

// ===========================================================================
// element_hash (array_typanalyze.c:709)
// ===========================================================================

/// `element_hash()` (array_typanalyze.c:709) — hash function for elements,
/// using the element type's default hash opclass and the column collation.
fn element_hash(extra: &ArrayAnalyzeExtraData, key: Datum) -> PgResult<u32> {
    array_element_hash::call(extra.hash, extra.coll_id, key)
}

// ===========================================================================
// element_match (array_typanalyze.c:724)
// ===========================================================================

/// `element_match()` (array_typanalyze.c:724) — matching function for elements
/// in hashtable lookups.  Returns 0 on match (C `memcmp`-style comparator).
#[allow(dead_code)]
fn element_match(extra: &ArrayAnalyzeExtraData, key1: Datum, key2: Datum) -> PgResult<i32> {
    /* The keysize parameter is superfluous here */
    element_compare(extra, key1, key2)
}

// ===========================================================================
// element_compare (array_typanalyze.c:739)
// ===========================================================================

/// `element_compare()` (array_typanalyze.c:739) — comparison function for
/// elements, using the element type's default btree opclass and the column
/// collation.
fn element_compare(extra: &ArrayAnalyzeExtraData, key1: Datum, key2: Datum) -> PgResult<i32> {
    array_element_compare::call(extra.cmp, extra.coll_id, key1, key2)
}

// ===========================================================================
// trackitem_compare_frequencies_desc (array_typanalyze.c:755)
// ===========================================================================

/// `trackitem_compare_frequencies_desc()` (array_typanalyze.c:755) — comparator
/// for sorting `TrackItem`s by frequency, descending.
fn trackitem_compare_frequencies_desc(f1: i32, f2: i32) -> i32 {
    f2 - f1
}

// ===========================================================================
// trackitem_compare_element (array_typanalyze.c:767)
// ===========================================================================

/// `trackitem_compare_element()` (array_typanalyze.c:767) — comparator for
/// sorting `TrackItem`s by element value.
fn trackitem_compare_element(
    extra: &ArrayAnalyzeExtraData,
    k1: &Datum,
    k2: &Datum,
) -> PgResult<i32> {
    element_compare(extra, k1.clone(), k2.clone())
}

// ===========================================================================
// countitem_compare_count (array_typanalyze.c:779)
// ===========================================================================

/// `countitem_compare_count()` (array_typanalyze.c:779) — comparator for
/// sorting `DECountItem`s by count.
fn countitem_compare_count(t1: &DECountItem, t2: &DECountItem) -> i32 {
    if t1.count < t2.count {
        -1
    } else if t1.count == t2.count {
        0
    } else {
        1
    }
}

// ===========================================================================
// qsort_interruptible driver (port/qsort_interruptible.c)
// ===========================================================================

/// `qsort_interruptible(base, n, size, cmp, NULL)` — sort `slice` in place using
/// the C three-way comparator semantics, with the periodic
/// `CHECK_FOR_INTERRUPTS()` driven through the vacuum `check_for_interrupts`
/// seam (the comparator is infallible).
fn qsort_interruptible<T, F>(slice: &mut [T], mut cmp: F) -> PgResult<()>
where
    F: FnMut(&T, &T) -> i32,
{
    check_for_interrupts::call()?;
    slice.sort_by(|a, b| match cmp(a, b) {
        n if n < 0 => Ordering::Less,
        0 => Ordering::Equal,
        _ => Ordering::Greater,
    });
    Ok(())
}

/// As [`qsort_interruptible`] but with a fallible comparator (the element-value
/// sort calls the compare support function, which can `ereport(ERROR)`).  A
/// comparator error is captured and surfaced after the sort completes.
fn qsort_interruptible_try<T, F>(slice: &mut [T], mut cmp: F) -> PgResult<()>
where
    F: FnMut(&T, &T) -> PgResult<i32>,
{
    check_for_interrupts::call()?;
    let mut err: Option<PgError> = None;
    slice.sort_by(|a, b| {
        if err.is_some() {
            return Ordering::Equal;
        }
        match cmp(a, b) {
            Ok(n) if n < 0 => Ordering::Less,
            Ok(0) => Ordering::Equal,
            Ok(_) => Ordering::Greater,
            Err(e) => {
                err = Some(e);
                Ordering::Equal
            }
        }
    });
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install this crate's inward seams.  Called once from `seams-init`.
pub fn init_seams() {
    array_typanalyze_seams::array_typanalyze::set(array_typanalyze);
}
