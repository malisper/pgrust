//! Port of `backend/tsearch/ts_typanalyze.c` (PostgreSQL 18.3) — gather
//! statistics from `tsvector` columns.
//!
//! The Lossy-Counting (LC) most-common-lexemes algorithm
//! (`compute_tsvector_stats`), the hashtable pruning (`prune_lexemes_hashtable`),
//! the lexeme hash/compare callbacks, and the two sort comparators live entirely
//! in this crate, mirroring the C control flow 1:1. Everything that crosses a
//! subsystem boundary goes through a seam declared in the owning subsystem's
//! `*-seams` crate (loud-panic default, never a silent fallback):
//!
//!   * the analyze driver invokes `ts_typanalyze` via the
//!     `backend-utils-adt-tsvector-typanalyze-seams` inward seam, keyed by the
//!     column type's `pg_type.typanalyze` OID (`OidFunctionCall1` could not carry
//!     the live `VacAttrStats*` through the by-word fmgr ABI);
//!   * `VARSIZE_ANY` / detoast (`access/common/detoast.c`) →
//!     `detoast_seams`;
//!   * `vacuum_delay_point` / `CHECK_FOR_INTERRUPTS` (`commands/vacuum.c`) → the
//!     vacuumlazy / vacuum seam crates;
//!   * `default_statistics_target` (`utils/misc/guc_tables.c`) → the guc-tables
//!     var slot.
//!
//! # Model adaptations
//!
//! * `VacAttrStats` is the owned value type in `statistics`; its result
//!   slots `stanumbers[0]` / `stavalues[0]` are owned `Vec<f32>` /
//!   `Vec<Datum<'mcx>>` (the C `palloc`-into-`anl_context` arrays). The MCELEM
//!   `text` values are built directly into `stats.anl_context`.
//! * The `tsvector` is detoasted to a `&[u8]` image (the C `DatumGetTSVector`)
//!   and read through the `backend-utils-adt-tsvector-core` access layer
//!   (`tsv_size` / `arrptr` / `lexeme`), which reproduces `STRPTR`/`ARRPTR`/the
//!   `WordEntry` `pos`/`len` macros.
//! * The dynahash LC table becomes an owned bucketed [`LexemesTab`]; entries are
//!   keyed by the (length-first, then byte-for-byte) `LexemeHashKey` exactly as
//!   the C `lexeme_hash` / `lexeme_compare`. It is crate-local working memory
//!   (the C "local memory" temp context); the surviving MCELEM lexemes are
//!   copied into `anl_context`.
//! * `fetchfunc` is the repo `AnalyzeAttrFetchFunc` fn pointer over
//!   `VacAttrStats`, matching the C `Datum fetchfunc(stats, rownum, &isnull)`.
//!
//! # Functions ported (every C function)
//!
//! * `ts_typanalyze`                       → [`ts_typanalyze`]
//! * `compute_tsvector_stats`              → [`compute_tsvector_stats`]
//! * `prune_lexemes_hashtable`             → `prune_lexemes_hashtable`
//! * `lexeme_hash`                         → `lexeme_hash`
//! * `lexeme_match`                        → `lexeme_match`
//! * `lexeme_compare`                      → `lexeme_compare`
//! * `trackitem_compare_frequencies_desc`  → `trackitem_compare_frequencies_desc`
//! * `trackitem_compare_lexemes`           → `trackitem_compare_lexemes`

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::vec::Vec;
use core::cmp::Ordering;

use ::mcx::Mcx;
use ::types_error::PgResult;
use ::statistics::{AnalyzeAttrFetchFunc, VacAttrStats, STATISTIC_NUM_SLOTS};
use ::types_tuple::Datum;

use ::types_selfuncs::STATISTIC_KIND_MCELEM;

use ::detoast_seams::{detoast_attr, pg_varsize_any};
use ::vacuumlazy_seams::vacuum_delay_point;
use ::vacuum_seams::check_for_interrupts;
use ::tsvector_core::access::{arrptr, lexeme, tsv_size};

/// `TEXTOID` (pg_type.dat) — element type of the MCELEM `text` values.
const TEXTOID: types_core::Oid = 25;
/// `TextEqualOperator` (pg_operator.dat: `texteq` `=` operator, OID 98).
const TEXT_EQUAL_OPERATOR: types_core::Oid = 98;
/// `DEFAULT_COLLATION_OID` (pg_collation.dat).
const DEFAULT_COLLATION_OID: types_core::Oid = 100;
/// `VARHDRSZ` — the 4-byte varlena header.
const VARHDRSZ: usize = 4;

// ===========================================================================
// LexemeHashKey / TrackItem (ts_typanalyze.c:26-38)
// ===========================================================================

/// `TrackItem` (ts_typanalyze.c:33) — a hash table entry for the Lossy Counting
/// algorithm. `key` carries the lexeme bytes (the C `LexemeHashKey`'s
/// `lexeme`/`length`); for a surviving entry the bytes are copied into the
/// owned `Vec<u8>` (the C `palloc` + `memcpy` of `item->key.lexeme`).
struct TrackItem {
    /// `key` — the lexeme bytes (not NUL-terminated). 'e' from the LC algorithm.
    key: Vec<u8>,
    /// `frequency` — 'f' from the LC algorithm.
    frequency: i32,
    /// `delta` — 'delta' from the LC algorithm.
    delta: i32,
}

// ===========================================================================
// Owned model of the LC dynahash table
// ===========================================================================
//
// dynahash's only observable semantics here are enter/find/remove/seq-scan and
// holding a stable handle to an entry across later inserts. We model it as a
// vector of buckets indexed by `lexeme_hash`, equality decided by
// `lexeme_compare(...) == 0`. This is crate-local working memory (the C "local
// memory" temp context); it is dropped when the function returns.

/// A stable handle to a live [`LexemesTab`] entry (its bucket and position),
/// modelling the `TrackItem *` the C holds across the per-lexeme body.
#[derive(Clone, Copy)]
struct ItemHandle {
    bucket: usize,
    index: usize,
}

struct LexemesTab {
    buckets: Vec<Vec<TrackItem>>,
}

impl LexemesTab {
    /// dynahash sizes for `num_mcelem` entries; the bucket count is unobservable
    /// to the algorithm, so a fixed power-of-two base suffices.
    fn new() -> Self {
        let mut buckets = Vec::with_capacity(1024);
        for _ in 0..1024 {
            buckets.push(Vec::new());
        }
        LexemesTab { buckets }
    }

    #[inline]
    fn bucket_for(&self, key: &[u8]) -> usize {
        (lexeme_hash(key) & (self.buckets.len() as u32 - 1)) as usize
    }

    /// `hash_search(lexemes_tab, &hash_key, HASH_ENTER, &found)` — return a
    /// stable handle to the (existing or freshly inserted) entry plus the
    /// `found` flag. A new entry is created carrying a *copy* of `key` (the C
    /// `palloc` + `memcpy` of the lexeme into the hashtable context — here the
    /// equivalent owned `Vec<u8>`, which we may release on prune).
    fn enter(&mut self, key: &[u8]) -> (ItemHandle, bool) {
        let b = self.bucket_for(key);
        for i in 0..self.buckets[b].len() {
            if lexeme_compare(&self.buckets[b][i].key, key) == 0 {
                return (ItemHandle { bucket: b, index: i }, true);
            }
        }
        let index = self.buckets[b].len();
        self.buckets[b].push(TrackItem {
            key: key.to_vec(),
            frequency: 0,
            delta: 0,
        });
        (ItemHandle { bucket: b, index }, false)
    }

    #[inline]
    fn get_mut(&mut self, h: ItemHandle) -> &mut TrackItem {
        &mut self.buckets[h.bucket][h.index]
    }

    /// `hash_get_num_entries(lexemes_tab)`.
    fn num_entries(&self) -> usize {
        self.buckets.iter().map(|b| b.len()).sum()
    }
}

// ===========================================================================
// ts_typanalyze (ts_typanalyze.c:58)
// ===========================================================================

/// `ts_typanalyze(PG_FUNCTION_ARGS)` (ts_typanalyze.c:58) — a custom
/// `typanalyze` function for `tsvector` columns.
///
/// The C `stats` is `PG_GETARG_POINTER(0)`; the result is `PG_RETURN_BOOL(true)`
/// and the function mutates `stats->attstattarget` / `stats->compute_stats` /
/// `stats->minrows` as a side effect. Here `stats` is the owned [`VacAttrStats`]
/// taken by `&mut`.
pub fn ts_typanalyze(stats: &mut VacAttrStats) -> PgResult<bool> {
    /* If the attstattarget column is negative, use the default value */
    if stats.attstattarget < 0 {
        stats.attstattarget = default_statistics_target();
    }

    stats.compute_stats = Some(compute_tsvector_stats_callback);
    /* see comment about the choice of minrows in commands/analyze.c */
    stats.minrows = 300 * stats.attstattarget;

    Ok(true) /* PG_RETURN_BOOL(true) */
}

/// `default_statistics_target` (guc_tables.c) — the per-backend default, read
/// from the real GUC slot.
fn default_statistics_target() -> i32 {
    guc_tables::vars::default_statistics_target.read()
}

/// `AnalyzeAttrComputeStatsFunc`-typed wrapper installed into
/// `stats.compute_stats`. The C `compute_stats` callbacks return `void` and
/// `ereport(ERROR)` (`longjmp`) on failure; this fn-pointer type likewise
/// returns `()`, so a recoverable `PgError` from the value-typed
/// [`compute_tsvector_stats`] is re-raised by panicking at the callback
/// boundary — the analyze driver's `ereport`/`longjmp` site.
fn compute_tsvector_stats_callback(
    stats: &mut VacAttrStats,
    fetchfunc: AnalyzeAttrFetchFunc,
    samplerows: i32,
    totalrows: f64,
) {
    if let Err(e) = compute_tsvector_stats(stats, fetchfunc, samplerows, totalrows) {
        // ereport(ERROR) longjmp analog: there is no `void`-returning way to
        // surface a recoverable error through the C callback ABI.
        panic!("compute_tsvector_stats raised an error: {e:?}");
    }
}

// ===========================================================================
// compute_tsvector_stats (ts_typanalyze.c:140)
// ===========================================================================

/// `compute_tsvector_stats()` (ts_typanalyze.c:140) — compute statistics for a
/// `tsvector` column using Lossy Counting (see the C comment for the full
/// algorithm derivation).
///
/// `fetchfunc` mirrors the C `Datum fetchfunc(stats, rownum, &isnull)`. Results
/// are written into the owned slot `Vec`s of `stats`; the MCELEM `text` values
/// are built into `stats.anl_context`.
pub fn compute_tsvector_stats(
    stats: &mut VacAttrStats,
    fetchfunc: AnalyzeAttrFetchFunc,
    samplerows: i32,
    totalrows: f64,
) -> PgResult<()> {
    let _ = totalrows; /* unused by the C body */

    let mut null_cnt: i32 = 0;
    let mut total_width: f64 = 0.0;

    /*
     * We want statistics_target * 10 lexemes in the MCELEM array. This
     * multiplier is pretty arbitrary, but is meant to reflect the fact that the
     * number of individual lexeme values tracked in pg_statistic ought to be
     * more than the number of values for a simple scalar column.
     */
    let mut num_mcelem: i32 = stats.attstattarget * 10;

    /*
     * We set bucket width equal to (num_mcelem + 10) / 0.007 as per the comment
     * above.
     */
    let bucket_width: i32 = (num_mcelem + 10) * 1000 / 7;

    /* This is D from the LC algorithm. */
    let mut lexemes_tab = LexemesTab::new();

    // The working Mcx for detoast results and the final MCELEM text values.
    let mcx: Mcx = stats
        .anl_context
        .expect("compute_tsvector_stats: stats.anl_context must be set by ANALYZE");

    /* Initialize counters. */
    let mut b_current: i32 = 1;
    let mut lexeme_no: i64 = 0;

    /* Loop over the tsvectors. */
    let mut vector_no: i32 = 0;
    while vector_no < samplerows {
        vacuum_delay_point::call(true)?;

        let mut isnull = false;
        let value = fetchfunc(stats, vector_no, &mut isnull);

        /* Check for null/nonnull. */
        if isnull {
            null_cnt += 1;
            vector_no += 1;
            continue;
        }

        /*
         * Add up widths for average-width calculation. Since it's a tsvector,
         * we know it's varlena. As in the regular compute_minimal_stats
         * function, we use the toasted width for this calculation.
         */
        total_width += pg_varsize_any::call(value.as_ref_bytes())? as f64;

        /*
         * Now detoast the tsvector if needed (C: DatumGetTSVector, i.e.
         * PG_DETOAST_DATUM / detoast_attr — which un-packs a short (1-byte
         * header) stored tsvector to the canonical 4-byte-header form). The
         * `_packed` variant must NOT be used here: `tsv_size` / `arrptr` read the
         * size word and WordEntry array at the FIXED post-VARHDRSZ offsets 4/8, so
         * a short-headed image (a small stored tsvector under
         * SHORT_VARLENA_PACKING) would be mis-decoded / panic. With the flag OFF
         * no stored tsvector is short, so this is byte-identical to the prior
         * `_packed` path (behavior-preserving).
         */
        let vector = detoast_attr::call(mcx, value.as_ref_bytes())?;
        let size = tsv_size(&vector);

        /*
         * We loop through the lexemes in the tsvector and add them to our
         * tracking hashtable.
         */
        let mut j: i32 = 0;
        while j < size {
            /*
             * Construct a hash key. The key points into the (detoasted)
             * tsvector value at this point, but if a new entry is created we
             * make a copy of it. lexeme(...) returns the WordEntry's bytes
             * (STRPTR(vector) + curentryptr->pos, curentryptr->len).
             */
            let entry = arrptr(&vector, j as usize);
            let key = lexeme(&vector, size, entry);

            /* Lookup current lexeme in hashtable, adding it if new */
            let (handle, found) = lexemes_tab.enter(key);

            if found {
                /* The lexeme is already on the tracking list */
                lexemes_tab.get_mut(handle).frequency += 1;
            } else {
                /* Initialize new tracking list element (key copied by enter()) */
                let item = lexemes_tab.get_mut(handle);
                item.frequency = 1;
                item.delta = b_current - 1;
            }

            /* lexeme_no is the number of elements processed (ie N) */
            lexeme_no += 1;

            /* We prune the D structure after processing each bucket */
            if lexeme_no % bucket_width as i64 == 0 {
                prune_lexemes_hashtable(&mut lexemes_tab, b_current);
                b_current += 1;
            }

            j += 1;
        }

        /*
         * The C frees the detoasted copy here if it was toasted; the owned
         * `vector` is dropped at the end of the loop body.
         */
        drop(vector);

        vector_no += 1;
    }

    /* We can only compute real stats if we found some non-null values. */
    if null_cnt < samplerows {
        let nonnull_cnt: i32 = samplerows - null_cnt;

        stats.stats_valid = true;
        /* Do the simple null-frac and average width stats */
        stats.stanullfrac = (null_cnt as f64 / samplerows as f64) as f32;
        stats.stawidth = (total_width / nonnull_cnt as f64) as i32;

        /* Assume it's a unique column (see notes above) */
        stats.stadistinct = (-1.0 * (1.0 - stats.stanullfrac as f64)) as f32;

        /*
         * Construct an array of the interesting hashtable items, that is, those
         * meeting the cutoff frequency (s - epsilon)*N. Also identify the
         * minimum and maximum frequencies among these items.
         *
         * Since epsilon = s/10 and bucket_width = 1/epsilon, the cutoff
         * frequency is 9*N / bucket_width.
         */
        let cutoff_freq: i64 = 9 * lexeme_no / bucket_width as i64;

        let i_count: i32 = lexemes_tab.num_entries() as i32; /* surely enough space */

        // sort_table (C: palloc(sizeof(TrackItem *) * i)) — collect qualifying
        // entries as (key, frequency) snapshots in seq-scan order.
        let mut sort_table: Vec<(Vec<u8>, i32)> = Vec::with_capacity(i_count as usize);
        let mut minfreq: i64 = lexeme_no;
        let mut maxfreq: i64 = 0;
        for bucket in lexemes_tab.buckets.iter() {
            for item in bucket.iter() {
                if item.frequency as i64 > cutoff_freq {
                    sort_table.push((item.key.clone(), item.frequency));
                    minfreq = if minfreq < item.frequency as i64 {
                        minfreq
                    } else {
                        item.frequency as i64
                    };
                    maxfreq = if maxfreq > item.frequency as i64 {
                        maxfreq
                    } else {
                        item.frequency as i64
                    };
                }
            }
        }
        let track_len: i32 = sort_table.len() as i32;
        debug_assert!(track_len <= i_count);

        /*
         * emit some statistics for debug purposes — C: elog(DEBUG3, ...). A
         * diagnostic-only message below the default log_min_messages; no
         * behavioural effect on the stats, so it is intentionally a no-op.
         */
        let _ = (num_mcelem, bucket_width, lexeme_no, i_count, track_len);

        /*
         * If we obtained more lexemes than we really want, get rid of those with
         * least frequencies. qsort the array into descending frequency order and
         * truncate.
         */
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
             * We want to store statistics sorted on the lexeme value using first
             * length, then byte-for-byte comparison. (See the C comment about
             * ts_selfuncs.c binary search.)
             */
            qsort_interruptible(&mut sort_table[..num_mcelem as usize], |a, b| {
                trackitem_compare_lexemes(&a.0, &b.0)
            })?;

            /* Must copy the target values into anl_context */
            let mut mcelem_values: Vec<Datum> = Vec::with_capacity(num_mcelem as usize);
            /* two extra cells for min/max freq (no null cell for tsvector) */
            let mut mcelem_freqs: Vec<f32> = Vec::with_capacity(num_mcelem as usize + 2);

            /*
             * See comments above about use of nonnull_cnt as the divisor for the
             * final frequency estimates.
             */
            for titem in sort_table[..num_mcelem as usize].iter() {
                mcelem_values.push(cstring_to_text_with_len(mcx, &titem.0)?);
                mcelem_freqs.push((titem.1 as f64 / nonnull_cnt as f64) as f32);
            }
            mcelem_freqs.push((minfreq as f64 / nonnull_cnt as f64) as f32);
            mcelem_freqs.push((maxfreq as f64 / nonnull_cnt as f64) as f32);

            stats.stakind[0] = STATISTIC_KIND_MCELEM as i16;
            stats.staop[0] = TEXT_EQUAL_OPERATOR;
            stats.stacoll[0] = DEFAULT_COLLATION_OID;
            /* See above comment about two extra frequency fields */
            stats.numnumbers[0] = num_mcelem + 2;
            stats.stanumbers[0] = mcelem_freqs;
            stats.numvalues[0] = num_mcelem;
            stats.stavalues[0] = mcelem_values;
            /* We are storing text values */
            stats.statypid[0] = TEXTOID;
            stats.statyplen[0] = -1; /* typlen, -1 for varlena */
            stats.statypbyval[0] = false;
            stats.statypalign[0] = b'i' as i8;
        }
    } else {
        /* We found only nulls; assume the column is entirely null */
        stats.stats_valid = true;
        stats.stanullfrac = 1.0;
        stats.stawidth = 0; /* "unknown" */
        stats.stadistinct = 0.0; /* "unknown" */
    }

    let _ = STATISTIC_NUM_SLOTS; /* tsvector only ever fills slot 0 */

    /*
     * We don't need to bother cleaning up any of our temporary allocations.
     * The hashtable is dropped here; the MCELEM values written into `stats`
     * live in anl_context.
     */
    Ok(())
}

/// `cstring_to_text_with_len(s, len)` (varlena.c) — build a `text` varlena
/// (`VARHDRSZ + len` bytes) Datum whose payload is the lexeme bytes, allocated
/// in `mcx`. Mirrors the C `palloc(len + VARHDRSZ); SET_VARSIZE; memcpy`.
fn cstring_to_text_with_len<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<Datum<'mcx>> {
    let len = s.len();
    let total = VARHDRSZ + len;
    let mut image: Vec<u8> = alloc::vec![0u8; total];
    /* SET_VARSIZE(item, total): (total << 2) on little-endian */
    let header: u32 = if cfg!(target_endian = "big") {
        total as u32
    } else {
        (total as u32) << 2
    };
    image[0..4].copy_from_slice(&header.to_ne_bytes());
    image[VARHDRSZ..total].copy_from_slice(s);
    Datum::from_byref_bytes_in(mcx, &image)
}

// ===========================================================================
// prune_lexemes_hashtable (ts_typanalyze.c:452)
// ===========================================================================

/// `prune_lexemes_hashtable()` (ts_typanalyze.c:452) — prune the `D` structure
/// from the Lossy Counting algorithm: remove every entry with
/// `frequency + delta <= b_current`.
fn prune_lexemes_hashtable(lexemes_tab: &mut LexemesTab, b_current: i32) {
    /*
     * C hash_seq_search tolerates HASH_REMOVE of the just-returned entry. We
     * retain in place: walk each bucket and keep only the entries that survive
     * the cutoff (retain order is unobservable to the algorithm). The freed
     * entries' `key` bytes are dropped (the C `pfree(lexeme)`).
     */
    for bucket in lexemes_tab.buckets.iter_mut() {
        bucket.retain(|item| item.frequency + item.delta > b_current);
    }
}

// ===========================================================================
// lexeme_hash (ts_typanalyze.c:477)
// ===========================================================================

/// `lexeme_hash()` (ts_typanalyze.c:477) — hash function for lexemes. They are
/// strings, but not NUL-terminated, so we hash exactly `length` bytes with
/// `hash_any` (the `common-hashfn` `hash_bytes`).
fn lexeme_hash(key: &[u8]) -> u32 {
    hashfn::hash_bytes(key)
}

// ===========================================================================
// lexeme_match (ts_typanalyze.c:489)
// ===========================================================================

/// `lexeme_match()` (ts_typanalyze.c:489) — matching function for lexemes in
/// hashtable lookups. Returns 0 on match.
#[allow(dead_code)]
fn lexeme_match(key1: &[u8], key2: &[u8]) -> i32 {
    /* The keysize parameter is superfluous, the keys store their lengths */
    lexeme_compare(key1, key2)
}

// ===========================================================================
// lexeme_compare (ts_typanalyze.c:499)
// ===========================================================================

/// `lexeme_compare()` (ts_typanalyze.c:499) — comparison function for lexemes:
/// first by length, then byte-for-byte.
fn lexeme_compare(d1: &[u8], d2: &[u8]) -> i32 {
    /* First, compare by length */
    if d1.len() > d2.len() {
        return 1;
    } else if d1.len() < d2.len() {
        return -1;
    }
    /* Lengths are equal, do a byte-by-byte comparison (strncmp) */
    match d1.cmp(d2) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

// ===========================================================================
// trackitem_compare_frequencies_desc (ts_typanalyze.c:517)
// ===========================================================================

/// `trackitem_compare_frequencies_desc()` (ts_typanalyze.c:517) — comparator
/// for sorting `TrackItem`s by frequency, descending.
fn trackitem_compare_frequencies_desc(f1: i32, f2: i32) -> i32 {
    f2 - f1
}

// ===========================================================================
// trackitem_compare_lexemes (ts_typanalyze.c:529)
// ===========================================================================

/// `trackitem_compare_lexemes()` (ts_typanalyze.c:529) — comparator for sorting
/// `TrackItem`s by lexeme value.
fn trackitem_compare_lexemes(k1: &[u8], k2: &[u8]) -> i32 {
    lexeme_compare(k1, k2)
}

// ===========================================================================
// qsort_interruptible driver (port/qsort_interruptible.c)
// ===========================================================================

/// `qsort_interruptible(base, n, size, cmp, NULL)` — sort `slice` in place using
/// the C three-way comparator semantics, with the periodic
/// `CHECK_FOR_INTERRUPTS()` driven through the vacuum `check_for_interrupts`
/// seam (the comparators here are infallible).
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

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install this crate's inward seams. Called once from `seams-init`.
pub fn init_seams() {
    tsvector_typanalyze_seams::ts_typanalyze::set(ts_typanalyze);
}
