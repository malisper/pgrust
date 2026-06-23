//! Port of PostgreSQL 18.3 `src/backend/utils/adt/rangetypes_typanalyze.c` —
//! gather statistics from range and multirange columns.
//!
//! For a range type column, histograms of lower and upper bounds, and the
//! fraction of NULL and empty ranges are collected. Both histograms have the
//! same length and are combined into a single array of ranges, identical in
//! shape to what `std_typanalyze` would collect.
//!
//! Functions ported (every C function):
//!   * `range_typanalyze`        -> [`range_typanalyze`]
//!   * `multirange_typanalyze`   -> [`multirange_typanalyze`]
//!   * `float8_qsort_cmp`        -> [`float8_qsort_cmp`]
//!   * `range_bound_qsort_cmp`   -> [`range_bound_qsort_cmp`] (driven via
//!     [`compute_range_stats`]'s sort)
//!   * `compute_range_stats`     -> [`compute_range_stats`]
//!
//! # The `void *extra_data` faithful model
//!
//! The C `range_typanalyze` stashes the range `TypeCacheEntry *` (and
//! `multirange_typanalyze` the multirange entry) into `stats->extra_data`
//! (`void *`), and `compute_range_stats` reads it back, branching on
//! `typcache->typtype == TYPTYPE_MULTIRANGE` to decide whether the column is a
//! multirange. The repo's [`VacAttrStats::extra_data`] is a `u64` owner-token
//! mirroring the C `void *`, and the repo's value-typed [`TypeCacheEntry`] does
//! not carry `typtype`. So this crate threads the analyze payload through a
//! `thread_local!` side table keyed by the `u64` token (a faithful model of the
//! `void *` round-trip): `*_typanalyze` inserts a [`RangeAnalyzeExtraData`] and
//! writes its key into `stats.extra_data`; [`compute_range_stats`] reads it back
//! by that key. This is the one inherent `void*` indirection in the C, not an
//! introduced opacity.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

extern crate alloc;
// The `void *extra_data` side table (seam.rs) uses `std::thread_local!`; std is
// linkable in this workspace (the seam macro itself uses `std::sync::OnceLock`).
extern crate std;

use core::cmp::Ordering;

use mcx::{Mcx, PgVec};
use ::cache::typcache::TypeCacheEntry;
use ::types_core::primitive::{InvalidOid, Oid};
use ::types_error::PgResult;
use types_rangetypes::{MultirangeTypeP, RangeBound, RangeTypeP};
use types_selfuncs::{STATISTIC_KIND_BOUNDS_HISTOGRAM, STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM};
use ::statistics::VacAttrStats;
use ::types_tuple::Datum;

// The analyze working values (RangeBound.val) and the detoast/(de)serialization
// seams speak the bare-word transitional `Datum` (a varlena *pointer* word);
// the stored statistic slots (`VacAttrStats.stavalues`) speak the canonical
// `::types_tuple::Datum<'mcx>` byte-lane enum. The two only meet at the two
// bridges in this file (`bridge_value_ptr` and the bounds-histogram serialize).
use ::datum::datum::Datum as WordDatum;

use vacuum_seams as vacuum_seams;
use multirangetypes_seams as mr_seams;
use rangetypes_seams as range_seams;
use lsyscache_seams as lsyscache_seams;

mod seam;

pub use seam::{init_seams, RangeAnalyzeExtraData};

// ---------------------------------------------------------------------------
// Constants matching the C source / referenced headers.
// ---------------------------------------------------------------------------

/// `TYPTYPE_RANGE` (`catalog/pg_type.h`).
const TYPTYPE_RANGE: i8 = b'r' as i8;
/// `TYPTYPE_MULTIRANGE` (`catalog/pg_type.h`).
const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;

/// `FLOAT8OID` (`catalog/pg_type.dat`, oid 701).
const FLOAT8OID: Oid = 701;
/// `FLOAT8PASSBYVAL` (`pg_config.h` on a `USE_FLOAT8_BYVAL`/LP64 build).
const FLOAT8PASSBYVAL: bool = true;
/// `Float8LessOperator` (`catalog/pg_operator.dat`, oid 672) — the `float8`
/// `<` operator, stored as the `staop` of the length-histogram slot.
const Float8LessOperator: Oid = 672;

/// `OidIsValid(oid)` (`postgres_ext.h`).
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// `get_float8_infinity()` (`utils/float.h` / float.c): the IEEE-754 +Inf used
/// as the length of any infinite range.
#[inline]
fn get_float8_infinity() -> f64 {
    f64::INFINITY
}

// ===========================================================================
// range_typanalyze (rangetypes_typanalyze.c:45)
// ===========================================================================

/// `range_typanalyze(PG_FUNCTION_ARGS)` (rangetypes_typanalyze.c:45) —
/// typanalyze function for range columns.
///
/// `stats` is the C `(VacAttrStats *) PG_GETARG_POINTER(0)`. Returns
/// `Ok(true)` (the C `PG_RETURN_BOOL(true)`).
pub fn range_typanalyze(stats: &mut VacAttrStats<'_>) -> PgResult<bool> {
    /* Get information about range type; note column might be a domain */
    let base = lsyscache_seams::get_base_type::call(stats.attrtypid)?;
    let typcache = range_seams::range_get_typcache::call(base)?;

    typanalyze_common(
        stats,
        RangeAnalyzeExtraData {
            typcache,
            is_multirange: false,
            mltrng_type_oid: InvalidOid,
        },
    )
}

// ===========================================================================
// multirange_typanalyze (rangetypes_typanalyze.c:71)
// ===========================================================================

/// `multirange_typanalyze(PG_FUNCTION_ARGS)` (rangetypes_typanalyze.c:71) —
/// typanalyze function for multirange columns. We do the same analysis as for
/// ranges, but on the smallest range that completely includes the multirange.
pub fn multirange_typanalyze(stats: &mut VacAttrStats<'_>) -> PgResult<bool> {
    /* Get information about multirange type; note column might be a domain */
    let base = lsyscache_seams::get_base_type::call(stats.attrtypid)?;
    let mltrng_typcache = mr_seams::multirange_get_typcache::call(base)?;
    let mltrng_type_oid = mltrng_typcache.type_id;

    /*
     * compute_range_stats does `typcache = typcache->rngtype` for a multirange.
     * Carry the inner range entry directly (the C `typcache->rngtype`) so the
     * (de)serialization / subdiff seams operate on the range entry, exactly as
     * the C does after the `typtype == TYPTYPE_MULTIRANGE` branch.
     */
    let rng = mltrng_typcache
        .rngtype
        .as_deref()
        .cloned()
        .expect("multirange TypeCacheEntry must carry its rngtype sub-entry");

    typanalyze_common(
        stats,
        RangeAnalyzeExtraData {
            typcache: rng,
            is_multirange: true,
            mltrng_type_oid,
        },
    )
}

/// The body shared by `range_typanalyze` and `multirange_typanalyze` after the
/// typcache lookup (identical in both C functions):
///
/// ```c
/// if (stats->attstattarget < 0)
///     stats->attstattarget = default_statistics_target;
/// stats->compute_stats = compute_range_stats;
/// stats->extra_data = typcache;
/// stats->minrows = 300 * stats->attstattarget;
/// PG_RETURN_BOOL(true);
/// ```
fn typanalyze_common(stats: &mut VacAttrStats<'_>, extra: RangeAnalyzeExtraData) -> PgResult<bool> {
    if stats.attstattarget < 0 {
        // `default_statistics_target` GUC (guc_tables.c).
        stats.attstattarget =
            guc_tables::vars::default_statistics_target.read();
    }

    // `stats->compute_stats = compute_range_stats;`
    stats.compute_stats = Some(compute_range_stats);
    // `stats->extra_data = typcache;` — round-trip the analyze payload through
    // the `void *`-faithful side table and store its token.
    stats.extra_data = seam::extra_data_put(extra);
    /* same as in std_typanalyze */
    stats.minrows = 300 * stats.attstattarget;

    Ok(true) // PG_RETURN_BOOL(true)
}

// ===========================================================================
// float8_qsort_cmp (rangetypes_typanalyze.c:94)
// ===========================================================================

/// `float8_qsort_cmp(a1, a2, arg)` (rangetypes_typanalyze.c:94) — comparison
/// function for sorting float8s, used for range lengths.
#[inline]
pub fn float8_qsort_cmp(f1: f64, f2: f64) -> i32 {
    if f1 < f2 {
        -1
    } else if f1 == f2 {
        0
    } else {
        1
    }
}

// ===========================================================================
// range_bound_qsort_cmp (rangetypes_typanalyze.c:106)
// ===========================================================================

/// `range_bound_qsort_cmp(a1, a2, arg)` (rangetypes_typanalyze.c:106) —
/// comparison function for sorting `RangeBound`s; `arg` is the
/// `TypeCacheEntry`. Just calls `range_cmp_bounds(typcache, b1, b2)`.
pub fn range_bound_qsort_cmp(
    typcache: &TypeCacheEntry,
    b1: &RangeBound,
    b2: &RangeBound,
) -> PgResult<i32> {
    range_seams::range_cmp_bounds::call(typcache, b1, b2)
}

/// Map a C three-way comparison result (`< 0` / `0` / `> 0`) to [`Ordering`].
#[inline]
fn int_ordering(c: i32) -> Ordering {
    c.cmp(&0)
}

// ===========================================================================
// compute_range_stats (rangetypes_typanalyze.c:124)
// ===========================================================================

/// `compute_range_stats(stats, fetchfunc, samplerows, totalrows)`
/// (rangetypes_typanalyze.c:124) — compute statistics for a range column.
/// Control flow, loop bounds, branch order, and slot bookkeeping are
/// transcribed 1:1 from the C.
///
/// The signature matches [`::statistics::AnalyzeAttrComputeStatsFunc`] so
/// this is the function pointer the typanalyze routine installs into
/// `stats.compute_stats`. The C `stats->extra_data` is read back here from the
/// `void *`-faithful side table; on a missing token the call panics loudly
/// (the analyze driver must have run `*_typanalyze` first).
pub fn compute_range_stats<'mcx>(
    stats: &mut VacAttrStats<'mcx>,
    fetchfunc: ::statistics::AnalyzeAttrFetchFunc,
    samplerows: i32,
    _totalrows: f64,
) {
    // An ereport(ERROR) inside the seams is surfaced here as a panic: the C
    // `compute_stats` callback returns void and an ERROR longjmps out of it;
    // in the owned model the panic is the analog (the analyze driver's catch
    // frame re-raises it, mirroring the C unwind).
    if let Err(e) = compute_range_stats_inner(stats, fetchfunc, samplerows) {
        panic!("compute_range_stats: {}", e.message());
    }
}

fn compute_range_stats_inner<'mcx>(
    stats: &mut VacAttrStats<'mcx>,
    fetchfunc: ::statistics::AnalyzeAttrFetchFunc,
    samplerows: i32,
) -> PgResult<()> {
    /* TypeCacheEntry *typcache = (TypeCacheEntry *) stats->extra_data; */
    let extra = seam::extra_data_get(stats.extra_data);
    // The C does:
    //   if (typcache->typtype == TYPTYPE_MULTIRANGE) {
    //       mltrng_typcache = typcache; typcache = typcache->rngtype;
    //   } else
    //       Assert(typcache->typtype == TYPTYPE_RANGE);
    // We carry the (already-unwrapped) range entry plus the `is_multirange`
    // discriminator in the side-table payload; assert the discriminator the
    // same way the C asserts `typtype`.
    let typcache = &extra.typcache;
    let is_multirange = extra.is_multirange;
    debug_assert!(
        extra.discriminant_typtype() == TYPTYPE_MULTIRANGE
            || extra.discriminant_typtype() == TYPTYPE_RANGE
    );
    let _ = TYPTYPE_RANGE; // referenced via discriminant_typtype()

    let has_subdiff = OidIsValid(typcache.rng_subdiff_finfo.fn_oid);

    let mut null_cnt: i32 = 0;
    let mut non_null_cnt: i32 = 0;
    let mut non_empty_cnt: i32 = 0;
    let mut empty_cnt: i32 = 0;
    let num_bins: i32 = stats.attstattarget;
    let mut total_width: f64 = 0.0;

    // The C palloc's three `samplerows`-sized arrays and writes only the first
    // `non_empty_cnt` slots (`lowers`/`uppers`/`lengths`). We allocate them in
    // the analyze working context and push as we go (capacity reserved). Bound
    // working storage lives in `anl_context` (long-lived for the whole call),
    // matching where the C's palloc'd arrays live.
    let mcx = stats
        .anl_context
        .expect("VacAttrStats.anl_context must be set before compute_stats");
    let cap = samplerows.max(0) as usize;
    let mut lowers: PgVec<'mcx, RangeBound> = ::mcx::vec_with_capacity_in(mcx, cap)?;
    let mut uppers: PgVec<'mcx, RangeBound> = ::mcx::vec_with_capacity_in(mcx, cap)?;
    let mut lengths: PgVec<'mcx, f64> = ::mcx::vec_with_capacity_in(mcx, cap)?;

    // The accumulated `RangeBound.val`s are *pointers* into the fetched sample
    // value's bytes (for a by-reference subtype like `numeric`: the deserialized
    // range buffer, whose lower/upper element images live inside the `value`
    // Datum returned by `fetchfunc`). In C those bytes are part of the sample
    // tuple, which persists in `anl_context` for the whole function. In the owned
    // model the fetched `value` owns its `Datum::ByRef` buffer and would free it
    // at the end of each loop iteration — leaving every stored bound dangling
    // (all bounds collapse onto the last-freed slot, then read as garbage when
    // the histogram is serialized). Hold every non-empty row's `value` alive
    // here, in `anl_context`, so the bound pointers stay valid until the
    // histograms are built — the faithful analog of C keeping the sample tuples.
    let mut keep_alive: alloc::vec::Vec<Datum<'mcx>> = alloc::vec::Vec::new();
    keep_alive
        .try_reserve(cap)
        .map_err(|_| mcx.oom(cap))?;

    /* Loop over the sample ranges. */
    let mut range_no = 0;
    while range_no < samplerows {
        // vacuum_delay_point(true) — the repo's seam models the no-argument
        // form (is_analyze flag not carried); the interrupt/delay check is the
        // same.
        vacuum_seams::vacuum_delay_point::call()?;

        let mut isnull = false;
        let value = fetchfunc(stats, range_no, &mut isnull);
        if isnull {
            /* range is null, just count that */
            null_cnt += 1;
            range_no += 1;
            continue;
        }

        /*
         * XXX: should we ignore wide values, like std_typanalyze does, to
         * avoid bloating the statistics table?
         *
         * total_width += VARSIZE_ANY(DatumGetPointer(value));
         * The fetched range/multirange value is a by-reference (varlena)
         * Datum; its detoasted image bytes (header included) are VARSIZE_ANY.
         */
        total_width += value.as_ref_bytes().len() as f64;

        // Bridge the canonical by-reference value into the bare-word `Datum`
        // (a varlena pointer) the detoast/deserialize seams consume.
        let value_word = bridge_value_ptr(&value);

        let lower: RangeBound;
        let upper: RangeBound;
        let empty: bool;

        /* Get range and deserialize it for further analysis. */
        if is_multirange {
            /* Treat multiranges like a big range without gaps. */
            let multirange: MultirangeTypeP<'mcx> =
                mr_seams::datum_get_multirange_type_p::call(mcx, value_word)?;
            // MultirangeIsEmpty(multirange) == rangeCount == 0
            if multirange.range_count() != 0 {
                // multirange_get_bounds(typcache, multirange, 0, &lower, &tmp);
                let (lo, _tmp0) = mr_seams::multirange_get_bounds::call(typcache, multirange, 0)?;
                // multirange_get_bounds(typcache, multirange,
                //                       multirange->rangeCount - 1, &tmp, &upper);
                let (_tmp1, up) = mr_seams::multirange_get_bounds::call(
                    typcache,
                    multirange,
                    multirange.range_count() - 1,
                )?;
                lower = lo;
                upper = up;
                empty = false;
            } else {
                // empty = true; lower/upper are unread on the empty path.
                lower = RangeBound::default();
                upper = RangeBound::default();
                empty = true;
            }
        } else {
            let range: RangeTypeP<'mcx> =
                range_seams::datum_get_range_type_p::call(mcx, value_word)?;
            let (lo, up, e) = range_seams::range_deserialize::call(typcache, range)?;
            lower = lo;
            upper = up;
            empty = e;
        }

        if !empty {
            /* Remember bounds and length for further usage in histograms */
            lowers.push(lower);
            uppers.push(upper);
            // Keep the fetched value's bytes alive for the whole function: the
            // bounds just pushed point into them (see `keep_alive` above). For a
            // by-value subtype the bound `val` is a self-contained word, so this
            // is harmless; for a by-reference subtype it is load-bearing.
            keep_alive.push(value);

            let length: f64 = if lower.infinite || upper.infinite {
                /* Length of any kind of an infinite range is infinite */
                get_float8_infinity()
            } else if has_subdiff {
                /*
                 * For an ordinary range, use subdiff function between upper and
                 * lower bound values.
                 *
                 * length = DatumGetFloat8(FunctionCall2Coll(
                 *     &typcache->rng_subdiff_finfo, typcache->rng_collation,
                 *     upper.val, lower.val));
                 * (range_subdiff calls upper, lower in that order.)
                 */
                range_seams::range_subdiff::call(typcache, upper.val, lower.val)?
            } else {
                /* Use default value of 1.0 if no subdiff is available. */
                1.0
            };
            lengths.push(length);

            non_empty_cnt += 1;
        } else {
            empty_cnt += 1;
        }

        non_null_cnt += 1;
        range_no += 1;
    }

    let mut slot_idx = 0usize;

    /* We can only compute real stats if we found some non-null values. */
    if non_null_cnt > 0 {
        stats.stats_valid = true;
        /* Do the simple null-frac and width stats */
        stats.stanullfrac = (null_cnt as f64 / samplerows as f64) as f32;
        stats.stawidth = (total_width / non_null_cnt as f64) as i32;

        /* Estimate that non-null values are unique */
        stats.stadistinct = (-1.0 * (1.0 - stats.stanullfrac as f64)) as f32;

        /*
         * old_cxt = MemoryContextSwitchTo(stats->anl_context);
         * The result arrays must live in `anl_context`; the owned `Vec`s below
         * are built directly in `mcx` (== `stats.anl_context`), so the C
         * context switch is implicit here.
         */

        /*
         * Generate a bounds histogram slot entry if there are at least two
         * values.
         */
        if non_empty_cnt >= 2 {
            /* Sort bound values */
            sort_range_bounds(typcache, &mut lowers)?;
            sort_range_bounds(typcache, &mut uppers)?;

            let mut num_hist = non_empty_cnt;
            if num_hist > num_bins {
                num_hist = num_bins + 1;
            }

            // bound_hist_values = palloc(num_hist * sizeof(Datum));
            // The stored slot (`stats.stavalues`) is a plain owned `Vec`
            // charged to the VacAttrStats (the C anl_context); build it as such.
            let mut bound_hist_values: alloc::vec::Vec<Datum<'mcx>> = alloc::vec::Vec::new();
            bound_hist_values
                .try_reserve(num_hist as usize)
                .map_err(|_| mcx.oom(num_hist as usize))?;

            /*
             * The object of this loop is to construct ranges from first and
             * last entries in lowers[] and uppers[] along with evenly-spaced
             * values in between. So the i'th value is a range of lowers[(i *
             * (nvals - 1)) / (num_hist - 1)] and uppers[(i * (nvals - 1)) /
             * (num_hist - 1)]. But computing that subscript directly risks
             * integer overflow when the stats target is more than a couple
             * thousand.  Instead we add (nvals - 1) / (num_hist - 1) to pos at
             * each step, tracking the integral and fractional parts of the sum
             * separately.
             */
            let delta = (non_empty_cnt - 1) / (num_hist - 1);
            let deltafrac = (non_empty_cnt - 1) % (num_hist - 1);
            let mut pos = 0i32;
            let mut posfrac = 0i32;

            let mut i = 0;
            while i < num_hist {
                // bound_hist_values[i] = PointerGetDatum(range_serialize(
                //     typcache, &lowers[pos], &uppers[pos], false, NULL));
                let serialized = range_seams::range_serialize::call(
                    mcx,
                    typcache,
                    &lowers[pos as usize],
                    &uppers[pos as usize],
                    false,
                )?;
                bound_hist_values.push(range_type_p_to_datum(mcx, serialized)?);

                pos += delta;
                posfrac += deltafrac;
                if posfrac >= (num_hist - 1) {
                    /* fractional part exceeds 1, carry to integer part */
                    pos += 1;
                    posfrac -= num_hist - 1;
                }
                i += 1;
            }

            stats.stakind[slot_idx] = STATISTIC_KIND_BOUNDS_HISTOGRAM as i16;
            stats.numvalues[slot_idx] = num_hist;
            stats.stavalues[slot_idx] = bound_hist_values;

            /* Store ranges even if we're analyzing a multirange column */
            stats.statypid[slot_idx] = typcache.type_id;
            stats.statyplen[slot_idx] = typcache.typlen;
            stats.statypbyval[slot_idx] = typcache.typbyval;
            stats.statypalign[slot_idx] = typcache.typalign;

            slot_idx += 1;
        }

        /*
         * Generate a length histogram slot entry if there are at least two
         * values.
         */
        let length_hist_values: alloc::vec::Vec<Datum<'mcx>>;
        let num_hist_len: i32;
        if non_empty_cnt >= 2 {
            /*
             * Ascending sort of range lengths for further filling of histogram.
             * qsort_interruptible(lengths, ..., float8_qsort_cmp, NULL).
             */
            // The interrupt check the qsort_interruptible driver performs:
            vacuum_seams::vacuum_delay_point::call()?;
            lengths.sort_by(|a, b| int_ordering(float8_qsort_cmp(*a, *b)));

            let mut num_hist = non_empty_cnt;
            if num_hist > num_bins {
                num_hist = num_bins + 1;
            }

            // length_hist_values = palloc(num_hist * sizeof(Datum));
            let mut vals: alloc::vec::Vec<Datum<'mcx>> = alloc::vec::Vec::new();
            vals.try_reserve(num_hist as usize)
                .map_err(|_| mcx.oom(num_hist as usize))?;

            /*
             * The object of this loop is to copy the first and last lengths[]
             * entries along with evenly-spaced values in between. So the i'th
             * value is lengths[(i * (nvals - 1)) / (num_hist - 1)]. But
             * computing that subscript directly risks integer overflow when the
             * stats target is more than a couple thousand.  Instead we add
             * (nvals - 1) / (num_hist - 1) to pos at each step, tracking the
             * integral and fractional parts of the sum separately.
             */
            let delta = (non_empty_cnt - 1) / (num_hist - 1);
            let deltafrac = (non_empty_cnt - 1) % (num_hist - 1);
            let mut pos = 0i32;
            let mut posfrac = 0i32;

            let mut i = 0;
            while i < num_hist {
                /* length_hist_values[i] = Float8GetDatum(lengths[pos]); */
                vals.push(Datum::from_f64(lengths[pos as usize]));
                pos += delta;
                posfrac += deltafrac;
                if posfrac >= (num_hist - 1) {
                    /* fractional part exceeds 1, carry to integer part */
                    pos += 1;
                    posfrac -= num_hist - 1;
                }
                i += 1;
            }
            length_hist_values = vals;
            num_hist_len = num_hist;
        } else {
            /*
             * Even when we don't create the histogram, store an empty array to
             * mean "no histogram". We can't just leave stavalues NULL, because
             * get_attstatsslot() errors if you ask for stavalues, and it's
             * NULL. We'll still store the empty fraction in stanumbers.
             *
             * length_hist_values = palloc(0); num_hist = 0;
             */
            length_hist_values = alloc::vec::Vec::new();
            num_hist_len = 0;
        }
        stats.staop[slot_idx] = Float8LessOperator;
        stats.stacoll[slot_idx] = InvalidOid;
        stats.numvalues[slot_idx] = num_hist_len;
        stats.stavalues[slot_idx] = length_hist_values;
        stats.statypid[slot_idx] = FLOAT8OID;
        stats.statyplen[slot_idx] = core::mem::size_of::<f64>() as i16; /* sizeof(float8) */
        stats.statypbyval[slot_idx] = FLOAT8PASSBYVAL;
        stats.statypalign[slot_idx] = b'd' as i8;

        /* Store the fraction of empty ranges */
        // emptyfrac = palloc(sizeof(float4));
        // *emptyfrac = ((double) empty_cnt) / ((double) non_null_cnt);
        let mut emptyfrac: alloc::vec::Vec<f32> = alloc::vec::Vec::new();
        emptyfrac.push((empty_cnt as f64 / non_null_cnt as f64) as f32);
        stats.numnumbers[slot_idx] = 1;
        stats.stanumbers[slot_idx] = emptyfrac;

        stats.stakind[slot_idx] = STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM as i16;
        slot_idx += 1;
        let _ = slot_idx;
    } else if null_cnt > 0 {
        /* We found only nulls; assume the column is entirely null */
        stats.stats_valid = true;
        stats.stanullfrac = 1.0;
        stats.stawidth = 0; /* "unknown" */
        stats.stadistinct = 0.0; /* "unknown" */
    }

    /*
     * We don't need to bother cleaning up any of our temporary palloc's. The
     * hashtable should also go away, as it used a child memory context.
     */
    Ok(())
}

/// `qsort_interruptible(bounds, n, sizeof(RangeBound), range_bound_qsort_cmp,
/// typcache)` — the comparator (`range_bound_qsort_cmp`, which calls
/// `range_cmp_bounds(typcache, b1, b2)`) is owned in-crate; the driver's
/// interrupt check is taken before the sort.
///
/// A comparator `ereport(ERROR)` (the subtype `cmp` support function) cannot
/// propagate through `slice::sort_by`; it is captured into `err` and rethrown
/// once the sort returns.
fn sort_range_bounds<'mcx>(typcache: &TypeCacheEntry, bounds: &mut [RangeBound]) -> PgResult<()> {
    // qsort_interruptible's cooperative interrupt check.
    vacuum_seams::vacuum_delay_point::call()?;

    let mut captured: Option<::types_error::PgError> = None;
    bounds.sort_by(|a, b| match range_bound_qsort_cmp(typcache, a, b) {
        Ok(c) => int_ordering(c),
        Err(e) => {
            if captured.is_none() {
                captured = Some(e);
            }
            Ordering::Equal
        }
    });
    match captured {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Bridge a canonical by-reference [`Datum`] (the fetched range/multirange
/// value) into the bare-word transitional `Datum` the detoast/(de)serialization
/// seams consume: a varlena pointer word into the value's `'mcx` bytes. The
/// referenced bytes outlive every seam call that reads them (they belong to the
/// value held in the sample row's `anl_context`).
#[inline]
fn bridge_value_ptr(value: &Datum<'_>) -> WordDatum {
    WordDatum::from_usize(value.as_ref_bytes().as_ptr() as usize)
}

/// `PointerGetDatum(range_serialize(...))`: materialize a serialized
/// `RangeTypeP` (a varlena image living in `mcx`) into a canonical
/// by-reference [`Datum`] for storage in `stats->stavalues`. The verbatim
/// varlena bytes (header included, `VARSIZE`) are copied into an `mcx`
/// `PgVec`, mirroring the C storing the `palloc`'d range pointer in the slot.
fn range_type_p_to_datum<'mcx>(mcx: Mcx<'mcx>, range: RangeTypeP<'mcx>) -> PgResult<Datum<'mcx>> {
    // The serialized range is always an uncompressed 4-byte-header varlena
    // (range_serialize builds it with SET_VARSIZE). Read its total length from
    // the header and copy the whole image.
    let p = range.ptr as *const u8;
    // SAFETY: `range.ptr` is a valid, 'mcx-lived, fully-serialized RangeType
    // varlena (a 4B uncompressed image); its first 4 bytes are the length word.
    let len = unsafe { varsize_4b_le(p) };
    let bytes = unsafe { core::slice::from_raw_parts(p, len) };
    Ok(Datum::ByRef(::mcx::slice_in(mcx, bytes)?))
}

/// `VARSIZE_4B(PTR)` (varatt.h, little-endian build): the total varlena length
/// `(va_header >> 2) & 0x3FFFFFFF` for an uncompressed 4-byte-header datum.
///
/// # Safety
/// `ptr` must point at a valid 4-byte-header varlena with at least 4 readable
/// bytes.
#[inline]
unsafe fn varsize_4b_le(ptr: *const u8) -> usize {
    let header = (ptr as *const u32).read_unaligned();
    ((header >> 2) & 0x3FFF_FFFF) as usize
}
