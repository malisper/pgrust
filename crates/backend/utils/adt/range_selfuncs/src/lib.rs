//! Selectivity estimation of range and multirange operators
//! (`utils/adt/rangetypes_selfuncs.c`, `utils/adt/multirangetypes_selfuncs.c`,
//! PostgreSQL 18.3).
//!
//! Estimates are based on histograms of lower and upper bounds, a histogram of
//! range lengths, and the fraction of empty (multi)ranges. The estimation
//! kernels and the `pg_statistic`-extraction prologue are byte-identical
//! between the two C files (only the operator vocabulary and the const/typcache
//! construction differ), so they live once here; the operator-specific dispatch
//! and the fmgr entry points are in the [`range`] and [`multirange`] modules.
//!
//! The fmgr-callable entry points reach the planner (`get_restriction_variable`,
//! `ReleaseVariableStats`, `statistic_proc_security_check`), `lsyscache`
//! (`get_commutator`, `get_attstatsslot`) and the range/multirange ADTs
//! (`range_get_typcache`, `range_serialize`, `make_multirange`,
//! `DatumGetRangeTypeP`, `range_deserialize`, `multirange_get_bounds`) — all
//! unported — through per-owner seams. The per-comparison `cmp` / `subdiff`
//! support functions cross the `backend-utils-adt-rangetypes-seams` seam. The
//! orchestration over those neighbor calls is this crate's own logic.

#![allow(non_upper_case_globals)]

extern crate alloc;

use ::mcx::Mcx;
use ::cache::typcache::TypeCacheEntry;
use ::types_core::primitive::{OidIsValid, Selectivity};
use types_error::{PgError, PgResult, ERROR};
use ::types_rangetypes::RangeBound;
use types_selfuncs::{
    AttStatsSlot, VariableStatData, ATTSTATSSLOT_NUMBERS, ATTSTATSSLOT_VALUES,
    STATISTIC_KIND_BOUNDS_HISTOGRAM, STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
};

use rangetypes_seams::{
    datum_get_range_type_p_value, range_cmp_bounds, range_deserialize, range_subdiff,
};
use lsyscache_seams::{get_attstatsslot, get_attstatsslot_value_datums};

pub mod multirange;
pub mod range;

/// Install every seam this crate owns. This crate owns no inward seams (its
/// fmgr entry points are reached through fmgr dispatch, not a cross-cycle seam),
/// so there is nothing to install yet.
pub fn init_seams() {}

/* ---------------------------------------------------------------------------
 * Variable-stats RAII guard (C: ReleaseVariableStats on every exit path).
 * ------------------------------------------------------------------------- */

use ::selfuncs_seams::release_variable_stats;

/// Holds a `VariableStatData` acquired by `get_restriction_variable`, running
/// `ReleaseVariableStats` (the `release_variable_stats` seam) on drop — covering
/// every early return / `?` exit, the C cleanup that AGENTS.md requires be RAII.
pub(crate) struct VarStatsGuard {
    vardata: VariableStatData,
}

impl VarStatsGuard {
    pub(crate) fn new(vardata: VariableStatData) -> Self {
        Self { vardata }
    }

    pub(crate) fn data(&self) -> &VariableStatData {
        &self.vardata
    }
}

impl Drop for VarStatsGuard {
    fn drop(&mut self) {
        release_variable_stats::call(self.vardata);
    }
}

/* ---------------------------------------------------------------------------
 * Local mirrors of C macros / helpers.
 * ------------------------------------------------------------------------- */

/// `CLAMP_PROBABILITY(p)` (selfuncs.h) — clamp to `[0, 1]`. Mirrors the C macro
/// branch order exactly (`< 0.0` first, then `> 1.0`) so a NaN passes through.
#[inline]
pub(crate) fn clamp_probability(p: f64) -> f64 {
    if p < 0.0 {
        0.0
    } else if p > 1.0 {
        1.0
    } else {
        p
    }
}

/// `Max(a, b)` for `double`.
#[inline]
fn max_f64(a: f64, b: f64) -> f64 {
    if a > b {
        a
    } else {
        b
    }
}

/// `Min(a, b)` for `double`.
#[inline]
fn min_f64(a: f64, b: f64) -> f64 {
    if a < b {
        a
    } else {
        b
    }
}

/// `Max(a, b)` for `int`.
#[inline]
fn max_i32(a: i32, b: i32) -> i32 {
    if a > b {
        a
    } else {
        b
    }
}

/// `Min(a, b)` for `int`.
#[inline]
fn min_i32(a: i32, b: i32) -> i32 {
    if a < b {
        a
    } else {
        b
    }
}

/// `get_float8_infinity()` (utils/float.h) — `(double) INFINITY`.
#[inline]
fn get_float8_infinity() -> f64 {
    f64::INFINITY
}

/// `elog(ERROR, msg)` — raise an internal error as a recoverable value.
#[inline]
pub(crate) fn elog_error<T>(msg: impl Into<alloc::string::String>) -> PgResult<T> {
    Err(PgError::new(ERROR, msg))
}

/* ===========================================================================
 * Shared null/empty-fraction lookup + merge epilogue (calc_*sel).
 *
 * `calc_rangesel` and `calc_multirangesel` are identical except for the
 * operator vocabulary in the empty-const switch and the `<@` merge test. They
 * delegate those to the vocabulary modules via closures and share this body.
 * =========================================================================== */

/// Look up `(null_frac, empty_frac)` from `pg_statistic` (C: the `calc_*sel`
/// prologue reading `stanullfrac` and the single
/// `STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM` *number*).
fn lookup_null_empty_frac(
    mcx: Mcx<'_>,
    vardata: &VariableStatData,
) -> PgResult<(f32, f32)> {
    if let Some(stats_tuple) = vardata.stats_tuple {
        let null_frac =
            ::selfuncs_seams::stats_tuple_stanullfrac::call(stats_tuple);

        /* Try to get fraction of empty (multi)ranges */
        let empty_frac = match get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
            0,
            ATTSTATSSLOT_NUMBERS,
        )? {
            Some(sslot) => {
                if sslot.numbers.len() != 1 {
                    /* shouldn't happen */
                    return elog_error("invalid empty fraction statistic");
                }
                sslot.numbers[0]
                /* sslot frees on drop (C: free_attstatsslot) */
            }
            /* No empty fraction statistic. Assume no empty ranges. */
            None => 0.0,
        };

        Ok((null_frac, empty_frac))
    } else {
        /*
         * No stats are available. Follow through assuming no NULLs and no empty
         * ranges; this still gives a better-than-nothing estimate based on
         * whether the constant is empty.
         */
        Ok((0.0, 0.0))
    }
}

/// The shared body of `calc_rangesel` / `calc_multirangesel`: the empty-const
/// switch, the histogram path with the default-estimate fallback, the empty /
/// non-empty merge, and the strict-operator null multiplier + clamp.
///
/// `const_is_empty` is `RangeIsEmpty(constval)` / `MultirangeIsEmpty(constval)`.
/// `empty_selec` resolves the empty-constant switch (per-operator selectivity,
/// or `Err` for the `elog(ERROR, "unexpected operator")` arms). `is_contained_op`
/// is the `<@`-family merge test. `default_selec` is the default-selectivity
/// routine. `hist_dispatch` runs `calc_hist_selectivity`.
fn calc_sel(
    mcx: Mcx<'_>,
    vardata: &VariableStatData,
    operator: u32,
    const_is_empty: bool,
    empty_selec: impl Fn(u32, f32) -> PgResult<f64>,
    is_contained_op: impl Fn(u32) -> bool,
    default_selec: impl Fn(u32) -> f64,
    hist_dispatch: impl FnOnce(Mcx<'_>, &VariableStatData) -> PgResult<f64>,
) -> PgResult<f64> {
    let (null_frac, empty_frac) = lookup_null_empty_frac(mcx, vardata)?;

    let selec: f64;

    if const_is_empty {
        /*
         * An empty (multi)range matches all (multi)ranges, all empty ones, or
         * nothing, depending on the operator.
         */
        selec = empty_selec(operator, empty_frac)?;
    } else {
        /*
         * Calculate selectivity using bound histograms. If that fails for some
         * reason, e.g no histogram in pg_statistic, use the default constant
         * estimate for the fraction of non-empty values. This still takes into
         * account the fraction of empty and NULL tuples, if we had statistics.
         */
        let mut hist_selec = hist_dispatch(mcx, vardata)?;
        if hist_selec < 0.0 {
            hist_selec = default_selec(operator);
        }

        /*
         * Now merge the results for the empty (multi)ranges and histogram
         * calculations, realizing that the histogram covers only the non-null,
         * non-empty values.
         */
        if is_contained_op(operator) {
            /* empty is contained by anything non-empty */
            selec = (1.0 - empty_frac as f64) * hist_selec + empty_frac as f64;
        } else {
            /* with any other operator, empty Op non-empty matches nothing */
            selec = (1.0 - empty_frac as f64) * hist_selec;
        }
    }

    /* all (multi)range operators are strict */
    let selec = selec * (1.0 - null_frac as f64);

    /* result should be in range, but make sure... */
    Ok(clamp_probability(selec))
}

/* ===========================================================================
 * Shared calc_hist_selectivity prologue (identical in both C files except the
 * range-vs-multirange const-bounds extraction, which the vocabulary modules do
 * themselves after this returns the histograms).
 * =========================================================================== */

/// The histograms a `calc_hist_selectivity` operator switch needs, or `None`
/// (→ the C `return -1.0`).
pub(crate) struct HistData<'mcx> {
    /// Lower bounds of the bounds histogram (parallel with `hist_upper`).
    pub hist_lower: alloc::vec::Vec<RangeBound>,
    /// Upper bounds of the bounds histogram.
    pub hist_upper: alloc::vec::Vec<RangeBound>,
    /// Range-length histogram values (`DatumGetFloat8`'d), or empty when the
    /// operator does not need it.
    pub length_hist: alloc::vec::Vec<f64>,
    /// Keeps the length-histogram slot alive while `length_hist` is used.
    _length_slot: Option<AttStatsSlot<'mcx>>,
}

impl HistData<'_> {
    pub(crate) fn length_hist(&self) -> &[f64] {
        &self.length_hist
    }
}

/// `calc_hist_selectivity` prologue: the support-function security checks, the
/// bounds-histogram extraction + `range_deserialize` loop (with the empty-range
/// `elog(ERROR)`), and the optional range-length histogram for `@>` / `<@`.
///
/// `rng_typcache` is the *range* type-cache entry (for multiranges,
/// `typcache->rngtype`). `needs_length_hist` is true for the contains/contained
/// operator family. Returns `None` for every C `return -1.0` path.
pub(crate) fn calc_hist_prologue<'mcx>(
    mcx: Mcx<'mcx>,
    rng_typcache: &TypeCacheEntry,
    vardata: &VariableStatData,
    needs_length_hist: bool,
) -> PgResult<Option<HistData<'mcx>>> {
    use ::selfuncs_seams::statistic_proc_security_check;

    /* Can't use the histogram with insecure range support functions */
    if !statistic_proc_security_check::call(vardata, rng_typcache.rng_cmp_proc_finfo.fn_oid)? {
        return Ok(None);
    }
    if OidIsValid(rng_typcache.rng_subdiff_finfo.fn_oid)
        && !statistic_proc_security_check::call(vardata, rng_typcache.rng_subdiff_finfo.fn_oid)?
    {
        return Ok(None);
    }

    /* Try to get histogram of ranges */
    let stats_tuple = match vardata.stats_tuple {
        Some(t) => t,
        None => return Ok(None),
    };
    /*
     * C: get_attstatsslot(&hslot, statistic_tuple, STATISTIC_KIND_BOUNDS_HISTOGRAM,
     *                     InvalidOid, ATTSTATSSLOT_VALUES); the slot's value array
     * elements are serialized `RangeType`s (a by-reference element type), so read
     * them as value-carrying canonical `Datum::ByRef` images -- the bare-word
     * `get_attstatsslot` path would yield non-dereferenceable in-buffer offsets
     * for each by-reference element.
     */
    let hist_values = match get_attstatsslot_value_datums::call(
        mcx,
        stats_tuple,
        STATISTIC_KIND_BOUNDS_HISTOGRAM,
        0,
    )? {
        Some(s) => s,
        None => return Ok(None),
    };

    /* check that it's a histogram, not just a dummy entry */
    let nhist = hist_values.len();
    if nhist < 2 {
        return Ok(None);
    }

    /*
     * Convert histogram of ranges into histograms of its lower and upper
     * bounds.
     */
    let mut hist_lower = alloc::vec::Vec::new();
    hist_lower.try_reserve(nhist).map_err(|_| mcx.oom(nhist))?;
    let mut hist_upper = alloc::vec::Vec::new();
    hist_upper.try_reserve(nhist).map_err(|_| mcx.oom(nhist))?;
    for i in 0..nhist {
        let range = datum_get_range_type_p_value::call(mcx, &hist_values[i])?;
        let (lower, upper, empty) = range_deserialize::call(rng_typcache, range)?;
        /* The histogram should not contain any empty ranges */
        if empty {
            return elog_error("bounds histogram contains an empty range");
        }
        hist_lower.push(lower);
        hist_upper.push(upper);
    }

    /* @> and @< also need a histogram of range lengths */
    let (length_hist, length_slot) = if needs_length_hist {
        let lslot = match get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
            0,
            ATTSTATSSLOT_VALUES,
        )? {
            Some(s) => s,
            None => return Ok(None),
        };
        /* check that it's a histogram, not just a dummy entry */
        if lslot.values.len() < 2 {
            return Ok(None);
        }
        let mut lh = alloc::vec::Vec::new();
        lh.try_reserve(lslot.values.len())
            .map_err(|_| mcx.oom(lslot.values.len()))?;
        for &v in lslot.values.iter() {
            lh.push(v.as_f64());
        }
        (lh, Some(lslot))
    } else {
        (alloc::vec::Vec::new(), None)
    };

    Ok(Some(HistData {
        hist_lower,
        hist_upper,
        length_hist,
        _length_slot: length_slot,
    }))
}

/* ===========================================================================
 * Shared estimation kernels.
 *
 * These are identical in both C files; `calc_hist_selectivity` for each
 * operator vocabulary calls them.
 * =========================================================================== */

/// `calc_hist_selectivity_scalar` — look up the fraction of values less than
/// (or equal, if `equal`) a given const in a histogram of range bounds.
pub(crate) fn calc_hist_selectivity_scalar(
    typcache: &TypeCacheEntry,
    constbound: &RangeBound,
    hist: &[RangeBound],
    equal: bool,
) -> PgResult<f64> {
    let hist_nvalues = hist.len() as i32;

    /*
     * Find the histogram bin the given constant falls into. Estimate
     * selectivity as the number of preceding whole bins.
     */
    let index = rbound_bsearch(typcache, constbound, hist, equal)?;
    let mut selec: Selectivity =
        (max_i32(index, 0) as Selectivity) / ((hist_nvalues - 1) as Selectivity);

    /* Adjust using linear interpolation within the bin */
    if index >= 0 && index < hist_nvalues - 1 {
        selec += get_position(
            typcache,
            constbound,
            &hist[index as usize],
            &hist[(index + 1) as usize],
        )? / ((hist_nvalues - 1) as Selectivity);
    }

    Ok(selec)
}

/// `rbound_bsearch` — binary search on an array of range bounds. Returns the
/// greatest index of a range bound which is less (less or equal, when `equal`)
/// than the given range bound, or -1 if none.
fn rbound_bsearch(
    typcache: &TypeCacheEntry,
    value: &RangeBound,
    hist: &[RangeBound],
    equal: bool,
) -> PgResult<i32> {
    let hist_length = hist.len() as i32;
    let mut lower: i32 = -1;
    let mut upper: i32 = hist_length - 1;

    while lower < upper {
        let middle = (lower + upper + 1) / 2;
        let cmp = range_cmp_bounds::call(typcache, &hist[middle as usize], value)?;

        if cmp < 0 || (equal && cmp == 0) {
            lower = middle;
        } else {
            upper = middle - 1;
        }
    }
    Ok(lower)
}

/// `length_hist_bsearch` — binary search on a length histogram.
fn length_hist_bsearch(length_hist: &[f64], value: f64, equal: bool) -> i32 {
    let length_hist_nvalues = length_hist.len() as i32;
    let mut lower: i32 = -1;
    let mut upper: i32 = length_hist_nvalues - 1;

    while lower < upper {
        let middle = (lower + upper + 1) / 2;

        let middleval = length_hist[middle as usize];
        if middleval < value || (equal && middleval <= value) {
            lower = middle;
        } else {
            upper = middle - 1;
        }
    }
    lower
}

/// `get_position` — relative position of a value in a histogram bin, in `[0,1]`.
fn get_position(
    typcache: &TypeCacheEntry,
    value: &RangeBound,
    hist1: &RangeBound,
    hist2: &RangeBound,
) -> PgResult<f64> {
    let has_subdiff = OidIsValid(typcache.rng_subdiff_finfo.fn_oid);

    if !hist1.infinite && !hist2.infinite {
        /*
         * Both bounds are finite. Assuming the subtype's comparison function
         * works sanely, the value must be finite, too, because it lies
         * somewhere between the bounds. If it doesn't, arbitrarily return 0.5.
         */
        if value.infinite {
            return Ok(0.5);
        }

        /* Can't interpolate without subdiff function */
        if !has_subdiff {
            return Ok(0.5);
        }

        /* Calculate relative position using subdiff function. */
        let bin_width = range_subdiff::call(typcache, hist2.val, hist1.val)?;
        if bin_width.is_nan() || bin_width <= 0.0 {
            return Ok(0.5); /* punt for NaN or zero-width bin */
        }

        let mut position = range_subdiff::call(typcache, value.val, hist1.val)? / bin_width;

        if position.is_nan() {
            return Ok(0.5); /* punt for NaN from subdiff, Inf/Inf, etc */
        }

        /* Relative position must be in [0,1] range */
        position = max_f64(position, 0.0);
        position = min_f64(position, 1.0);
        Ok(position)
    } else if hist1.infinite && !hist2.infinite {
        /*
         * Lower bin boundary is -infinite, upper is finite. If the value is
         * -infinite, return 0.0 to indicate it's equal to the lower bound.
         * Otherwise return 1.0 to indicate it's infinitely far from the lower
         * bound.
         */
        Ok(if value.infinite && value.lower {
            0.0
        } else {
            1.0
        })
    } else if !hist1.infinite && hist2.infinite {
        /* same as above, but in reverse */
        Ok(if value.infinite && !value.lower {
            1.0
        } else {
            0.0
        })
    } else {
        /*
         * If both bin boundaries are infinite, they should be equal to each
         * other, and the value should also be infinite and equal to both
         * bounds. (But don't Assert that, to avoid crashing if a user creates a
         * datatype with a broken comparison function).
         *
         * Assume the value to lie in the middle of the infinite bounds.
         */
        Ok(0.5)
    }
}

/// `get_len_position` — relative position of a value in a length-histogram bin,
/// in `[0,1]`.
fn get_len_position(value: f64, hist1: f64, hist2: f64) -> f64 {
    if !hist1.is_infinite() && !hist2.is_infinite() {
        /*
         * Both bounds are finite. The value should be finite too, because it
         * lies somewhere between the bounds. If it doesn't, just return
         * something.
         */
        if value.is_infinite() {
            return 0.5;
        }

        1.0 - (hist2 - value) / (hist2 - hist1)
    } else if hist1.is_infinite() && !hist2.is_infinite() {
        /*
         * Lower bin boundary is -infinite, upper is finite. Return 1.0 to
         * indicate the value is infinitely far from the lower bound.
         */
        1.0
    } else if hist1.is_infinite() && hist2.is_infinite() {
        /* same as above, but in reverse */
        0.0
    } else {
        /*
         * If both bin boundaries are infinite, they should be equal to each
         * other, and the value should also be infinite and equal to both
         * bounds. (But don't Assert that.) Assume the value to lie in the
         * middle of the infinite bounds.
         */
        0.5
    }
}

/// `get_distance` — measure the distance between two range bounds.
fn get_distance(
    typcache: &TypeCacheEntry,
    bound1: &RangeBound,
    bound2: &RangeBound,
) -> PgResult<f64> {
    let has_subdiff = OidIsValid(typcache.rng_subdiff_finfo.fn_oid);

    if !bound1.infinite && !bound2.infinite {
        /*
         * Neither bound is infinite, use subdiff function or return default
         * value of 1.0 if no subdiff is available.
         */
        if has_subdiff {
            let res = range_subdiff::call(typcache, bound2.val, bound1.val)?;
            /* Reject possible NaN result, also negative result */
            if res.is_nan() || res < 0.0 {
                Ok(1.0)
            } else {
                Ok(res)
            }
        } else {
            Ok(1.0)
        }
    } else if bound1.infinite && bound2.infinite {
        /* Both bounds are infinite */
        if bound1.lower == bound2.lower {
            Ok(0.0)
        } else {
            Ok(get_float8_infinity())
        }
    } else {
        /* One bound is infinite, the other is not */
        Ok(get_float8_infinity())
    }
}

/// `calc_length_hist_frac` — average of `P(x)` over `[length1, length2]`, where
/// `P(x)` is the fraction of tuples with `length < x` (or `length <= x` if
/// `equal`).
fn calc_length_hist_frac(length_hist: &[f64], length1: f64, length2: f64, equal: bool) -> f64 {
    let length_hist_nvalues = length_hist.len() as i32;

    debug_assert!(length2 >= length1);

    if length2 < 0.0 {
        return 0.0; /* shouldn't happen, but doesn't hurt to check */
    }

    /* All lengths in the table are <= infinite. */
    if length2.is_infinite() && equal {
        return 1.0;
    }

    /*
     * The average of a function between A and B is the area under the graph of
     * P(x) divided by the width. P(x) is defined by the length histogram; we
     * compute the area piecewise over the histogram bins, each a trapezoid of
     * area 1/2 * (P(x2) + P(x1)) * (x2 - x1). See the C source for the full
     * geometric derivation.
     */

    let mut a: f64;
    let mut b: f64;
    let mut pa: f64;
    let mut pb: f64;
    let mut pos: f64;
    let mut area: f64;

    /* First bin, the one that contains lower bound */
    let mut i = length_hist_bsearch(length_hist, length1, equal);
    if i >= length_hist_nvalues - 1 {
        return 1.0;
    }

    if i < 0 {
        i = 0;
        pos = 0.0;
    } else {
        /* interpolate length1's position in the bin */
        pos = get_len_position(
            length1,
            length_hist[i as usize],
            length_hist[(i + 1) as usize],
        );
    }
    pb = ((i as f64) + pos) / ((length_hist_nvalues - 1) as f64);
    b = length1;

    /*
     * In the degenerate case that length1 == length2, simply return P(length1).
     * This is not merely an optimization: if length1 == length2, we'd divide by
     * zero later on.
     */
    if length2 == length1 {
        return pb;
    }

    /*
     * Loop through all the bins, until we hit the last bin, the one that
     * contains the upper bound. (if lower and upper bounds are in the same bin,
     * this falls out immediately)
     */
    area = 0.0;
    while i < length_hist_nvalues - 1 {
        let bin_upper = length_hist[(i + 1) as usize];

        /* check if we've reached the last bin */
        if !(bin_upper < length2 || (equal && bin_upper <= length2)) {
            break;
        }

        /* the upper bound of previous bin is the lower bound of this bin */
        a = b;
        pa = pb;

        b = bin_upper;
        pb = (i as f64) / ((length_hist_nvalues - 1) as f64);

        /*
         * Add the area of this trapezoid to the total. The if-check avoids NaN
         * in the corner case PA == PB == 0 and B - A == Inf: the area of a
         * zero-height trapezoid is zero regardless of the width.
         */
        if pa > 0.0 || pb > 0.0 {
            area += 0.5 * (pb + pa) * (b - a);
        }

        i += 1;
    }

    /* Last bin */
    a = b;
    pa = pb;

    b = length2; /* last bin ends at the query upper bound */
    if i >= length_hist_nvalues - 1 {
        pos = 0.0;
    } else if length_hist[i as usize] == length_hist[(i + 1) as usize] {
        pos = 0.0;
    } else {
        pos = get_len_position(
            length2,
            length_hist[i as usize],
            length_hist[(i + 1) as usize],
        );
    }
    pb = ((i as f64) + pos) / ((length_hist_nvalues - 1) as f64);

    if pa > 0.0 || pb > 0.0 {
        area += 0.5 * (pb + pa) * (b - a);
    }

    /*
     * Divide the area (the integral) by width to get the requested average.
     * Avoid NaN arising from infinite / infinite (happens at least if length2
     * is infinite); 0.5 is as good as any value there.
     */
    if area.is_infinite() && length2.is_infinite() {
        0.5
    } else {
        area / (length2 - length1)
    }
}

/// `calc_hist_selectivity_contained` — selectivity of `var <@ const`: estimate
/// the fraction of (multi)ranges that fall within the constant lower and upper
/// bounds. The caller has already checked that the constant bounds are finite.
pub(crate) fn calc_hist_selectivity_contained(
    typcache: &TypeCacheEntry,
    lower: &RangeBound,
    upper: &mut RangeBound,
    hist_lower: &[RangeBound],
    length_hist: &[f64],
) -> PgResult<f64> {
    let hist_nvalues = hist_lower.len() as i32;

    /*
     * Begin by finding the bin containing the upper bound, in the lower bound
     * histogram. Any range with a lower bound > constant upper bound can't
     * match, ie. there are no matches in bins greater than upper_index.
     */
    upper.inclusive = !upper.inclusive;
    upper.lower = true;
    let mut upper_index = rbound_bsearch(typcache, upper, hist_lower, false)?;

    /*
     * If the upper bound value is below the histogram's lower limit, there are
     * no matches.
     */
    if upper_index < 0 {
        return Ok(0.0);
    }

    /*
     * If the upper bound value is at or beyond the histogram's upper limit,
     * start our loop at the last actual bin, as though the upper bound were
     * within that bin; get_position will clamp its result to 1.0 anyway.
     */
    upper_index = min_i32(upper_index, hist_nvalues - 2);

    /*
     * Calculate upper_bin_width, ie. the fraction of the (upper_index,
     * upper_index + 1) bin which is greater than upper bound of query range
     * using linear interpolation of subdiff function.
     */
    let upper_bin_width = get_position(
        typcache,
        upper,
        &hist_lower[upper_index as usize],
        &hist_lower[(upper_index + 1) as usize],
    )?;

    /*
     * In the loop, dist and prev_dist are the distance of the "current" bin's
     * lower and upper bounds from the constant upper bound. bin_width is the
     * width of the current bin: normally 1.0, but less at the start/end of the
     * loop. We start with bin_width = upper_bin_width, because we begin at the
     * bin containing the upper bound.
     */
    let mut prev_dist: f64 = 0.0;
    let mut bin_width = upper_bin_width;

    let mut sum_frac: f64 = 0.0;
    let mut i = upper_index;
    while i >= 0 {
        let dist: f64;
        let mut final_bin = false;

        /*
         * dist -- distance from upper bound of query range to lower bound of
         * the current bin in the lower bound histogram. Or to the lower bound
         * of the constant range, if this is the final bin, containing the
         * constant lower bound.
         */
        if range_cmp_bounds::call(typcache, &hist_lower[i as usize], lower)? < 0 {
            dist = get_distance(typcache, lower, upper)?;

            /*
             * Subtract from bin_width the portion of this bin that we want to
             * ignore.
             */
            bin_width -= get_position(
                typcache,
                lower,
                &hist_lower[i as usize],
                &hist_lower[(i + 1) as usize],
            )?;
            if bin_width < 0.0 {
                bin_width = 0.0;
            }
            final_bin = true;
        } else {
            dist = get_distance(typcache, &hist_lower[i as usize], upper)?;
        }

        /*
         * Estimate the fraction of tuples in this bin that are narrow enough to
         * not exceed the distance to the upper bound of the query range.
         */
        let length_hist_frac = calc_length_hist_frac(length_hist, prev_dist, dist, true);

        /*
         * Add the fraction of tuples in this bin, with a suitable length, to
         * the total.
         */
        sum_frac += length_hist_frac * bin_width / ((hist_nvalues - 1) as f64);

        if final_bin {
            break;
        }

        bin_width = 1.0;
        prev_dist = dist;

        i -= 1;
    }

    Ok(sum_frac)
}

/// `calc_hist_selectivity_contains` — selectivity of `var @> const`: estimate
/// the fraction of (multi)ranges that contain the constant lower and upper
/// bounds.
pub(crate) fn calc_hist_selectivity_contains(
    typcache: &TypeCacheEntry,
    lower: &RangeBound,
    upper: &RangeBound,
    hist_lower: &[RangeBound],
    length_hist: &[f64],
) -> PgResult<f64> {
    let hist_nvalues = hist_lower.len() as i32;

    /* Find the bin containing the lower bound of query range. */
    let mut lower_index = rbound_bsearch(typcache, lower, hist_lower, true)?;

    /*
     * If the lower bound value is below the histogram's lower limit, there are
     * no matches.
     */
    if lower_index < 0 {
        return Ok(0.0);
    }

    /*
     * If the lower bound value is at or beyond the histogram's upper limit,
     * start our loop at the last actual bin, as though the upper bound were
     * within that bin; get_position will clamp its result to 1.0 anyway.
     */
    lower_index = min_i32(lower_index, hist_nvalues - 2);

    /*
     * Calculate lower_bin_width, ie. the fraction of the (lower_index,
     * lower_index + 1) bin which is greater than lower bound of query range
     * using linear interpolation of subdiff function.
     */
    let lower_bin_width = get_position(
        typcache,
        lower,
        &hist_lower[lower_index as usize],
        &hist_lower[(lower_index + 1) as usize],
    )?;

    /*
     * Loop through all the lower bound bins, smaller than the query lower
     * bound. We begin from the query lower bound and walk backwards; the first
     * bin's upper bound is the query lower bound, and its distance to the query
     * upper bound is the length of the query range. bin_width is 1.0 except for
     * the first bin, which is only counted up to the constant lower bound.
     */
    let mut prev_dist = get_distance(typcache, lower, upper)?;
    let mut sum_frac: f64 = 0.0;
    let mut bin_width = lower_bin_width;
    let mut i = lower_index;
    while i >= 0 {
        /*
         * dist -- distance from upper bound of query range to current value of
         * lower bound histogram or lower bound of query range (if we've reached
         * it).
         */
        let dist = get_distance(typcache, &hist_lower[i as usize], upper)?;

        /*
         * Get average fraction of length histogram which covers intervals
         * longer than (or equal to) distance to upper bound of query range.
         */
        let length_hist_frac = 1.0 - calc_length_hist_frac(length_hist, prev_dist, dist, false);

        sum_frac += length_hist_frac * bin_width / ((hist_nvalues - 1) as f64);

        bin_width = 1.0;
        prev_dist = dist;

        i -= 1;
    }

    Ok(sum_frac)
}

#[cfg(test)]
mod tests;
