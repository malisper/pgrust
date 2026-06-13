//! Unit tests for the range/multirange selectivity kernels.
//!
//! The math-time seams (`range_cmp_bounds` / `range_subdiff`) are
//! process-global slots, so the tests that install them run under a shared
//! mutex; once installed a slot cannot be replaced, so a single canonical
//! implementation is installed for the whole test run.

use super::*;
use std::sync::{Mutex, MutexGuard, Once};

use types_core::fmgr::FmgrInfo;
use types_datum::datum::Datum;

static SEAM_LOCK: Mutex<()> = Mutex::new(());

fn seam_lock() -> MutexGuard<'static, ()> {
    SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// Install canonical `cmp` / `subdiff` implementations once: `cmp` orders by
/// the `f64` payload (infinite-aware), `subdiff` is plain subtraction.
fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        range_cmp_bounds::set(|_tc, b1, b2| Ok(cmp_bounds(b1, b2)));
        range_subdiff::set(|_tc, v1, v2| Ok(v1.as_f64() - v2.as_f64()));
    });
}

/// A reference range-bound comparison consistent with the C `range_cmp_bounds`
/// for our test subtype (order by value, then by the inclusive/lower flags).
fn cmp_bounds(b1: &RangeBound, b2: &RangeBound) -> i32 {
    let v1 = bound_sort_key(b1);
    let v2 = bound_sort_key(b2);
    if v1 < v2 {
        -1
    } else if v1 > v2 {
        1
    } else {
        0
    }
}

fn bound_sort_key(b: &RangeBound) -> f64 {
    if b.infinite {
        if b.lower {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        }
    } else {
        b.val.as_f64()
    }
}

fn finite_bound(x: f64, lower: bool, inclusive: bool) -> RangeBound {
    RangeBound {
        val: Datum::from_f64(x),
        infinite: false,
        inclusive,
        lower,
    }
}

fn inf_bound(lower: bool) -> RangeBound {
    RangeBound {
        val: Datum::null(),
        infinite: true,
        inclusive: false,
        lower,
    }
}

/// A typcache whose `rng_subdiff_finfo.fn_oid` is set (so interpolation runs).
fn typcache_with_subdiff() -> TypeCacheEntry {
    TypeCacheEntry {
        rng_subdiff_finfo: FmgrInfo { fn_oid: 1234, ..Default::default() },
        ..Default::default()
    }
}

/// A typcache with no subdiff (interpolation punts to 0.5).
fn typcache_no_subdiff() -> TypeCacheEntry {
    TypeCacheEntry::default()
}

#[test]
fn clamp_probability_matches_c_order() {
    assert_eq!(clamp_probability(-0.5), 0.0);
    assert_eq!(clamp_probability(1.5), 1.0);
    assert_eq!(clamp_probability(0.25), 0.25);
    // NaN passes through unchanged (< 0.0 false, > 1.0 false).
    assert!(clamp_probability(f64::NAN).is_nan());
}

#[test]
fn default_range_selectivity_table() {
    use crate::range::*;
    assert_eq!(default_range_selectivity(OID_RANGE_OVERLAP_OP), 0.01);
    assert_eq!(default_range_selectivity(OID_RANGE_CONTAINS_OP), 0.005);
    assert_eq!(default_range_selectivity(OID_RANGE_CONTAINED_OP), 0.005);
    assert_eq!(
        default_range_selectivity(OID_RANGE_CONTAINS_ELEM_OP),
        0.005
    );
    assert!((default_range_selectivity(OID_RANGE_LESS_OP) - 0.3333333333333333).abs() < 1e-15);
    assert_eq!(default_range_selectivity(9_999_999), 0.01);
}

#[test]
fn default_multirange_selectivity_table() {
    use crate::multirange::*;
    assert_eq!(
        default_multirange_selectivity(OID_MULTIRANGE_OVERLAPS_MULTIRANGE_OP),
        0.01
    );
    assert_eq!(
        default_multirange_selectivity(OID_MULTIRANGE_CONTAINS_RANGE_OP),
        0.005
    );
    assert_eq!(
        default_multirange_selectivity(OID_MULTIRANGE_CONTAINS_ELEM_OP),
        0.005
    );
    assert!(
        (default_multirange_selectivity(OID_MULTIRANGE_LESS_OP) - 0.3333333333333333).abs()
            < 1e-15
    );
    assert_eq!(default_multirange_selectivity(9_999_999), 0.01);
}

#[test]
fn length_hist_bsearch_basic() {
    let hist = [0.0, 1.0, 2.0, 3.0];
    // less-than search
    assert_eq!(length_hist_bsearch(&hist, 2.0, false), 1);
    // less-or-equal search
    assert_eq!(length_hist_bsearch(&hist, 2.0, true), 2);
    // below the table
    assert_eq!(length_hist_bsearch(&hist, -1.0, false), -1);
    // at/above the top
    assert_eq!(length_hist_bsearch(&hist, 10.0, false), 3);
}

#[test]
fn get_len_position_interpolates() {
    // value midway between finite bounds → 0.5
    assert!((get_len_position(1.5, 1.0, 2.0) - 0.5).abs() < 1e-12);
    // lower bound -inf → 1.0
    assert_eq!(get_len_position(5.0, f64::NEG_INFINITY, 10.0), 1.0);
    // both bounds -inf → 0.0 (the `isinf(hist1) && isinf(hist2)` arm).
    assert_eq!(
        get_len_position(0.0, f64::NEG_INFINITY, f64::NEG_INFINITY),
        0.0
    );
    // value infinite, both bounds finite → 0.5
    assert_eq!(get_len_position(f64::INFINITY, 0.0, 1.0), 0.5);
}

#[test]
fn calc_length_hist_frac_degenerate_and_full() {
    let hist = [0.0, 1.0, 2.0, 3.0];
    // length2 == length1 returns P(length1).
    let p = calc_length_hist_frac(&hist, 1.0, 1.0, true);
    assert!((0.0..=1.0).contains(&p));
    // equal && length2 == +inf returns 1.0.
    assert_eq!(calc_length_hist_frac(&hist, 0.0, f64::INFINITY, true), 1.0);
    // length2 < 0 returns 0.0.
    assert_eq!(calc_length_hist_frac(&hist, -3.0, -2.0, false), 0.0);
}

#[test]
fn get_position_punts_without_subdiff() {
    let _g = seam_lock();
    install_seams();
    let tc = typcache_no_subdiff();
    let v = finite_bound(0.5, true, true);
    let h1 = finite_bound(0.0, true, true);
    let h2 = finite_bound(1.0, true, true);
    // No subdiff → interpolation punts to 0.5.
    assert_eq!(get_position(&tc, &v, &h1, &h2).unwrap(), 0.5);
}

#[test]
fn get_position_interpolates_with_subdiff() {
    let _g = seam_lock();
    install_seams();
    let tc = typcache_with_subdiff();
    let v = finite_bound(0.25, true, true);
    let h1 = finite_bound(0.0, true, true);
    let h2 = finite_bound(1.0, true, true);
    // (0.25 - 0) / (1 - 0) = 0.25.
    assert!((get_position(&tc, &v, &h1, &h2).unwrap() - 0.25).abs() < 1e-12);
}

#[test]
fn get_position_infinite_bins() {
    let _g = seam_lock();
    install_seams();
    let tc = typcache_with_subdiff();
    // hist1 = -inf, hist2 finite, value = -inf lower → 0.0.
    let lo = inf_bound(true);
    let hi = finite_bound(5.0, false, true);
    assert_eq!(get_position(&tc, &lo, &lo, &hi).unwrap(), 0.0);
    // hist1 = -inf, hist2 finite, finite value → 1.0.
    let v = finite_bound(2.0, false, true);
    assert_eq!(get_position(&tc, &v, &lo, &hi).unwrap(), 1.0);
}

#[test]
fn get_distance_subdiff_and_infinities() {
    let _g = seam_lock();
    install_seams();
    let tc = typcache_with_subdiff();
    let b1 = finite_bound(1.0, true, true);
    let b2 = finite_bound(4.0, false, true);
    // subdiff(b2, b1) = 4 - 1 = 3.
    assert!((get_distance(&tc, &b1, &b2).unwrap() - 3.0).abs() < 1e-12);
    // both infinite, same side → 0.0.
    let lo = inf_bound(true);
    assert_eq!(get_distance(&tc, &lo, &lo).unwrap(), 0.0);
    // both infinite, opposite sides → +inf.
    let hi = inf_bound(false);
    assert!(get_distance(&tc, &lo, &hi).unwrap().is_infinite());
    // one infinite → +inf.
    assert!(get_distance(&tc, &b1, &hi).unwrap().is_infinite());
}

#[test]
fn get_distance_without_subdiff_is_one() {
    let _g = seam_lock();
    install_seams();
    let tc = typcache_no_subdiff();
    let b1 = finite_bound(1.0, true, true);
    let b2 = finite_bound(4.0, false, true);
    assert_eq!(get_distance(&tc, &b1, &b2).unwrap(), 1.0);
}

#[test]
fn rbound_bsearch_orders_by_value() {
    let _g = seam_lock();
    install_seams();
    let tc = typcache_with_subdiff();
    let hist = [
        finite_bound(0.0, true, true),
        finite_bound(1.0, true, true),
        finite_bound(2.0, true, true),
        finite_bound(3.0, true, true),
    ];
    let v = finite_bound(2.0, true, true);
    // less-than: greatest index strictly less than 2.0 is index 1 (value 1.0).
    assert_eq!(rbound_bsearch(&tc, &v, &hist, false).unwrap(), 1);
    // less-or-equal: greatest index <= 2.0 is index 2.
    assert_eq!(rbound_bsearch(&tc, &v, &hist, true).unwrap(), 2);
    // below all → -1.
    let below = finite_bound(-5.0, true, true);
    assert_eq!(rbound_bsearch(&tc, &below, &hist, false).unwrap(), -1);
}

#[test]
fn calc_hist_selectivity_scalar_fraction() {
    let _g = seam_lock();
    install_seams();
    let tc = typcache_with_subdiff();
    let hist = [
        finite_bound(0.0, true, true),
        finite_bound(1.0, true, true),
        finite_bound(2.0, true, true),
        finite_bound(3.0, true, true),
    ];
    // const 1.0, less-than: index 0 (greatest strictly < 1.0), whole-bin part
    // 0/3, plus interpolation get_position(1.0; 0.0,1.0)=1.0 → 1/3.
    let c = finite_bound(1.0, true, true);
    let sel = calc_hist_selectivity_scalar(&tc, &c, &hist, false).unwrap();
    assert!((sel - (1.0 / 3.0)).abs() < 1e-12);
    // value 1.5, less-than: index 1 whole bin (1/3) plus interpolated 0.5/3.
    let c2 = finite_bound(1.5, true, true);
    let sel2 = calc_hist_selectivity_scalar(&tc, &c2, &hist, false).unwrap();
    assert!((sel2 - (1.0 / 3.0 + 0.5 / 3.0)).abs() < 1e-12);
}
