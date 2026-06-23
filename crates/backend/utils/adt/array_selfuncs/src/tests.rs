//! Unit tests for the array selectivity kernels.
//!
//! The fmgr `function_call2_coll_datum` seam is a process-global slot, so the
//! tests that install it run under a shared mutex and install a single canonical
//! implementation once for the whole test run (an int4-style comparator that
//! orders the by-reference-capable `DatumV` words as signed `i32`s).

use super::*;
use std::sync::{Mutex, MutexGuard, Once};

use mcx::MemoryContext;

static SEAM_LOCK: Mutex<()> = Mutex::new(());

fn seam_lock() -> MutexGuard<'static, ()> {
    SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// Install a canonical `function_call2_coll_datum` once: a three-way comparison
/// of the two `DatumV` words read back as signed `i32` (the int4 `btint4cmp`),
/// with the result returned as an `int32` `DatumV` (C: `DatumGetInt32`).
fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        function_call2_coll_datum::set(|_mcx, _fn_oid, _coll, a, b| {
            use core::cmp::Ordering::*;
            let r = match (a.as_i32()).cmp(&b.as_i32()) {
                Less => -1i32,
                Equal => 0,
                Greater => 1,
            };
            Ok(DatumV::from_i32(r))
        });
    });
}

/// A comparator context whose `cmp_proc` is valid; the test comparator ignores
/// the actual OID / collation and orders by the raw word.
fn sample_typentry() -> ElemCmpInfo {
    ElemCmpInfo {
        type_id: 23, // INT4OID
        typlen: 4,
        typbyval: true,
        typalign: b'i' as i8,
        cmp_proc: 97, // F_BTINT4CMP (any valid OID; the test comparator ignores it)
        typcollation: 0,
    }
}

/* ---- floor_log2 (pure helper) ----------------------------------------- */

#[test]
fn floor_log2_matches_c() {
    assert_eq!(floor_log2(0), -1);
    assert_eq!(floor_log2(1), 0);
    assert_eq!(floor_log2(2), 1);
    assert_eq!(floor_log2(3), 1);
    assert_eq!(floor_log2(4), 2);
    assert_eq!(floor_log2(255), 7);
    assert_eq!(floor_log2(256), 8);
    assert_eq!(floor_log2(65535), 15);
    assert_eq!(floor_log2(65536), 16);
    assert_eq!(floor_log2(0xFFFF_FFFF), 31);
}

/* ---- float_compare_desc ----------------------------------------------- */

#[test]
fn float_compare_desc_orders_descending() {
    assert_eq!(float_compare_desc(2.0, 1.0), -1);
    assert_eq!(float_compare_desc(1.0, 2.0), 1);
    assert_eq!(float_compare_desc(1.0, 1.0), 0);

    let mut v = [0.1f32, 0.5, 0.2, 0.9, 0.3];
    qsort_float_desc(&mut v);
    assert_eq!(v, [0.9, 0.5, 0.3, 0.2, 0.1]);
}

/* ---- clamp_probability ------------------------------------------------ */

#[test]
fn clamp_probability_branch_order() {
    assert_eq!(clamp_probability(-0.5), 0.0);
    assert_eq!(clamp_probability(1.5), 1.0);
    assert_eq!(clamp_probability(0.5), 0.5);
}

/* ---- default_sel ------------------------------------------------------ */

#[test]
fn default_sel_by_operator() {
    assert_eq!(default_sel(OID_ARRAY_OVERLAP_OP), DEFAULT_OVERLAP_SEL);
    assert_eq!(default_sel(OID_ARRAY_CONTAINS_OP), DEFAULT_CONTAIN_SEL);
    assert_eq!(default_sel(OID_ARRAY_CONTAINED_OP), DEFAULT_CONTAIN_SEL);
}

/* ---- arraycontjoinsel (stub) ------------------------------------------ */

#[test]
fn arraycontjoinsel_returns_default() {
    assert_eq!(
        arraycontjoinsel(OID_ARRAY_OVERLAP_OP).unwrap(),
        DEFAULT_OVERLAP_SEL
    );
    assert_eq!(
        arraycontjoinsel(OID_ARRAY_CONTAINS_OP).unwrap(),
        DEFAULT_CONTAIN_SEL
    );
}

/* ---- calc_hist -------------------------------------------------------- */

#[test]
fn calc_hist_uniform_box() {
    // A single histogram with nhist boundaries {0,1,2,3,4}.  frac = 1/(nhist-1)
    // = 1/4.
    let ctx = MemoryContext::new("calc_hist_uniform_box");
    let hist = [0.0f32, 1.0, 2.0, 3.0, 4.0];
    let part = calc_hist(ctx.mcx(), &hist, 5, 4).unwrap();
    assert_eq!(part.len(), 5);
    // k=0: count=1, prev_interval=0, next_interval = 1 - 0 = 1 -> val = 0.5/1.
    assert!((part[0] - 0.25 * 0.5).abs() < 1e-6);
    // k=2 (interior bound): val = 0.5/1 + 0.5/1 = 1.0 -> frac*1.0 = 0.25.
    assert!((part[2] - 0.25).abs() < 1e-6);
}

/* ---- calc_distr ------------------------------------------------------- */

#[test]
fn calc_distr_two_independent_events() {
    // p = [0.5, 0.5], rest = 0.  Distribution of number of occurrences:
    // P(0) = 0.25, P(1) = 0.5, P(2) = 0.25.
    let ctx = MemoryContext::new("calc_distr_two_independent_events");
    let p = [0.5f32, 0.5];
    let dist = calc_distr(ctx.mcx(), &p, 2, 2, 0.0).unwrap();
    assert_eq!(dist.len(), 3);
    assert!((dist[0] - 0.25).abs() < 1e-6);
    assert!((dist[1] - 0.5).abs() < 1e-6);
    assert!((dist[2] - 0.25).abs() < 1e-6);
    let sum: f32 = dist.iter().sum();
    assert!((sum - 1.0).abs() < 1e-5);
}

#[test]
fn calc_distr_zero_events_is_certain_zero() {
    // n = 0, rest = 0: only M[0,0] = 1.
    let ctx = MemoryContext::new("calc_distr_zero_events_is_certain_zero");
    let dist = calc_distr(ctx.mcx(), &[], 0, 3, 0.0).unwrap();
    assert_eq!(dist.len(), 4);
    assert_eq!(dist[0], 1.0);
    assert_eq!(dist[1], 0.0);
    assert_eq!(dist[2], 0.0);
    assert_eq!(dist[3], 0.0);
}

/* ---- mcelem_array_contain_overlap_selec: no-stats single element ------ */

#[test]
fn overlap_selec_no_stats_contains_single_element() {
    // No MCELEM stats (numbers = None).  For "@>" with one element, the element
    // isn't found, so elem_selec = Min(DEFAULT_CONTAIN_SEL, minfreq/2) where
    // minfreq = 2*DEFAULT_CONTAIN_SEL, so minfreq/2 = DEFAULT_CONTAIN_SEL.
    // selec starts at 1.0 and is multiplied once.
    let _g = seam_lock();
    install_seams();
    let ctx = MemoryContext::new("overlap_selec_no_stats_contains_single_element");
    let typentry = sample_typentry();
    let array_data = [DatumV::from_i32(42)];
    let selec = mcelem_array_contain_overlap_selec(
        ctx.mcx(),
        &[],
        0,
        None,
        0,
        &array_data,
        1,
        OID_ARRAY_CONTAINS_OP,
        &typentry,
    )
    .unwrap();
    assert!((selec - DEFAULT_CONTAIN_SEL).abs() < 1e-9);
}

#[test]
fn overlap_selec_overlap_starts_at_zero() {
    // For "&&" with one no-stats element, selec = 0 + e - 0*e = e where
    // e = DEFAULT_CONTAIN_SEL.
    let _g = seam_lock();
    install_seams();
    let ctx = MemoryContext::new("overlap_selec_overlap_starts_at_zero");
    let typentry = sample_typentry();
    let array_data = [DatumV::from_i32(7)];
    let selec = mcelem_array_contain_overlap_selec(
        ctx.mcx(),
        &[],
        0,
        None,
        0,
        &array_data,
        1,
        OID_ARRAY_OVERLAP_OP,
        &typentry,
    )
    .unwrap();
    assert!((selec - DEFAULT_CONTAIN_SEL).abs() < 1e-9);
}

/* ---- mcelem_array_contained_selec: punts without stats ---------------- */

#[test]
fn contained_selec_punts_without_numbers() {
    let _g = seam_lock();
    install_seams();
    let ctx = MemoryContext::new("contained_selec_punts_without_numbers");
    let typentry = sample_typentry();
    let array_data = [DatumV::from_i32(1)];
    let selec = mcelem_array_contained_selec(
        ctx.mcx(),
        &[],
        0,
        None,
        0,
        &array_data,
        1,
        None,
        0,
        OID_ARRAY_CONTAINED_OP,
        &typentry,
    )
    .unwrap();
    assert_eq!(selec, DEFAULT_CONTAIN_SEL);
}

#[test]
fn contained_selec_full_path_in_range() {
    // Valid MCELEM (nnumbers == nmcelem + 3) and count histogram (nhist >= 3) so
    // the worker reaches the full allocation/computation path; the result is a
    // real selectivity in [0, 1].
    let _g = seam_lock();
    install_seams();
    let ctx = MemoryContext::new("contained_selec_full_path");
    let typentry = sample_typentry();
    let numbers = [0.01f32, 0.5, 0.02]; // minfreq, maxfreq, nullelem_freq
    let hist = [0.0f32, 1.0, 1.0]; // count histogram; hist[nhist-1] = avg_count
    let array_data = [DatumV::from_i32(7)];
    let selec = mcelem_array_contained_selec(
        ctx.mcx(),
        &[], // mcelem
        0,   // nmcelem
        Some(&numbers),
        3, // nnumbers == nmcelem + 3
        &array_data,
        1, // nitems
        Some(&hist),
        3, // nhist >= 3
        OID_ARRAY_CONTAINED_OP,
        &typentry,
    )
    .unwrap();
    assert!((0.0..=1.0).contains(&selec));
}
