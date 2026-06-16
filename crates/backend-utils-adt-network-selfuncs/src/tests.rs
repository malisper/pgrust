//! Unit tests for the `network_selfuncs` port.
//!
//! The pure-logic helpers (`inet_opr_codenum`, `inet_inclusion_cmp`,
//! `inet_masklen_inclusion_cmp`, `inet_hist_match_divider`, `mcv_population`,
//! `clamp_probability`, `default_sel`) take no seam and are exercised directly
//! against hand-built [`inet_struct`]s. The histogram kernel `inet_hist_value_sel`
//! installs the inet-detoast seam (a process-global slot, so the seam-installing
//! tests share a mutex) so the decision arithmetic can be checked end-to-end.
//! `networksel`'s operator guard errors before any seam is consulted, so it is
//! exercised with a real (empty) `MemoryContext`.

use super::*;

use std::cell::RefCell;
use std::sync::{Mutex, MutexGuard, Once};

use mcx::MemoryContext;
use types_network::{PGSQL_AF_INET, PGSQL_AF_INET6};

static SEAM_LOCK: Mutex<()> = Mutex::new(());

fn seam_lock() -> MutexGuard<'static, ()> {
    SEAM_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/* ---- inet_struct builders ---------------------------------------------- */

/// A `PGSQL_AF_INET` (IPv4) `inet_struct` with the given /bits and the first
/// `addr` bytes set (the rest zero).
fn v4(bits: u8, addr: &[u8]) -> inet_struct {
    let mut ipaddr = [0u8; 16];
    ipaddr[..addr.len()].copy_from_slice(addr);
    inet_struct {
        family: PGSQL_AF_INET,
        bits,
        ipaddr,
    }
}

/// A `PGSQL_AF_INET6` (IPv6) `inet_struct`.
fn v6(bits: u8, addr: &[u8]) -> inet_struct {
    let mut ipaddr = [0u8; 16];
    ipaddr[..addr.len()].copy_from_slice(addr);
    inet_struct {
        family: PGSQL_AF_INET6,
        bits,
        ipaddr,
    }
}

/* ====================================================================== *
 * inet_opr_codenum (network_selfuncs.c:853-873)
 * ====================================================================== */

#[test]
fn opr_codenum_map_matches_c() {
    assert_eq!(inet_opr_codenum(OID_INET_SUP_OP).unwrap(), -2);
    assert_eq!(inet_opr_codenum(OID_INET_SUPEQ_OP).unwrap(), -1);
    assert_eq!(inet_opr_codenum(OID_INET_OVERLAP_OP).unwrap(), 0);
    assert_eq!(inet_opr_codenum(OID_INET_SUBEQ_OP).unwrap(), 1);
    assert_eq!(inet_opr_codenum(OID_INET_SUB_OP).unwrap(), 2);
}

#[test]
fn opr_codenum_unsupported_errors() {
    // Some unrelated operator OID.
    assert!(inet_opr_codenum(96).is_err());
}

/* ====================================================================== *
 * default_sel (network_selfuncs.c DEFAULT_SEL macro)
 * ====================================================================== */

#[test]
fn default_sel_by_operator() {
    assert_eq!(default_sel(OID_INET_OVERLAP_OP), DEFAULT_OVERLAP_SEL);
    assert_eq!(default_sel(OID_INET_SUB_OP), DEFAULT_INCLUSION_SEL);
    assert_eq!(default_sel(OID_INET_SUBEQ_OP), DEFAULT_INCLUSION_SEL);
    assert_eq!(default_sel(OID_INET_SUP_OP), DEFAULT_INCLUSION_SEL);
    assert_eq!(default_sel(OID_INET_SUPEQ_OP), DEFAULT_INCLUSION_SEL);
}

/* ====================================================================== *
 * clamp_probability (selfuncs.h CLAMP_PROBABILITY)
 * ====================================================================== */

#[test]
fn clamp_probability_branch_order() {
    let mut p = -0.5;
    clamp_probability(&mut p);
    assert_eq!(p, 0.0);

    let mut p = 1.5;
    clamp_probability(&mut p);
    assert_eq!(p, 1.0);

    let mut p = 0.5;
    clamp_probability(&mut p);
    assert_eq!(p, 0.5);
}

/* ====================================================================== *
 * mcv_population (network_selfuncs.c:553-565)
 * ====================================================================== */

#[test]
fn mcv_population_sums_leading_n() {
    let numbers = [0.1f32, 0.2, 0.3, 0.4];
    // Only the first 3 are summed.
    let sum = mcv_population(&numbers, 3);
    assert!((sum - 0.6).abs() < 1e-6);
    // All four.
    let sum = mcv_population(&numbers, 4);
    assert!((sum - 1.0).abs() < 1e-6);
    // None.
    assert_eq!(mcv_population(&numbers, 0), 0.0);
}

/* ====================================================================== *
 * inet_masklen_inclusion_cmp (network_selfuncs.c:922-944)
 * ====================================================================== */

#[test]
fn masklen_inclusion_cmp_accept_rules() {
    // order > 0 (left has more bits) is accepted by opr_codenum >= 0
    // (overlap/subeq/sub).
    let left = v4(24, &[10, 0, 0, 0]);
    let right = v4(16, &[10, 0, 0, 0]);
    assert_eq!(inet_masklen_inclusion_cmp(&left, &right, 0), 0); // overlap
    assert_eq!(inet_masklen_inclusion_cmp(&left, &right, 1), 0); // subeq
    assert_eq!(inet_masklen_inclusion_cmp(&left, &right, 2), 0); // sub
                                                                 // rejected by sup/supeq (codenum < 0): returns the codenum unchanged.
    assert_eq!(inet_masklen_inclusion_cmp(&left, &right, -2), -2);
    assert_eq!(inet_masklen_inclusion_cmp(&left, &right, -1), -1);

    // order == 0 (equal masklen): accepted for -1..=1, rejected otherwise.
    let a = v4(24, &[10, 0, 0, 0]);
    let b = v4(24, &[10, 0, 0, 0]);
    assert_eq!(inet_masklen_inclusion_cmp(&a, &b, -1), 0);
    assert_eq!(inet_masklen_inclusion_cmp(&a, &b, 0), 0);
    assert_eq!(inet_masklen_inclusion_cmp(&a, &b, 1), 0);
    assert_eq!(inet_masklen_inclusion_cmp(&a, &b, -2), -2);
    assert_eq!(inet_masklen_inclusion_cmp(&a, &b, 2), 2);

    // order < 0 (left fewer bits): accepted by opr_codenum <= 0.
    let left = v4(16, &[10, 0, 0, 0]);
    let right = v4(24, &[10, 0, 0, 0]);
    assert_eq!(inet_masklen_inclusion_cmp(&left, &right, -2), 0);
    assert_eq!(inet_masklen_inclusion_cmp(&left, &right, 0), 0);
    assert_eq!(inet_masklen_inclusion_cmp(&left, &right, 2), 2);
}

/* ====================================================================== *
 * inet_inclusion_cmp (network_selfuncs.c:896-912)
 * ====================================================================== */

#[test]
fn inclusion_cmp_family_difference() {
    let l = v4(32, &[10, 0, 0, 1]);
    let r = v6(128, &[0xfe, 0x80]);
    // Different families: returns family difference (INET - INET6 = 2 - 3 = -1).
    assert_eq!(inet_inclusion_cmp(&l, &r, 0), -1);
    assert_eq!(inet_inclusion_cmp(&r, &l, 0), 1);
}

#[test]
fn inclusion_cmp_address_difference_dominates() {
    // Same family, differing common-prefix bits -> bitncmp result returned.
    let l = v4(32, &[10, 0, 0, 1]);
    let r = v4(32, &[10, 0, 0, 2]);
    // 10.0.0.1 < 10.0.0.2 over 32 common bits.
    assert!(inet_inclusion_cmp(&l, &r, 0) < 0);
    assert!(inet_inclusion_cmp(&r, &l, 0) > 0);
}

#[test]
fn inclusion_cmp_falls_through_to_masklen() {
    // Same family, same address bits over the shorter mask -> masklen compare.
    let l = v4(24, &[10, 0, 0, 0]);
    let r = v4(16, &[10, 0, 0, 0]);
    // bitncmp over min(24,16)=16 bits is equal, so the masklen rule applies:
    // for "sub" (codenum 2), order>0 is accepted -> 0.
    assert_eq!(inet_inclusion_cmp(&l, &r, 2), 0);
    // for "sup" (codenum -2), order>0 is rejected -> codenum returned.
    assert_eq!(inet_inclusion_cmp(&l, &r, -2), -2);
}

/* ====================================================================== *
 * inet_hist_match_divider (network_selfuncs.c:956-990)
 * ====================================================================== */

#[test]
fn hist_match_divider_different_family_is_minus_one() {
    let b = v4(32, &[10, 0, 0, 1]);
    let q = v6(128, &[0xfe, 0x80]);
    assert_eq!(inet_hist_match_divider(&b, &q, 0), -1);
}

#[test]
fn hist_match_divider_exact_match_is_zero() {
    // Identical boundary and query: no non-common decisive bits.
    let b = v4(24, &[10, 1, 2, 0]);
    let q = v4(24, &[10, 1, 2, 0]);
    // overlap (codenum 0): decisive_bits = min_bits = 24; bitncommon = 24 -> 0.
    assert_eq!(inet_hist_match_divider(&b, &q, 0), 0);
}

#[test]
fn hist_match_divider_counts_noncommon_bits() {
    // boundary 10.1.0.0/24 vs query 10.1.128.0/24, codenum 0 (overlap).
    // masklen equal -> accepted (codenum in -1..=1). min_bits = 24.
    let b = v4(24, &[10, 1, 0, 0]);
    let q = v4(24, &[10, 1, 128, 0]);
    // bitncommon over 24 bits = 16 (first 16 bits common, bit 17 differs).
    // decisive_bits = min_bits = 24, so divider = 24 - 16 = 8.
    assert_eq!(inet_hist_match_divider(&b, &q, 0), 8);
}

/* ====================================================================== *
 * inet_hist_value_sel (network_selfuncs.c:618-679) — with the detoast seam.
 * ====================================================================== */

// A toy "inet datum registry": tests pack an index into the Datum word and the
// detoast seam recovers the inet_struct from a thread-local table.
thread_local! {
    static INET_TABLE: RefCell<Vec<inet_struct>> = const { RefCell::new(Vec::new()) };
}

fn intern_inet(s: inet_struct) -> Datum {
    INET_TABLE.with(|t| {
        let mut v = t.borrow_mut();
        let idx = v.len();
        v.push(s);
        // store idx+1 so a 0 word is never a valid handle (mirrors a non-null ptr)
        Datum::from_usize(idx + 1)
    })
}

fn test_detoast(value: Datum) -> PgResult<inet_struct> {
    INET_TABLE.with(|t| {
        let v = t.borrow();
        let idx = value.as_usize();
        assert!(idx >= 1 && idx <= v.len(), "bad inet handle in test");
        Ok(v[idx - 1])
    })
}

fn install_detoast() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        datum_get_inet_pp::set(test_detoast);
    });
}

#[test]
fn hist_value_sel_too_few_values_is_zero() {
    let _g = seam_lock();
    install_detoast();
    // nvalues <= 1 -> 0.0 (guard against zero divide), without touching values.
    let q = intern_inet(v4(32, &[10, 0, 0, 1]));
    assert_eq!(inet_hist_value_sel(&[], q, 0).unwrap(), 0.0);
    assert_eq!(inet_hist_value_sel(&[Datum::null()], q, 0).unwrap(), 0.0);
}

#[test]
fn hist_value_sel_full_bucket_match() {
    let _g = seam_lock();
    install_detoast();
    // A 2-endpoint (1-bucket) histogram whose endpoints both compare == 0 to the
    // query under the overlap operator -> the whole bucket matches: match_/n = 1.
    let same = v4(16, &[10, 0, 0, 0]);
    let v0 = intern_inet(same);
    let v1 = intern_inet(same);
    let q = intern_inet(v4(16, &[10, 0, 0, 0]));
    let values = [v0, v1];
    // one bucket (k=1, n=1), both endpoints order 0 -> whole bucket match.
    let sel = inet_hist_value_sel(&values, q, 0).unwrap();
    assert!((sel - 1.0).abs() < 1e-9, "sel = {sel}");
}

#[test]
fn hist_value_sel_no_match_is_zero() {
    let _g = seam_lock();
    install_detoast();
    // Histogram values in a different family than the query: the family-difference
    // orders are both positive (never bracket the query), so no bucket matches.
    let v0 = intern_inet(v6(128, &[0x20, 0x01]));
    let v1 = intern_inet(v6(128, &[0x20, 0x02]));
    let q = intern_inet(v4(32, &[10, 0, 0, 1]));
    let values = [v0, v1];
    // left_order = family(v6=3) - family(v4=2) = 1 > 0; right_order likewise 1.
    // Both positive: not both 0, and the bracketing clause needs (<=0 && >=0).
    let sel = inet_hist_value_sel(&values, q, 2).unwrap();
    assert_eq!(sel, 0.0);
}

/* ====================================================================== *
 * networksel (network_selfuncs.c:78-183) — operator guard.
 * ====================================================================== */

#[test]
fn networksel_rejects_unsupported_operator() {
    // inet_opr_codenum errors before any seam is consulted.
    let ctx = MemoryContext::new("test");
    let root = PlannerInfo::default();
    let res = networksel(ctx.mcx(), &root, 96, &[], 0);
    assert!(res.is_err());
}
