//! Unit tests for `nbtcompare.c`.
//!
//! The three-way comparison kernels and the skipsupport increment/decrement
//! kernels are pure (no seam), so most tests exercise them directly and need no
//! seam providers. The `*sortsupport` / `*skipsupport` strategy routines route
//! their live-node field write through the OUTWARD install seams; one test
//! installs recording stubs and confirms the in-crate dispatch reaches the
//! right installer with the correct boundary `Datum`s.
//!
//! The install seams are process-global once-only slots, so the seam-installing
//! test installs each slot exactly once and exercises every routine in a single
//! test body.

use super::*;

// ===========================================================================
// pure comparison kernels
// ===========================================================================

#[test]
fn int4_cmp_total_order() {
    assert_eq!(btint4cmp(1, 2), -1);
    assert_eq!(btint4cmp(2, 2), 0);
    assert_eq!(btint4cmp(3, 2), 1);
    // No overflow even at the extremes.
    assert_eq!(btint4cmp(i32::MIN, i32::MAX), -1);
    assert_eq!(btint4cmp(i32::MAX, i32::MIN), 1);
}

#[test]
fn int2_cmp_uses_subtraction() {
    assert_eq!(btint2cmp(-5, 3), -8);
    assert_eq!(btint2cmp(3, -5), 8);
}

#[test]
fn int8_cmp_total_order() {
    assert_eq!(btint8cmp(1, 2), -1);
    assert_eq!(btint8cmp(2, 2), 0);
    assert_eq!(btint8cmp(3, 2), 1);
    assert_eq!(btint8cmp(i64::MIN, i64::MAX), -1);
    assert_eq!(btint8cmp(i64::MAX, i64::MIN), 1);
}

#[test]
fn bool_cmp() {
    assert_eq!(btboolcmp(false, true), -1);
    assert_eq!(btboolcmp(true, false), 1);
    assert_eq!(btboolcmp(true, true), 0);
}

#[test]
fn oid_cmp_total_order() {
    assert_eq!(btoidcmp(1, 2), -1);
    assert_eq!(btoidcmp(2, 2), 0);
    assert_eq!(btoidcmp(3, 2), 1);
    // Oid is unsigned: large values are greater.
    assert_eq!(btoidcmp(u32::MAX, 0), 1);
    assert_eq!(btoidcmp(0, u32::MAX), -1);
}

#[test]
fn oid_vector_cmp_length_first() {
    assert_eq!(btoidvectorcmp(&[1, 2], &[1, 2, 3]), -1);
    assert_eq!(btoidvectorcmp(&[1, 2, 3], &[1, 2]), 1);
    assert_eq!(btoidvectorcmp(&[1, 2, 3], &[1, 9, 3]), -1);
    assert_eq!(btoidvectorcmp(&[1, 2, 3], &[1, 2, 3]), 0);
}

#[test]
fn char_cmp_unsigned() {
    // -1 as i8 is 255 unsigned, which is > 1.
    assert_eq!(btcharcmp(-1, 1), 254);
    assert_eq!(btcharcmp(1, -1), -254);
    assert_eq!(btcharcmp(0, 0), 0);
}

#[test]
fn cross_width_cmp() {
    assert_eq!(btint48cmp(5, 5_i64), 0);
    assert_eq!(btint48cmp(i32::MIN, i64::MAX), -1);
    assert_eq!(btint84cmp(i64::MAX, 0), 1);
    assert_eq!(btint24cmp(3, 100000), -1);
    assert_eq!(btint42cmp(100000, 3), 1);
    assert_eq!(btint28cmp(3, 3_i64), 0);
    assert_eq!(btint82cmp(3_i64, 3), 0);
}

// ===========================================================================
// fast comparators (over packed Datums)
// ===========================================================================

#[test]
fn fastcmp_kernels_match_typed_cmp() {
    // int2
    assert_eq!(btint2fastcmp(int16_get_datum(-5), int16_get_datum(3)), -8);
    // int4
    assert_eq!(
        ssup_datum_int32_cmp(int32_get_datum(i32::MIN), int32_get_datum(i32::MAX)),
        -1
    );
    assert_eq!(
        ssup_datum_int32_cmp(int32_get_datum(7), int32_get_datum(7)),
        0
    );
    // int8
    assert_eq!(
        ssup_datum_signed_cmp(int64_get_datum(i64::MAX), int64_get_datum(i64::MIN)),
        1
    );
    // oid
    assert_eq!(
        btoidfastcmp(object_id_get_datum(u32::MAX), object_id_get_datum(0)),
        1
    );
    assert_eq!(
        btoidfastcmp(object_id_get_datum(2), object_id_get_datum(2)),
        0
    );
}

// ===========================================================================
// increment / decrement kernels (overflow / underflow)
// ===========================================================================

#[test]
fn bool_inc_dec() {
    // decrement true -> false, no underflow
    let (v, uf) = bool_decrement(bool_get_datum(true));
    assert!(!uf);
    assert!(!datum_get_bool(v));
    // decrement false -> underflow
    let (_v, uf) = bool_decrement(bool_get_datum(false));
    assert!(uf);
    // increment false -> true, no overflow
    let (v, of) = bool_increment(bool_get_datum(false));
    assert!(!of);
    assert!(datum_get_bool(v));
    // increment true -> overflow
    let (_v, of) = bool_increment(bool_get_datum(true));
    assert!(of);
}

#[test]
fn int2_inc_dec_extremes() {
    let (_v, uf) = int2_decrement(int16_get_datum(i16::MIN));
    assert!(uf);
    let (_v, of) = int2_increment(int16_get_datum(i16::MAX));
    assert!(of);
    let (v, of) = int2_increment(int16_get_datum(5));
    assert!(!of);
    assert_eq!(datum_get_int16(v), 6);
    let (v, uf) = int2_decrement(int16_get_datum(5));
    assert!(!uf);
    assert_eq!(datum_get_int16(v), 4);
}

#[test]
fn int4_inc_dec_extremes() {
    let (_v, uf) = int4_decrement(int32_get_datum(i32::MIN));
    assert!(uf);
    let (_v, of) = int4_increment(int32_get_datum(i32::MAX));
    assert!(of);
    let (v, of) = int4_increment(int32_get_datum(5));
    assert!(!of);
    assert_eq!(datum_get_int32(v), 6);
}

#[test]
fn int8_inc_dec_extremes() {
    let (_v, uf) = int8_decrement(int64_get_datum(i64::MIN));
    assert!(uf);
    let (_v, of) = int8_increment(int64_get_datum(i64::MAX));
    assert!(of);
    let (v, uf) = int8_decrement(int64_get_datum(0));
    assert!(!uf);
    assert_eq!(datum_get_int64(v), -1);
}

#[test]
fn oid_inc_dec_extremes() {
    // decrement InvalidOid (0) -> underflow
    let (_v, uf) = oid_decrement(object_id_get_datum(InvalidOid));
    assert!(uf);
    // increment OID_MAX -> overflow
    let (_v, of) = oid_increment(object_id_get_datum(OID_MAX));
    assert!(of);
    let (v, of) = oid_increment(object_id_get_datum(5));
    assert!(!of);
    assert_eq!(datum_get_object_id(v), 6);
}

#[test]
fn char_inc_dec_extremes() {
    // decrement 0 -> underflow
    let (_v, uf) = char_decrement(uint8_get_datum(0));
    assert!(uf);
    // increment UCHAR_MAX -> overflow
    let (_v, of) = char_increment(uint8_get_datum(UCHAR_MAX));
    assert!(of);
    let (v, of) = char_increment(uint8_get_datum(10));
    assert!(!of);
    assert_eq!(datum_get_uint8(v), 11);
}

// ===========================================================================
// sortsupport / skipsupport strategy routines route to the install seams
//
// The install seams are process-global once-only slots: install each exactly
// once here, then exercise every routine in this single test body.
// ===========================================================================

#[test]
fn strategy_routines_route_to_installers() {
    use ::mcx::MemoryContext;
    use ::types_sortsupport::{SkipSupportIncDecId, SortComparatorId};

    // Recording sortsupport installers: stamp an observable token so we know
    // which install path the in-crate dispatch reached.
    sort::install_sortsupport_int2::set(|ssup, _cmp| ssup.comparator = Some(SortComparatorId(2)));
    sort::install_sortsupport_int4::set(|ssup, _cmp| ssup.comparator = Some(SortComparatorId(4)));
    sort::install_sortsupport_int8::set(|ssup, _cmp| ssup.comparator = Some(SortComparatorId(8)));
    sort::install_sortsupport_oid::set(|ssup, _cmp| ssup.comparator = Some(SortComparatorId(26)));

    // Recording skipsupport installers: stamp a token into both inc/dec slots so
    // we can confirm the routine reached the right installer; low/high are set
    // by the routine directly and asserted below.
    sort::install_skipsupport_bool::set(|sk, _inc, _dec| {
        sk.decrement = Some(SkipSupportIncDecId(1));
        sk.increment = Some(SkipSupportIncDecId(1));
    });
    sort::install_skipsupport_int2::set(|sk, _inc, _dec| {
        sk.decrement = Some(SkipSupportIncDecId(2));
        sk.increment = Some(SkipSupportIncDecId(2));
    });
    sort::install_skipsupport_int4::set(|sk, _inc, _dec| {
        sk.decrement = Some(SkipSupportIncDecId(4));
        sk.increment = Some(SkipSupportIncDecId(4));
    });
    sort::install_skipsupport_int8::set(|sk, _inc, _dec| {
        sk.decrement = Some(SkipSupportIncDecId(8));
        sk.increment = Some(SkipSupportIncDecId(8));
    });
    sort::install_skipsupport_oid::set(|sk, _inc, _dec| {
        sk.decrement = Some(SkipSupportIncDecId(26));
        sk.increment = Some(SkipSupportIncDecId(26));
    });
    sort::install_skipsupport_char::set(|sk, _inc, _dec| {
        sk.decrement = Some(SkipSupportIncDecId(18));
        sk.increment = Some(SkipSupportIncDecId(18));
    });

    // --- sortsupport ---
    let cx = MemoryContext::new("nbtcompare test");
    let mcx = cx.mcx();

    let mut s = SortSupportData::new(mcx);
    btint2sortsupport(&mut s);
    assert_eq!(s.comparator, Some(SortComparatorId(2)));

    let mut s = SortSupportData::new(mcx);
    btint4sortsupport(&mut s);
    assert_eq!(s.comparator, Some(SortComparatorId(4)));

    let mut s = SortSupportData::new(mcx);
    btint8sortsupport(&mut s);
    assert_eq!(s.comparator, Some(SortComparatorId(8)));

    let mut s = SortSupportData::new(mcx);
    btoidsortsupport(&mut s);
    assert_eq!(s.comparator, Some(SortComparatorId(26)));

    // --- run_sortsupport by-OID dispatch (the OidFunctionCall1 stand-in) ---
    let mut s = SortSupportData::new(mcx);
    assert!(super::run_sortsupport(super::F_BTINT4SORTSUPPORT, &mut s));
    assert_eq!(s.comparator, Some(SortComparatorId(4)));

    let mut s = SortSupportData::new(mcx);
    assert!(super::run_sortsupport(super::F_BTOIDSORTSUPPORT, &mut s));
    assert_eq!(s.comparator, Some(SortComparatorId(26)));

    // An OID that is not an nbtcompare sortsupport routine: no dispatch, the
    // caller (sortsupport.c) falls through to its fmgr path.
    let mut s = SortSupportData::new(mcx);
    assert!(!super::run_sortsupport(9999, &mut s));
    assert_eq!(s.comparator, None);

    // --- skipsupport: boundary Datums set by routine + inc/dec installed ---
    let mut sk = SkipSupportData::new();
    btboolskipsupport(&mut sk);
    assert_eq!(sk.low_elem, bool_get_datum(false));
    assert_eq!(sk.high_elem, bool_get_datum(true));
    assert_eq!(sk.decrement, Some(SkipSupportIncDecId(1)));
    assert_eq!(sk.increment, Some(SkipSupportIncDecId(1)));

    let mut sk = SkipSupportData::new();
    btint2skipsupport(&mut sk);
    assert_eq!(sk.low_elem, int16_get_datum(i16::MIN));
    assert_eq!(sk.high_elem, int16_get_datum(i16::MAX));
    assert_eq!(sk.decrement, Some(SkipSupportIncDecId(2)));

    let mut sk = SkipSupportData::new();
    btint4skipsupport(&mut sk);
    assert_eq!(sk.low_elem, int32_get_datum(i32::MIN));
    assert_eq!(sk.high_elem, int32_get_datum(i32::MAX));
    assert_eq!(sk.decrement, Some(SkipSupportIncDecId(4)));

    let mut sk = SkipSupportData::new();
    btint8skipsupport(&mut sk);
    assert_eq!(sk.low_elem, int64_get_datum(i64::MIN));
    assert_eq!(sk.high_elem, int64_get_datum(i64::MAX));
    assert_eq!(sk.decrement, Some(SkipSupportIncDecId(8)));

    let mut sk = SkipSupportData::new();
    btoidskipsupport(&mut sk);
    assert_eq!(sk.low_elem, object_id_get_datum(InvalidOid));
    assert_eq!(sk.high_elem, object_id_get_datum(OID_MAX));
    assert_eq!(sk.decrement, Some(SkipSupportIncDecId(26)));

    let mut sk = SkipSupportData::new();
    btcharskipsupport(&mut sk);
    assert_eq!(sk.low_elem, uint8_get_datum(0));
    assert_eq!(sk.high_elem, uint8_get_datum(UCHAR_MAX));
    assert_eq!(sk.decrement, Some(SkipSupportIncDecId(18)));
}
