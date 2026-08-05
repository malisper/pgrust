use datum::Datum;
use types_fmgr::LocalFcinfo;

use super::builtins::*;
use super::*;

fn call2(f: types_fmgr::PGFunction, a: Datum, b: Datum) -> i32 {
    let mut fcinfo = LocalFcinfo::<2>::new(0);
    fcinfo.set_arg(0, a);
    fcinfo.set_arg(1, b);
    f(None, &mut fcinfo).unwrap().as_i32()
}

#[test]
fn scalar_cmp_signs_and_exact_values() {
    assert_eq!(btint4cmp(1, 2), -1);
    assert_eq!(btint4cmp(2, 2), 0);
    assert_eq!(btint4cmp(3, 2), 1);
    assert_eq!(btint4cmp(i32::MIN, i32::MAX), -1);
    assert_eq!(btint8cmp(i64::MIN, i64::MAX), -1);
    assert_eq!(btint8cmp(i64::MAX, i64::MIN), 1);
    assert_eq!(btint48cmp(-1, i64::MIN), 1);
    assert_eq!(btint84cmp(i64::MIN, -1), -1);
    assert_eq!(btint24cmp(-1, i32::MIN), 1);
    assert_eq!(btint42cmp(i32::MIN, -1), -1);
    assert_eq!(btint28cmp(-1, i64::MAX), -1);
    assert_eq!(btint82cmp(i64::MAX, -1), 1);
    assert_eq!(btoidcmp(0, u32::MAX), -1);
    assert_eq!(btoidcmp(7, 7), 0);
}

#[test]
fn subtraction_family_matches_c() {
    assert_eq!(btboolcmp(false, true), -1);
    assert_eq!(btboolcmp(true, false), 1);
    assert_eq!(btboolcmp(true, true), 0);
    assert_eq!(btint2cmp(i16::MIN, i16::MAX), i16::MIN as i32 - i16::MAX as i32);
    assert_eq!(btint2cmp(5, 5), 0);
    assert_eq!(btcharcmp(0, 255), -255);
    assert_eq!(btcharcmp(200, 100), 100);
}

#[test]
fn fc_wrappers_round_trip() {
    assert_eq!(call2(fc_btint4cmp, Datum::from_i32(-5), Datum::from_i32(9)), -1);
    assert_eq!(
        call2(fc_btint8cmp, Datum::from_i64(i64::MAX), Datum::from_i64(0)),
        1
    );
    assert_eq!(
        call2(fc_btint84cmp, Datum::from_i64(-1), Datum::from_i32(-1)),
        0
    );
    assert_eq!(call2(fc_btcharcmp, Datum::from_u8(200), Datum::from_u8(100)), 100);
    assert_eq!(call2(fc_btboolcmp, Datum::from_bool(false), Datum::from_bool(true)), -1);
}

#[test]
fn oidvector_cmp_and_validation() {
    // u32-backed for the datum's int alignment (typalign 'i').
    fn ov(values: &[u32]) -> Vec<u32> {
        let mut img = vec![0u32; 6 + values.len()];
        img[0] = ((img.len() as u32) * 4) << 2;
        img[1] = 1; // ndim
        img[2] = 0; // dataoffset
        img[3] = 26; // elemtype = OIDOID
        img[4] = values.len() as u32; // dim1
        img[5] = 0; // lbound1
        img[6..].copy_from_slice(values);
        img
    }
    let d = |img: &Vec<u32>| Datum::from_usize(img.as_ptr() as usize);

    let a = ov(&[1, 2, 3]);
    let b = ov(&[1, 2, 4]);
    let short = ov(&[1, 2]);
    assert_eq!(call2(fc_btoidvectorcmp, d(&a), d(&b)), -1);
    assert_eq!(call2(fc_btoidvectorcmp, d(&b), d(&a)), 1);
    assert_eq!(call2(fc_btoidvectorcmp, d(&a), d(&a)), 0);
    assert_eq!(call2(fc_btoidvectorcmp, d(&short), d(&a)), -1);

    let mut bad = ov(&[1]);
    bad[1] = 2; // ndim != 1
    let mut fcinfo = LocalFcinfo::<2>::new(0);
    fcinfo.set_arg(0, d(&bad));
    fcinfo.set_arg(1, d(&a));
    let err = fc_btoidvectorcmp(None, &mut fcinfo).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DATATYPE_MISMATCH);
}

#[test]
fn builtin_table_oid_ascending() {
    for w in NBT_BUILTINS.windows(2) {
        assert!(w[0].foid < w[1].foid);
    }
}
