use super::*;
use crate::num::{float_penalty_num, penalty_num, Ctx, NumOps};

fn ctx() -> Ctx<'static> {
    Ctx {
        flinfo: None,
        collation: 0,
    }
}

// upstream 1e1d07792e08 (18.5): btree_gist: fix NaN handling in float4/float8 opclasses.
#[test]
fn float_opclass_comparators_are_nan_aware() {
    let c = &mut ctx();
    let nan = f64::NAN;
    assert!(Float8::eq(nan, nan, c).unwrap());
    assert!(Float8::le(nan, nan, c).unwrap() && Float8::ge(nan, nan, c).unwrap());
    assert!(!Float8::lt(nan, nan, c).unwrap() && !Float8::gt(nan, nan, c).unwrap());
    assert!(Float8::gt(nan, f64::INFINITY, c).unwrap());
    assert!(Float8::lt(f64::INFINITY, nan, c).unwrap());
    assert!(Float8::le(1.0, nan, c).unwrap());
    assert!(!Float8::eq(1.0, nan, c).unwrap());
    assert_eq!(Float8::key_cmp((nan, nan), (nan, nan), c).unwrap(), 0);
    assert_eq!(Float8::key_cmp((1.0, nan), (1.0, 2.0), c).unwrap(), 1);
    assert_eq!(Float8::key_cmp((1.0, 2.0), (nan, 2.0), c).unwrap(), -1);
    assert_eq!(Float8::key_cmp((-0.0, 1.0), (0.0, 1.0), c).unwrap(), 0);
    let nan4 = f32::NAN;
    assert!(Float4::eq(nan4, nan4, c).unwrap());
    assert!(Float4::gt(nan4, f32::MAX, c).unwrap());
    assert!(Float4::lt(-1.0, nan4, c).unwrap());
    assert!(Float4::ge(nan4, f32::INFINITY, c).unwrap());
    assert_eq!(Float4::key_cmp((nan4, 1.0), (nan4, 1.0), c).unwrap(), 0);
    assert_eq!(Float4::key_cmp((2.0, 1.0), (nan4, 1.0), c).unwrap(), -1);
    assert_eq!(Float4::key_cmp((2.0, nan4), (2.0, 1.0), c).unwrap(), 1);
}

#[test]
fn float_opclass_distance_is_nan_aware() {
    let c = &mut ctx();
    let (nan, inf) = (f64::NAN, f64::INFINITY);
    assert_eq!(Float8::dist(nan, nan, c).unwrap(), 0.0);
    assert_eq!(Float8::dist(nan, 1.0, c).unwrap(), inf);
    assert_eq!(Float8::dist(1.0, nan, c).unwrap(), inf);
    assert_eq!(Float8::dist(nan, inf, c).unwrap(), inf);
    assert_eq!(Float8::dist(inf, inf, c).unwrap(), 0.0);
    assert_eq!(Float8::dist(-inf, inf, c).unwrap(), inf);
    assert_eq!(Float8::dist(1.0, 3.0, c).unwrap(), 2.0);
    let err = Float8::dist(f64::MAX, -f64::MAX, c).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(err.message(), "value out of range: overflow");
    assert_eq!(Float4::dist(f32::NAN, f32::NAN, c).unwrap(), 0.0);
    assert_eq!(Float4::dist(f32::NAN, 1.0, c).unwrap(), inf);
    assert_eq!(Float4::dist(2.5, f32::NAN, c).unwrap(), inf);
    assert_eq!(Float4::dist(f32::INFINITY, f32::INFINITY, c).unwrap(), 0.0);
    assert_eq!(
        Float4::dist(f32::MAX, -f32::MAX, c).unwrap(),
        2.0 * f32::MAX as f64
    );
}

#[test]
fn float_penalty_matches_c_oracle() {
    // Bits from 18.6 btree_utils_num.h's *penalty_num_impl run on these inputs.
    let (nan, inf) = (f64::NAN, f64::INFINITY);
    let float_cases: &[(&str, (f64, f64, f64, f64, u16), u32)] = &[
        ("nan-new-upper", (1.0, 2.0, 1.0, nan, 1), 0x7effffff),
        ("nan-new-lower", (1.0, 2.0, nan, 2.0, 1), 0x00000000),
        ("nan-orig-both", (nan, nan, 1.0, 1.0, 1), 0x7effffff),
        ("nan-orig-upper", (1.0, nan, 0.0, 0.0, 1), 0x401fffff),
        ("nan-both-sides", (nan, nan, nan, nan, 1), 0x00000000),
        ("inf-new-upper", (1.0, 2.0, 1.0, inf, 1), 0x7effffff),
        ("inf-orig-both", (inf, inf, 5.0, 5.0, 1), 0x7effffff),
        ("neg-inf-lower", (-inf, 2.0, -inf, 3.0, 2), 0x3fd55554),
        ("finite", (10.0, 20.0, 5.0, 25.0, 3), 0x7dffffff),
        ("inside", (10.0, 20.0, 12.0, 18.0, 1), 0x00000000),
        ("huge", (-1e308, 1e308, -1.5e308, 1.5e308, 1), 0x7eaaaaaa),
        ("tiny", (0.1, 0.2, 0.05, 0.2, 2), 0x7de38e38),
    ];
    for (name, (ol, ou, nl, nu, natts), bits) in float_cases {
        let got = float_penalty_num(*ol, *ou, *nl, *nu, *natts);
        assert_eq!(got.to_bits(), *bits, "{name}: got {got:e}");
    }
    let int_cases: &[(&str, (f64, f64, f64, f64, u16), u32)] = &[
        ("int-finite", (10.0, 20.0, 5.0, 25.0, 3), 0x7dffffff),
        ("int-inside", (10.0, 20.0, 12.0, 18.0, 1), 0x00000000),
        (
            "int-big",
            (
                -9223372036854775808.0,
                9223372036854775807.0,
                -9223372036854775808.0,
                9223372036854775807.0,
                1,
            ),
            0x00000000,
        ),
        ("int-expand", (0.0, 0.0, 0.0, 1.0, 1), 0x7effffff),
        ("int-frac", (3.0, 7.0, 1.0, 7.0, 2), 0x7de38e38),
        (
            "int-wide",
            (-2147483648.0, 2147483647.0, -2147483648.0, 2147483647.0, 3),
            0x00000000,
        ),
        ("int-shift", (100.0, 200.0, 150.0, 300.0, 1), 0x7e7fffff),
    ];
    for (name, (ol, ou, nl, nu, natts), bits) in int_cases {
        let got = penalty_num(*ol, *ou, *nl, *nu, *natts);
        assert_eq!(got.to_bits(), *bits, "{name}: got {got:e}");
    }
}

fn varbit_image(bits: &str) -> Vec<u8> {
    let nbytes = bits.len().div_ceil(8);
    let mut img = vec![0u8; VARHDRSZ + 4 + nbytes];
    var::set_varsize(&mut img, VARHDRSZ + 4 + nbytes);
    img[VARHDRSZ..VARHDRSZ + 4].copy_from_slice(&(bits.len() as i32).to_ne_bytes());
    for (i, ch) in bits.bytes().enumerate() {
        if ch == b'1' {
            img[VARHDRSZ + 4 + i / 8] |= 0x80 >> (i % 8);
        }
    }
    img
}

// upstream 558c4ea9a43b (18.5): Use the proper comparator in gbt_bit_ssup_cmp.
#[test]
fn bit_sortsupport_orders_leaf_keys_by_bitcmp() {
    let mem = mcx::MemoryContext::new("t");
    let m = mem.mcx();
    let one = var::key_from_datum(&varbit_image("1"));
    let ten = var::key_from_datum(&varbit_image("10"));
    let zero9 = var::key_from_datum(&varbit_image("011111111"));
    let d = |k: &[u8]| Datum::from_usize(k.as_ptr() as usize);
    // bitcmp: bit bytes first (0x80 > 0x7f); byteacmp put the 1-bit value first.
    assert!(BitV::ssup_cmp(d(&one), d(&zero9), 0, m).unwrap() > 0);
    assert!(BitV::ssup_cmp(d(&zero9), d(&one), 0, m).unwrap() < 0);
    assert!(BitV::ssup_cmp(d(&one), d(&ten), 0, m).unwrap() < 0);
    assert!(BitV::ssup_cmp(d(&ten), d(&one), 0, m).unwrap() > 0);
    assert_eq!(BitV::ssup_cmp(d(&one), d(&one), 0, m).unwrap(), 0);
    let empty = var::key_from_datum(&varbit_image(""));
    let zero = var::key_from_datum(&varbit_image("0"));
    assert!(BitV::ssup_cmp(d(&empty), d(&zero), 0, m).unwrap() < 0);
    assert!(BitV::ssup_cmp(d(&zero), d(&one), 0, m).unwrap() < 0);
    assert!(BitV::ssup_cmp(d(&zero), d(&zero9), 0, m).unwrap() < 0);
    let leaf = BitV::leaf_cmp(&varbit_image("1"), &varbit_image("011111111"), &mut ctx());
    assert_eq!(BitV::ssup_cmp(d(&one), d(&zero9), 0, m).unwrap(), leaf.unwrap());
}

fn numeric_image(v: i64) -> Vec<u8> {
    let n = adt_numeric::int64_to_numeric(v);
    let p = n.payload();
    let mut img = vec![0u8; VARHDRSZ + p.len()];
    var::set_varsize(&mut img, VARHDRSZ + p.len());
    img[VARHDRSZ..].copy_from_slice(p);
    img
}

// upstream 12c519207db0 (18.5): Fix btree_gist's NotEqual strategy on internal index pages.
#[test]
fn not_equal_descends_through_truncated_internal_keys() {
    use crate::num::BT_NOT_EQUAL;
    let c = &mut ctx();
    let (one, ten) = (varbit_image("1"), varbit_image("10"));
    // gbt_bit_xfrm drops the bit count: both leaves become the node key 0x80.
    let node = var::key_copy(&bit_xfrm(&one), &bit_xfrm(&ten));
    let nk = var::key_readable(&node);
    assert_eq!(nk.lower, nk.upper);
    assert!(var::consistent::<BitV>(&nk, &bit_xfrm(&one), BT_NOT_EQUAL, false, c).unwrap());
    assert!(var::consistent::<BitV>(&nk, &bit_xfrm(&ten), BT_NOT_EQUAL, false, c).unwrap());
    let leaf = var::key_from_datum(&one);
    let lk = var::key_readable(&leaf);
    assert!(!var::consistent::<BitV>(&lk, &one, BT_NOT_EQUAL, true, c).unwrap());
    assert!(var::consistent::<BitV>(&lk, &ten, BT_NOT_EQUAL, true, c).unwrap());
    let (n7, n8) = (numeric_image(7), numeric_image(8));
    let same = var::key_copy(&n7, &n7);
    let sk = var::key_readable(&same);
    assert!(!var::consistent::<NumericV>(&sk, &n7, BT_NOT_EQUAL, false, c).unwrap());
    assert!(var::consistent::<NumericV>(&sk, &n8, BT_NOT_EQUAL, false, c).unwrap());
    let span = var::key_copy(&n7, &n8);
    let spk = var::key_readable(&span);
    assert!(var::consistent::<NumericV>(&spk, &n7, BT_NOT_EQUAL, false, c).unwrap());
    assert!(!num::consistent::<Int4>((5, 5), 5, BT_NOT_EQUAL, true, c).unwrap());
    assert!(num::consistent::<Int4>((5, 5), 6, BT_NOT_EQUAL, true, c).unwrap());
    assert!(!num::consistent::<Int4>((5, 5), 5, BT_NOT_EQUAL, false, c).unwrap());
    assert!(num::consistent::<Int4>((3, 7), 5, BT_NOT_EQUAL, false, c).unwrap());
}
