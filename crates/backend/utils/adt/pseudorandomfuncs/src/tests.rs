use crate::*;
use ::adt_numeric::io;
use ::adt_numeric::var::NumericImage;

fn num(s: &str) -> NumericImage {
    io::numeric_in(s, -1, None).unwrap().unwrap()
}

fn num_str(img: &NumericImage) -> String {
    let mut buf = Vec::new();
    io::numeric_out_into(img.num(), &mut buf);
    String::from_utf8(buf).unwrap()
}

fn nrandom(lo: &str, hi: &str) -> String {
    let (lo, hi) = (num(lo), num(hi));
    num_str(&numeric_random(lo.num(), hi.num()).unwrap())
}

// All expected sequences captured from live Homebrew PostgreSQL 18.3
// (aarch64-darwin) after SELECT setseed(<seed>).
#[test]
fn drandom_seeded_sequences() {
    setseed(0.5).unwrap();
    let want = [
        0.9851677175347999,
        0.825301858027981,
        0.12974610012450416,
        0.16356291958601088,
        0.6476186144084,
        0.8822771983038762,
        0.1404566845227775,
        0.15619865764623442,
        0.5145227426983392,
        0.7712969548127826,
    ];
    for w in want {
        assert_eq!(drandom(), w);
    }

    for (seed, want) in [
        (-0.25, [0.5553213340039351, 0.25148985326005135, 0.7088686957046861]),
        (0.0, [0.8702553105818676, 0.426569726107606, 0.6684808914837377]),
        (1.0, [0.3978842227698167, 0.7438732417540841, 0.3875091442400458]),
        (-1.0, [0.725656831544149, 0.21342431605981593, 0.08668744483804192]),
    ] {
        setseed(seed).unwrap();
        for w in want {
            assert_eq!(drandom(), w);
        }
    }
}

#[test]
fn drandom_normal_seeded_sequence() {
    setseed(0.5).unwrap();
    let want = [
        2.5832426701605056,
        -0.45134209986141144,
        0.9735451174009585,
        -0.4573669421509548,
        1.1914367045143273,
    ];
    // libm ln/sin differ in final ulps across platforms (regress rounds via
    // extra_float_digits=-1 for the same reason); 1e-14 relative band per
    // pg_prng's normal_f64 KAT precedent.
    for w in want {
        let got = drandom_normal(0.0, 1.0);
        assert!((got - w).abs() <= 1e-14 * w.abs().max(1.0), "{got} vs {w}");
    }
}

#[test]
fn intrandom_seeded_sequences() {
    setseed(0.5).unwrap();
    for w in [2, 2, 6, 2, 2, 5, 4, 4, 5, 6] {
        assert_eq!(int4random(1, 6).unwrap(), w);
    }
    for w in [57, 96, 75, 6, 48, 32, 15, 76, 30, 79] {
        assert_eq!(int4random(1, 100).unwrap(), w);
    }
    for w in [1030514066, 25244183, 1865123249, 1854876680, -1689561032] {
        assert_eq!(int4random(i32::MIN, i32::MAX).unwrap(), w);
    }
    for w in [
        1904816578321642612,
        2378596988253719677,
        -7173405709529878816,
        7930325638982231920,
        -207417438227053356,
    ] {
        assert_eq!(int8random(i64::MIN, i64::MAX).unwrap(), w);
    }
}

#[test]
fn numeric_random_seeded_sequences() {
    // Replays the exact call sequence of the oracle capture (each ranged call
    // may consume several raw draws, so the calls must match, not the count).
    setseed(0.5).unwrap();
    for _ in 0..10 {
        int4random(1, 6).unwrap();
    }
    for _ in 0..10 {
        int4random(1, 100).unwrap();
    }
    for _ in 0..5 {
        int4random(i32::MIN, i32::MAX).unwrap();
    }
    for _ in 0..5 {
        int8random(i64::MIN, i64::MAX).unwrap();
    }
    for w in [
        "-348223851076143780327123987164",
        "-732044692431109396149119683873",
        "566205944126779477015095872108",
        "-348018656923947830008009372480",
        "515403213098447294900793098299",
    ] {
        assert_eq!(nrandom("-1e30", "1e30"), w);
    }
    for w in ["-0.4", "-0.3", "-0.3", "0.1", "0.3"] {
        assert_eq!(nrandom("-0.4", "0.4"), w);
    }
    for w in [
        "0.092371469424528382430932123238",
        "0.386548413029634290687749268039",
        "0.950700568669228322631249433623",
    ] {
        assert_eq!(nrandom("0", "0.999999999999999999999999999999"), w);
    }

    // pow10 / wide-integer / mixed-scale arms, same oracle after setseed(0.25).
    setseed(0.25).unwrap();
    assert_eq!(nrandom("0", "99999999999999999999"), "35535848313178653449");
    assert_eq!(nrandom("0", "0.9"), "0.2");
    assert_eq!(nrandom("0", "0.99999"), "0.45064");
    assert_eq!(nrandom("-1e15", "1e15"), "-21924355836624");
    assert_eq!(nrandom("3.139", "3.141"), "3.139");
}

#[test]
fn numeric_random_bounds() {
    assert_eq!(nrandom("3.14", "3.14"), "3.14");
    let inf = num("Infinity");
    let one = num("1");
    let e = numeric_random(
        one.num(),
        inf.num(),
    )
    .unwrap_err();
    assert!(format!("{e:?}").contains("upper bound cannot be infinity"));
    let e = numeric_random(
        inf.num(),
        one.num(),
    )
    .unwrap_err();
    assert!(format!("{e:?}").contains("lower bound cannot be infinity"));
    let nan = num("NaN");
    let e = numeric_random(
        nan.num(),
        one.num(),
    )
    .unwrap_err();
    assert!(format!("{e:?}").contains("lower bound cannot be NaN"));
    let two = num("2");
    let e = numeric_random(
        two.num(),
        one.num(),
    )
    .unwrap_err();
    assert!(format!("{e:?}").contains("lower bound must be less than or equal to upper bound"));
}

#[test]
fn bound_order_errors() {
    assert!(int4random(1, 0).is_err());
    assert!(int8random(1000000000001, 1000000000000).is_err());
    assert_eq!(int4random(101, 101).unwrap(), 101);
    assert_eq!(int8random(1000000000001, 1000000000001).unwrap(), 1000000000001);
}

#[test]
fn setseed_range_and_message() {
    let e = setseed(1.5).unwrap_err();
    assert!(format!("{e:?}").contains("setseed parameter 1.5 is out of allowed range [-1,1]"));
    let e = setseed(-3.0).unwrap_err();
    assert!(format!("{e:?}").contains("setseed parameter -3 is out of allowed range [-1,1]"));
    assert!(setseed(f64::NAN).is_err());
    assert!(setseed(1.0).is_ok());
    assert!(setseed(-1.0).is_ok());
}

#[test]
fn unseeded_initializes_from_entropy() {
    // Fresh thread: SEED_SET is false there.
    std::thread::spawn(|| {
        let a = drandom();
        assert!((0.0..1.0).contains(&a));
    })
    .join()
    .unwrap();
}

#[test]
fn fmt_g_matches_c_printf() {
    assert_eq!(fmt_g(1.5), "1.5");
    assert_eq!(fmt_g(-3.0), "-3");
    assert_eq!(fmt_g(0.0001), "0.0001");
    assert_eq!(fmt_g(0.00001), "1e-05");
    assert_eq!(fmt_g(1234567.0), "1.23457e+06");
    assert_eq!(fmt_g(f64::NAN), "nan");
    assert_eq!(fmt_g(f64::INFINITY), "inf");
}
