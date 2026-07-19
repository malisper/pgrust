use super::*;
use gin_vocab::GinBtreeType as T;

const C_COLLATION: Oid = 950;

fn num_img(s: &str) -> adt_numeric::NumericImage {
    adt_numeric::numeric_in(s, -1, None).unwrap().unwrap()
}

fn img_datum(bytes: &[u8]) -> Datum {
    Datum::from_usize(bytes.as_ptr() as usize)
}

#[test]
fn compare_prefix_strategy_mapping() {
    // orig=5 vs keys 4/5/6, per gin_btree_compare_prefix.
    let cases: [(u16, [i32; 3]); 5] = [
        (BT_LESS, [0, 1, 1]),
        (BT_LESS_EQUAL, [0, 0, 1]),
        (BT_EQUAL, [1, 0, 1]),
        (BT_GREATER_EQUAL, [1, 0, 0]),
        (BT_GREATER, [1, -1, 0]),
    ];
    for (strategy, expect) in cases {
        for (i, key) in [4i32, 5, 6].into_iter().enumerate() {
            let res = compare_prefix(
                T::Int4,
                Datum::from_i32(5),
                Datum::from_i32(key),
                strategy,
                C_COLLATION,
            )
            .unwrap();
            assert_eq!(res, expect[i], "strategy {strategy} key {key}");
        }
    }
    assert!(compare_prefix(T::Int4, Datum::from_i32(0), Datum::from_i32(0), 6, C_COLLATION)
        .is_err());
}

#[test]
fn numeric_leftmost_sentinel() {
    let cx = mcx::MemoryContext::new("btree_gin test");
    let sentinel = leftmost(cx.mcx(), T::Numeric).unwrap();
    assert_eq!(sentinel.as_usize(), 0);

    let v = num_img("-1e100");
    let d = img_datum(v.as_bytes());
    assert_eq!(compare(T::Numeric, sentinel, d, C_COLLATION).unwrap(), -1);
    assert_eq!(compare(T::Numeric, d, sentinel, C_COLLATION).unwrap(), 1);
    assert_eq!(compare(T::Numeric, sentinel, sentinel, C_COLLATION).unwrap(), 0);
}

#[test]
fn numeric_cross_category_compare() {
    let pairs = [
        ("0.001", "1", -1),
        ("1.5", "2", -1),
        ("1e10", "NaN", -1),
        ("NaN", "NaN", 0),
        ("-Infinity", "-1e300", -1),
        ("100", "1e2", 0),
    ];
    for (a, b, expect) in pairs {
        let (ia, ib) = (num_img(a), num_img(b));
        let (da, db) = (img_datum(ia.as_bytes()), img_datum(ib.as_bytes()));
        assert_eq!(compare(T::Numeric, da, db, C_COLLATION).unwrap(), expect, "{a} vs {b}");
        assert_eq!(compare(T::Numeric, db, da, C_COLLATION).unwrap(), -expect, "{b} vs {a}");
    }
}

#[test]
fn enum_leftmost_sentinel_even_oids() {
    let sentinel = Datum::from_oid(InvalidOid);
    let (a, b) = (Datum::from_oid(16384), Datum::from_oid(16386));
    assert_eq!(compare(T::Enum, sentinel, a, C_COLLATION).unwrap(), -1);
    assert_eq!(compare(T::Enum, a, sentinel, C_COLLATION).unwrap(), 1);
    assert_eq!(compare(T::Enum, sentinel, sentinel, C_COLLATION).unwrap(), 0);
    assert_eq!(compare(T::Enum, a, b, C_COLLATION).unwrap(), -1);
    assert_eq!(compare(T::Enum, b, a, C_COLLATION).unwrap(), 1);
    assert_eq!(compare(T::Enum, a, a, C_COLLATION).unwrap(), 0);
}

#[test]
fn scalar_leftmost_precede_all_values() {
    let cx = mcx::MemoryContext::new("btree_gin test");
    let m = cx.mcx();

    let lm = leftmost(m, T::Int2).unwrap();
    assert_eq!(compare(T::Int2, lm, Datum::from_i16(i16::MIN), C_COLLATION).unwrap(), 0);
    assert_eq!(compare(T::Int2, lm, Datum::from_i16(-1), C_COLLATION).unwrap(), -1);

    let lm = leftmost(m, T::Float8).unwrap();
    assert_eq!(compare(T::Float8, lm, Datum::from_f64(f64::MIN), C_COLLATION).unwrap(), -1);
    assert_eq!(
        compare(T::Float8, Datum::from_f64(f64::NAN), Datum::from_f64(f64::INFINITY), C_COLLATION)
            .unwrap(),
        1
    );

    let lm = leftmost(m, T::Timestamp).unwrap();
    assert_eq!(compare(T::Timestamp, lm, Datum::from_i64(0), C_COLLATION).unwrap(), -1);

    let lm = leftmost(m, T::Timetz).unwrap();
    let midnight_utc = adt_date::TimeTzADT { time: 0, zone: 0 };
    let mut b = [0u8; 12];
    b[..8].copy_from_slice(&midnight_utc.time.to_ne_bytes());
    b[8..].copy_from_slice(&midnight_utc.zone.to_ne_bytes());
    assert_eq!(compare(T::Timetz, lm, img_datum(&b), C_COLLATION).unwrap(), -1);

    let lm = leftmost(m, T::Interval).unwrap();
    let zero = [0u8; 16];
    assert_eq!(compare(T::Interval, lm, img_datum(&zero), C_COLLATION).unwrap(), -1);

    let lm = leftmost(m, T::Uuid).unwrap();
    let one = {
        let mut u = [0u8; 16];
        u[15] = 1;
        u
    };
    assert_eq!(compare(T::Uuid, lm, img_datum(&one), C_COLLATION).unwrap(), -1);
    assert_eq!(compare(T::Uuid, lm, img_datum(&[0u8; 16]), C_COLLATION).unwrap(), 0);

    let lm = leftmost(m, T::Bit).unwrap();
    let mut bits = [0u8; 9];
    bits[..4].copy_from_slice(&varatt::set_varsize_4b_word(9).to_ne_bytes());
    bits[4..8].copy_from_slice(&3i32.to_ne_bytes());
    bits[8] = 0b1010_0000;
    assert_eq!(compare(T::Bit, lm, img_datum(&bits), C_COLLATION).unwrap(), -1);

    // network_cmp returns a raw difference; only the sign is contractual.
    let lm = leftmost(m, T::Inet).unwrap();
    let mut inet = [0u8; 10];
    inet[..4].copy_from_slice(&varatt::set_varsize_4b_word(10).to_ne_bytes());
    inet[4] = adt_network::PGSQL_AF_INET;
    inet[5] = 32;
    inet[9] = 1;
    assert!(compare(T::Inet, lm, img_datum(&inet), C_COLLATION).unwrap() < 0);
}

#[test]
fn extract_query_shapes() {
    let cx = mcx::MemoryContext::new("btree_gin test");
    let m = cx.mcx();
    let q = Datum::from_i32(7);

    let (entry, partial, orig) = extract_query(m, T::Int4, q, BT_LESS).unwrap();
    assert_eq!(entry.as_i32(), i32::MIN);
    assert!(partial);
    assert_eq!(orig.as_i32(), 7);

    let (entry, partial, orig) = extract_query(m, T::Int4, q, BT_EQUAL).unwrap();
    assert_eq!(entry.as_i32(), 7);
    assert!(!partial);
    assert_eq!(orig.as_i32(), 7);

    let (entry, partial, _) = extract_query(m, T::Int4, q, BT_GREATER).unwrap();
    assert_eq!(entry.as_i32(), 7);
    assert!(partial);

    assert!(extract_query(m, T::Int4, q, 0).is_err());
}

#[test]
fn numeric_compare_realigns_short_keys() {
    let img = num_img("123.45");
    let payload = img.payload();
    let total = payload.len() + 1;
    let mut buf = vec![0u8; total + 1];
    // Image start even => 1B-header payload starts at an odd address.
    let o = buf.as_ptr() as usize % 2;
    buf[o] = ((total << 1) | 1) as u8;
    buf[o + 1..o + total].copy_from_slice(payload);
    let d_short = img_datum(&buf[o..o + total]);
    assert_eq!(var_payload(d_short).as_ptr() as usize % 2, 1);
    let d_flat = img_datum(img.as_bytes());
    assert_eq!(compare(T::Numeric, d_short, d_flat, C_COLLATION).unwrap(), 0);
    assert_eq!(compare(T::Numeric, d_flat, d_short, C_COLLATION).unwrap(), 0);
    let bigger = num_img("123.46");
    let d_bigger = img_datum(bigger.as_bytes());
    assert_eq!(compare(T::Numeric, d_short, d_bigger, C_COLLATION).unwrap(), -1);
}
