//! Unit tests for the self-contained ndistinct logic: serialize/deserialize
//! round-trip (exact C byte layout), the combination generator, the Duj1
//! estimator, and the I/O surface.

use super::*;
use mcx::MemoryContext;

fn item(ndistinct: f64, attrs: &[AttrNumber]) -> MVNDistinctItem {
    MVNDistinctItem {
        ndistinct,
        attributes: attrs.to_vec(),
    }
}

fn ndist(items: Vec<MVNDistinctItem>) -> MVNDistinct {
    MVNDistinct {
        magic: STATS_NDISTINCT_MAGIC,
        r#type: STATS_NDISTINCT_TYPE_BASIC,
        items,
    }
}

#[test]
fn magic_and_type_constants() {
    // Verified against statistics/statistics.h.
    assert_eq!(STATS_NDISTINCT_MAGIC, 0xA352_BFA4);
    assert_eq!(STATS_NDISTINCT_TYPE_BASIC, 1);
    // SizeOfHeader = 3 * sizeof(uint32) = 12.
    assert_eq!(SIZE_OF_HEADER, 12);
    // SizeOfItem(natts) = sizeof(double) + sizeof(int) + natts*sizeof(AttrNumber).
    assert_eq!(size_of_item(2), 8 + 4 + 2 * 2);
    assert_eq!(min_size_of_item(), 16);
}

#[test]
fn serialize_byte_layout() {
    let ctx = MemoryContext::new("ndist_test");
    let mcx = ctx.mcx();

    let d = ndist(vec![item(7.0, &[1, 2])]);
    let bytes = statext_ndistinct_serialize(mcx, &d).unwrap();

    // VARHDRSZ(4) + SizeOfHeader(12) + SizeOfItem(2)=16 => 4 + 12 + 16 = 32.
    assert_eq!(bytes.len(), VARHDRSZ + SIZE_OF_HEADER + size_of_item(2));
    assert_eq!(bytes.len(), 32);

    // varlena header: len << 2 (long format).
    assert_eq!(&bytes[0..4], &((32u32) << 2).to_ne_bytes());

    // header: magic, type, nitems (native-endian u32 each)
    assert_eq!(&bytes[4..8], &STATS_NDISTINCT_MAGIC.to_ne_bytes());
    assert_eq!(&bytes[8..12], &STATS_NDISTINCT_TYPE_BASIC.to_ne_bytes());
    assert_eq!(&bytes[12..16], &1u32.to_ne_bytes());

    // item: ndistinct (f64), nattributes (i32), attributes (i16 each)
    assert_eq!(&bytes[16..24], &7.0f64.to_ne_bytes());
    assert_eq!(&bytes[24..28], &2i32.to_ne_bytes());
    assert_eq!(&bytes[28..30], &1i16.to_ne_bytes());
    assert_eq!(&bytes[30..32], &2i16.to_ne_bytes());
}

#[test]
fn serialize_deserialize_roundtrip() {
    let ctx = MemoryContext::new("ndist_test");
    let mcx = ctx.mcx();

    let original = ndist(vec![
        item(3.0, &[1, 2]),
        item(12.0, &[1, 2, 3]),
        item(5.0, &[4, 5]),
    ]);

    let bytes = statext_ndistinct_serialize(mcx, &original).unwrap();
    let back = statext_ndistinct_deserialize(Some(&bytes)).unwrap().unwrap();

    assert_eq!(back, original);
}

#[test]
fn deserialize_null_is_none() {
    assert!(statext_ndistinct_deserialize(None).unwrap().is_none());
}

#[test]
fn deserialize_rejects_short_header() {
    // 4-byte varlena header claiming total=8 but no room for SizeOfHeader.
    let mut bytes = vec![0u8; 8];
    set_varsize(&mut bytes, 8);
    let err = statext_ndistinct_deserialize(Some(&bytes)).unwrap_err();
    assert!(err.to_string().contains("invalid MVNDistinct size"));
}

#[test]
fn deserialize_rejects_bad_magic() {
    let total = VARHDRSZ + SIZE_OF_HEADER;
    let mut bytes = vec![0u8; total];
    set_varsize(&mut bytes, total);
    bytes[4..8].copy_from_slice(&0xDEAD_BEEFu32.to_ne_bytes());
    let err = statext_ndistinct_deserialize(Some(&bytes)).unwrap_err();
    assert!(err.to_string().contains("invalid ndistinct magic"));
}

#[test]
fn deserialize_rejects_bad_type() {
    let total = VARHDRSZ + SIZE_OF_HEADER;
    let mut bytes = vec![0u8; total];
    set_varsize(&mut bytes, total);
    bytes[4..8].copy_from_slice(&STATS_NDISTINCT_MAGIC.to_ne_bytes());
    bytes[8..12].copy_from_slice(&99u32.to_ne_bytes());
    let err = statext_ndistinct_deserialize(Some(&bytes)).unwrap_err();
    assert!(err.to_string().contains("invalid ndistinct type"));
}

#[test]
fn deserialize_rejects_zero_nitems() {
    let total = VARHDRSZ + SIZE_OF_HEADER;
    let mut bytes = vec![0u8; total];
    set_varsize(&mut bytes, total);
    bytes[4..8].copy_from_slice(&STATS_NDISTINCT_MAGIC.to_ne_bytes());
    bytes[8..12].copy_from_slice(&STATS_NDISTINCT_TYPE_BASIC.to_ne_bytes());
    bytes[12..16].copy_from_slice(&0u32.to_ne_bytes());
    let err = statext_ndistinct_deserialize(Some(&bytes)).unwrap_err();
    assert!(err
        .to_string()
        .contains("invalid zero-length item array in MVNDistinct"));
}

#[test]
fn deserialize_rejects_too_small_for_nitems() {
    // claims 3 items but body is only header-sized
    let total = VARHDRSZ + SIZE_OF_HEADER;
    let mut bytes = vec![0u8; total];
    set_varsize(&mut bytes, total);
    bytes[4..8].copy_from_slice(&STATS_NDISTINCT_MAGIC.to_ne_bytes());
    bytes[8..12].copy_from_slice(&STATS_NDISTINCT_TYPE_BASIC.to_ne_bytes());
    bytes[12..16].copy_from_slice(&3u32.to_ne_bytes());
    let err = statext_ndistinct_deserialize(Some(&bytes)).unwrap_err();
    assert!(err.to_string().contains("invalid MVNDistinct size"));
}

#[test]
fn num_combinations_matches_c() {
    // (1 << n) - (n + 1)
    assert_eq!(num_combinations(2), (1 << 2) - 3); // 1
    assert_eq!(num_combinations(3), (1 << 3) - 4); // 4
    assert_eq!(num_combinations(4), (1 << 4) - 5); // 11
}

#[test]
fn n_choose_k_matches_c() {
    assert_eq!(n_choose_k(4, 2), 6);
    assert_eq!(n_choose_k(5, 3), 10);
    assert_eq!(n_choose_k(6, 1), 6);
    assert_eq!(n_choose_k(6, 6), 1);
}

#[test]
fn generator_two_of_three() {
    // n=3, k=2: lexicographic 2-combinations:
    //   (0,1),(0,2),(1,2) — n_choose_k(3,2)=3.
    let ctx = MemoryContext::new("ndist_test");
    let mcx = ctx.mcx();

    let mut gen = CombinationGenerator::init(mcx, 3, 2).unwrap();
    let mut got: Vec<Vec<i32>> = Vec::new();
    while let Some(idx) = gen.next() {
        got.push(gen.combination(idx).to_vec());
    }

    let expected: Vec<Vec<i32>> = vec![vec![0, 1], vec![0, 2], vec![1, 2]];
    assert_eq!(got, expected);
    assert_eq!(gen.ncombinations, 3);
}

#[test]
fn generator_three_of_four() {
    // n=4, k=3: lexicographic 3-combinations:
    //   (0,1,2),(0,1,3),(0,2,3),(1,2,3) — n_choose_k(4,3)=4.
    let ctx = MemoryContext::new("ndist_test");
    let mcx = ctx.mcx();

    let mut gen = CombinationGenerator::init(mcx, 4, 3).unwrap();
    let mut got: Vec<Vec<i32>> = Vec::new();
    while let Some(idx) = gen.next() {
        got.push(gen.combination(idx).to_vec());
    }

    let expected: Vec<Vec<i32>> = vec![
        vec![0, 1, 2],
        vec![0, 1, 3],
        vec![0, 2, 3],
        vec![1, 2, 3],
    ];
    assert_eq!(got, expected);
}

#[test]
fn estimate_ndistinct_duj1() {
    // numer = numrows*d; denom = (numrows-f1) + f1*numrows/totalrows;
    // floor(numer/denom + 0.5). With totalrows=1000, numrows=100, d=40, f1=10:
    //   numer = 4000; denom = 90 + 10*100/1000 = 90 + 1 = 91
    //   ndistinct = 4000/91 = 43.956..., clamps fine, floor(44.456) = 44.
    let e = estimate_ndistinct(1000.0, 100, 40, 10);
    assert_eq!(e, 44.0);

    // Clamp-low: ndistinct is clamped up to d. With f1=0 the estimate is exactly
    // numrows (n*d/n = d) so it never drops below d; force a sub-d estimate via
    // the rounding path is not reachable normally, so verify the floor + the
    // never-below-d invariant on a plain case: d=10,f1=0 => exactly 10.
    let e2 = estimate_ndistinct(1000.0, 50, 10, 0);
    assert_eq!(e2, 10.0);

    // Clamp-high: cannot exceed totalrows.
    let e3 = estimate_ndistinct(30.0, 100, 90, 0);
    assert_eq!(e3, 30.0);
}

#[test]
fn pg_ndistinct_in_recv_disallowed() {
    let in_err = pg_ndistinct_in().unwrap_err();
    assert!(in_err.to_string().contains("cannot accept a value of type"));
    let recv_err = pg_ndistinct_recv().unwrap_err();
    assert!(recv_err
        .to_string()
        .contains("cannot accept a value of type"));
}

#[test]
fn pg_ndistinct_out_formats() {
    let ctx = MemoryContext::new("ndist_test");
    let mcx = ctx.mcx();

    let d = ndist(vec![item(7.0, &[1, 2]), item(42.0, &[3, 4, 5])]);
    let bytes = statext_ndistinct_serialize(mcx, &d).unwrap();
    let out = pg_ndistinct_out(&bytes).unwrap();

    // trailing NUL is the cstring terminator
    assert_eq!(*out.last().unwrap(), 0u8);
    let s = std::str::from_utf8(&out[..out.len() - 1]).unwrap();
    assert_eq!(s, r#"{"1, 2": 7, "3, 4, 5": 42}"#);
}
