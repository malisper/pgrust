//! Unit tests for the self-contained dependency logic: serialize/deserialize
//! round-trip (exact C byte layout) + the k-permutation generator.

use super::*;
use mcx::MemoryContext;

fn dep(degree: f64, attrs: &[AttrNumber]) -> Box<MVDependency> {
    Box::new(MVDependency {
        degree,
        nattributes: attrs.len() as AttrNumber,
        attributes: attrs.to_vec(),
    })
}

fn deps(items: Vec<Box<MVDependency>>) -> MVDependencies {
    MVDependencies {
        magic: STATS_DEPS_MAGIC,
        r#type: STATS_DEPS_TYPE_BASIC,
        ndeps: items.len() as u32,
        deps: items,
    }
}

#[test]
fn serialize_byte_layout() {
    let ctx = MemoryContext::new("statdeps_test");
    let mcx = ctx.mcx();

    let d = deps(vec![dep(0.5, &[1, 2])]);
    let bytes = statext_dependencies_serialize(mcx, &d).unwrap();

    // SizeOfHeader (12) + SizeOfItem(2) = 12 + (8 + 2*(1+2)) = 12 + 14 = 26.
    assert_eq!(bytes.len(), SIZE_OF_HEADER + size_of_item(2));
    assert_eq!(bytes.len(), 26);

    // header: magic, type, ndeps (native-endian u32 each)
    assert_eq!(&bytes[0..4], &STATS_DEPS_MAGIC.to_ne_bytes());
    assert_eq!(&bytes[4..8], &STATS_DEPS_TYPE_BASIC.to_ne_bytes());
    assert_eq!(&bytes[8..12], &1u32.to_ne_bytes());

    // item: degree (f64), nattributes (i16), attributes (i16 each)
    assert_eq!(&bytes[12..20], &0.5f64.to_ne_bytes());
    assert_eq!(&bytes[20..22], &2i16.to_ne_bytes());
    assert_eq!(&bytes[22..24], &1i16.to_ne_bytes());
    assert_eq!(&bytes[24..26], &2i16.to_ne_bytes());
}

#[test]
fn serialize_deserialize_roundtrip() {
    let ctx = MemoryContext::new("statdeps_test");
    let mcx = ctx.mcx();

    let original = deps(vec![
        dep(0.75, &[1, 2]),
        dep(1.0, &[1, 2, 3]),
        dep(0.125, &[4, 5]),
    ]);

    let bytes = statext_dependencies_serialize(mcx, &original).unwrap();
    let back = statext_dependencies_deserialize(mcx, Some(&bytes))
        .unwrap()
        .unwrap();

    assert_eq!(back, original);
}

#[test]
fn deserialize_null_is_none() {
    let ctx = MemoryContext::new("statdeps_test");
    let mcx = ctx.mcx();
    assert!(statext_dependencies_deserialize(mcx, None).unwrap().is_none());
}

#[test]
fn deserialize_rejects_short_header() {
    let ctx = MemoryContext::new("statdeps_test");
    let mcx = ctx.mcx();
    let err = statext_dependencies_deserialize(mcx, Some(&[0u8; 4])).unwrap_err();
    assert!(err.to_string().contains("invalid MVDependencies size"));
}

#[test]
fn deserialize_rejects_bad_magic() {
    let ctx = MemoryContext::new("statdeps_test");
    let mcx = ctx.mcx();
    let mut bytes = vec![0u8; SIZE_OF_HEADER];
    bytes[0..4].copy_from_slice(&0xDEAD_BEEFu32.to_ne_bytes());
    let err = statext_dependencies_deserialize(mcx, Some(&bytes)).unwrap_err();
    assert!(err.to_string().contains("invalid dependency magic"));
}

#[test]
fn deserialize_rejects_bad_type() {
    let ctx = MemoryContext::new("statdeps_test");
    let mcx = ctx.mcx();
    let mut bytes = vec![0u8; SIZE_OF_HEADER];
    bytes[0..4].copy_from_slice(&STATS_DEPS_MAGIC.to_ne_bytes());
    bytes[4..8].copy_from_slice(&99u32.to_ne_bytes());
    let err = statext_dependencies_deserialize(mcx, Some(&bytes)).unwrap_err();
    assert!(err.to_string().contains("invalid dependency type"));
}

#[test]
fn deserialize_rejects_zero_ndeps() {
    let ctx = MemoryContext::new("statdeps_test");
    let mcx = ctx.mcx();
    let mut bytes = vec![0u8; SIZE_OF_HEADER];
    bytes[0..4].copy_from_slice(&STATS_DEPS_MAGIC.to_ne_bytes());
    bytes[4..8].copy_from_slice(&STATS_DEPS_TYPE_BASIC.to_ne_bytes());
    bytes[8..12].copy_from_slice(&0u32.to_ne_bytes());
    let err = statext_dependencies_deserialize(mcx, Some(&bytes)).unwrap_err();
    assert!(err
        .to_string()
        .contains("invalid zero-length item array in MVDependencies"));
}

#[test]
fn deserialize_rejects_truncated_items() {
    let ctx = MemoryContext::new("statdeps_test");
    let mcx = ctx.mcx();
    // claims 3 deps but body is only header-sized
    let mut bytes = vec![0u8; SIZE_OF_HEADER];
    bytes[0..4].copy_from_slice(&STATS_DEPS_MAGIC.to_ne_bytes());
    bytes[4..8].copy_from_slice(&STATS_DEPS_TYPE_BASIC.to_ne_bytes());
    bytes[8..12].copy_from_slice(&3u32.to_ne_bytes());
    let err = statext_dependencies_deserialize(mcx, Some(&bytes)).unwrap_err();
    assert!(err.to_string().contains("invalid dependencies size"));
}

#[test]
fn generator_two_of_three() {
    // n=3, k=2: the C generator yields (a)->b style 2-permutations where the
    // first (k-1)=1 element is ascending and the last differs from it:
    //   (0,1),(0,2),(1,0),(1,2),(2,0),(2,1) — 6 dependencies.
    let ctx = MemoryContext::new("statdeps_test");
    let mcx = ctx.mcx();

    let mut gen = DependencyGenerator::init(mcx, 3, 2).unwrap();
    let mut got: Vec<Vec<AttrNumber>> = Vec::new();
    while let Some(idx) = gen.next() {
        got.push(gen.tuple(idx).to_vec());
    }

    let expected: Vec<Vec<AttrNumber>> = vec![
        vec![0, 1],
        vec![0, 2],
        vec![1, 0],
        vec![1, 2],
        vec![2, 0],
        vec![2, 1],
    ];
    assert_eq!(got, expected);
    assert_eq!(gen.ndependencies, 6);
}

#[test]
fn generator_three_of_three() {
    // n=3, k=3: first (k-1)=2 ascending, last not in {first two}:
    //   (0,1,2),(0,2,1),(1,2,0) — 3 dependencies.
    let ctx = MemoryContext::new("statdeps_test");
    let mcx = ctx.mcx();

    let mut gen = DependencyGenerator::init(mcx, 3, 3).unwrap();
    let mut got: Vec<Vec<AttrNumber>> = Vec::new();
    while let Some(idx) = gen.next() {
        got.push(gen.tuple(idx).to_vec());
    }

    let expected: Vec<Vec<AttrNumber>> =
        vec![vec![0, 1, 2], vec![0, 2, 1], vec![1, 2, 0]];
    assert_eq!(got, expected);
}

#[test]
fn clamp_probability_branches() {
    assert_eq!(clamp_probability(-0.5), 0.0);
    assert_eq!(clamp_probability(1.5), 1.0);
    assert_eq!(clamp_probability(0.3), 0.3);
}

#[test]
fn pg_dependencies_in_recv_disallowed() {
    let in_err = pg_dependencies_in().unwrap_err();
    assert!(in_err.to_string().contains("cannot accept a value of type"));
    let recv_err = pg_dependencies_recv().unwrap_err();
    assert!(recv_err
        .to_string()
        .contains("cannot accept a value of type"));
}

#[test]
fn pg_dependencies_out_formats() {
    let ctx = MemoryContext::new("statdeps_test");
    let mcx = ctx.mcx();

    let d = deps(vec![dep(0.5, &[1, 2]), dep(1.0, &[3, 4, 5])]);
    let bytes = statext_dependencies_serialize(mcx, &d).unwrap();
    let out = pg_dependencies_out(mcx, &bytes).unwrap();

    // trailing NUL is the cstring terminator
    assert_eq!(*out.last().unwrap(), 0u8);
    let s = std::str::from_utf8(&out[..out.len() - 1]).unwrap();
    assert_eq!(s, r#"{"1 => 2": 0.500000, "3, 4 => 5": 1.000000}"#);
}

#[test]
fn find_strongest_picks_widest_then_strongest() {
    // attnums available: {1,2,3}
    let d = vec![deps(vec![
        dep(0.6, &[1, 2]),     // 2 attrs
        dep(0.9, &[1, 2, 3]),  // 3 attrs, fully matched -> strongest
        dep(0.95, &[1, 4]),    // references 4 (not available) but only 2 attrs
    ])];
    let attnums = [1, 2, 3];
    let strongest = find_strongest_dependency(&d, &attnums).unwrap();
    assert_eq!(strongest, (0, 1)); // the 3-attr dependency
}

#[test]
fn combine_two_attr_dependency() {
    // One dependency a=>b (members [0,1]) degree f over selectivities P(a),P(b).
    // s1 = P(a); s2 = P(b). With P(a)=0.2, P(b)=0.5, f=0.8:
    //   s1 <= s2 -> attr_sel[1] = f + (1-f)*s2 = 0.8 + 0.2*0.5 = 0.9
    //   overall = attr_sel[0]*attr_sel[1] = 0.2 * 0.9 = 0.18
    let mut attr_sel = [0.2f64, 0.5f64];
    let members = vec![vec![0usize, 1usize]];
    let degrees = [0.8f64];
    let s = combine_dependency_selectivities(&mut attr_sel, &members, &degrees);
    assert!((s - 0.18).abs() < 1e-12, "got {s}");
}
