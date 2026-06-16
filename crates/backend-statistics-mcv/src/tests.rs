//! Unit tests for the self-contained MCV logic: the EXACT serialize/deserialize
//! byte layout (round-trip + field offsets + MAGIC/const verification), the
//! pure arithmetic helpers, and the error paths.
//!
//! The per-dimension `Datum`<->bytes codec, the LT-operator lookup and the
//! scalar comparison cross seams owned by the unported `backend-statistics-core`
//! owner; the tests install trivial by-value implementations (treating the
//! `Datum` word as a little-endian int8 of `typlen` significant bytes) so the
//! in-crate byte-layout logic can be exercised end-to-end.

use super::*;
use core_seam as cs;
use mcx::MemoryContext;
use types_core::AttrNumber;

/* ---------------------------------------------------------------------------
 * Seam install helpers — a trivial by-value int codec for the byte-layout tests.
 * ------------------------------------------------------------------------- */

fn install_byval_seams() {
    // Seams are process-global; install the trivial by-value codec exactly once
    // (tests run in parallel and `set` panics on a double install).
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // lt_opr: any non-zero oid for a "type that has an ordering operator".
        cs::mcv_lookup_lt_opr::set(|_typid| Ok(97 /* int8lt-ish, arbitrary non-zero */));

        // compare: treat the Datum word as a signed integer value.
        cs::mcv_compare_scalars_simple::set(|a, b, _lt, _coll| {
            let av = a.as_usize() as i64;
            let bv = b.as_usize() as i64;
            av.cmp(&bv) as i32
        });

        // value -> bytes: low `typlen` bytes of the Datum word (by-value path).
        cs::mcv_value_to_serialized_bytes::set(|mcx, value, typlen, typbyval| {
            assert!(typbyval, "test codec only handles by-value types");
            let n = typlen as usize;
            let word = value.as_usize().to_ne_bytes();
            let mut v: PgVec<u8> = PgVec::new_in(mcx);
            v.extend_from_slice(&word[..n]);
            Ok(v)
        });

        // bytes -> value: reassemble the Datum word from `typlen` bytes.
        cs::mcv_serialized_bytes_to_value::set(|_mcx, bytes, typlen, typbyval| {
            assert!(typbyval, "test codec only handles by-value types");
            let n = typlen as usize;
            let mut word = [0u8; size_of::<usize>()];
            word[..n].copy_from_slice(&bytes[..n]);
            Ok(Datum::from_usize(usize::from_ne_bytes(word)))
        });
    });
}

fn byval_stats(ndims: usize) -> Vec<McvDimStats> {
    (0..ndims)
        .map(|i| McvDimStats {
            attrtypid: 20 + i as u32, /* arbitrary, distinct */
            attrcollid: 0,
            typlen: 8,
            typbyval: true,
        })
        .collect()
}

fn item(freq: f64, base: f64, vals: &[(i64, bool)]) -> MCVItem {
    MCVItem {
        frequency: freq,
        base_frequency: base,
        isnull: vals.iter().map(|(_, n)| *n).collect(),
        values: vals
            .iter()
            .map(|(v, _)| Datum::from_usize(*v as usize))
            .collect(),
    }
}

fn list(ndims: usize, items: Vec<MCVItem>) -> MCVList {
    let mut types = [0u32; STATS_MAX_DIMENSIONS];
    for (i, t) in types.iter_mut().enumerate().take(ndims) {
        *t = 20 + i as u32;
    }
    MCVList {
        magic: STATS_MCV_MAGIC,
        r#type: STATS_MCV_TYPE_BASIC,
        nitems: items.len() as u32,
        ndimensions: ndims as AttrNumber,
        types,
        items,
    }
}

/* ---------------------------------------------------------------------------
 * Constant + macro verification (against statistics.h / mcv.c).
 * ------------------------------------------------------------------------- */

#[test]
fn constants_match_c() {
    assert_eq!(STATS_MCV_MAGIC, 0xE1A651C2);
    assert_eq!(STATS_MCV_TYPE_BASIC, 1);
    assert_eq!(STATS_MCVLIST_MAX_ITEMS, 10000);
    assert_eq!(STATS_MAX_DIMENSIONS, 8);

    // ITEM_SIZE(ndims) = ndims*(2+1) + 2*8
    assert_eq!(item_size(1), 3 + 16);
    assert_eq!(item_size(2), 6 + 16);

    // MinSizeOfMCVList = VARHDRSZ(4) + 4*3 + 2 = 18
    assert_eq!(min_size_of_mcvlist(), 4 + 12 + 2);
    assert_eq!(min_size_of_mcvlist(), 18);

    // SizeOfMCVList(ndims, nitems)
    // = (18 + 4*ndims) + 20*ndims + nitems*ITEM_SIZE(ndims)
    assert_eq!(size_of_mcvlist(1, 1), (18 + 4) + 20 + (3 + 16));
    assert_eq!(SIZEOF_DIMENSION_INFO, 20);
}

#[test]
fn result_macros() {
    // RESULT_MERGE
    assert!(result_merge(true, true, false)); // OR keeps true
    assert!(!result_merge(false, false, true)); // AND keeps false
    assert!(result_merge(false, true, true));
    assert!(result_merge(true, false, true));
    // RESULT_IS_FINAL
    assert!(result_is_final(true, true)); // OR + true is final
    assert!(!result_is_final(false, true));
    assert!(result_is_final(false, false)); // AND + false is final
    assert!(!result_is_final(true, false));
}

/* ---------------------------------------------------------------------------
 * get_mincount_for_mcv_list (mcv.c:147)
 * ------------------------------------------------------------------------- */

#[test]
fn mincount_div_by_zero_guard() {
    // n = N = 1: denom = 1 - 1 + 0.04*1*0 = 0 -> 0.0
    assert_eq!(get_mincount_for_mcv_list(1, 1.0), 0.0);
}

#[test]
fn mincount_formula() {
    // n=300, N=1000: numer = 300*700 = 210000;
    // denom = 700 + 0.04*300*999 = 700 + 11988 = 12688
    let got = get_mincount_for_mcv_list(300, 1000.0);
    let want = 210000.0 / 12688.0;
    assert!((got - want).abs() < 1e-9);
}

/* ---------------------------------------------------------------------------
 * mcv_combine_selectivities (mcv.c:2005)
 * ------------------------------------------------------------------------- */

#[test]
fn combine_selectivities() {
    // other_sel = clamp(simple - basesel); capped at 1 - totalsel; sel = clamp(mcv_sel + other)
    // simple=0.5, mcv_sel=0.2, basesel=0.1, totalsel=0.3
    // other = clamp(0.4)=0.4; 1-totalsel=0.7 -> stays 0.4; sel=clamp(0.6)=0.6
    assert!((mcv_combine_selectivities(0.5, 0.2, 0.1, 0.3) - 0.6).abs() < 1e-12);

    // other capped by 1 - totalsel
    // simple=1.0, basesel=0.0 -> other=clamp(1.0)=1.0, cap=1-0.3=0.7 -> 0.7; sel=clamp(0.2+0.7)=0.9
    assert!((mcv_combine_selectivities(1.0, 0.2, 0.0, 0.3) - 0.9).abs() < 1e-12);

    // negative simple-basesel clamps to 0
    // simple=0.1, basesel=0.5 -> other=clamp(-0.4)=0; sel=clamp(0.2+0)=0.2
    assert!((mcv_combine_selectivities(0.1, 0.2, 0.5, 0.3) - 0.2).abs() < 1e-12);

    // final sel clamps above 1
    assert_eq!(mcv_combine_selectivities(1.0, 1.0, 0.0, 0.0), 1.0);
}

/* ---------------------------------------------------------------------------
 * serialize / deserialize byte layout
 * ------------------------------------------------------------------------- */

#[test]
fn serialize_header_layout() {
    install_byval_seams();
    let ctx = MemoryContext::new("mcv_test");
    let mcx = ctx.mcx();

    // 1 dim, 1 item, non-null value 42.
    let l = list(1, vec![item(0.5, 0.25, &[(42, false)])]);
    let bytes = statext_mcv_serialize(mcx, &l, &byval_stats(1)).unwrap();

    // SizeOfMCVList already includes VARHDRSZ; add nbytes (8 for the single
    // by-value value) for the dedup'd value array.
    let expected = size_of_mcvlist(1, 1) + 8;
    assert_eq!(bytes.len(), expected);

    // varlena long header: (len << 2)
    let hdr = u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    assert_eq!((hdr >> 2) as usize, expected);

    // body offset 0: magic, type, nitems (u32 ne each), ndimensions (i16)
    let b = &bytes[VARHDRSZ..];
    assert_eq!(&b[0..4], &STATS_MCV_MAGIC.to_ne_bytes());
    assert_eq!(&b[4..8], &STATS_MCV_TYPE_BASIC.to_ne_bytes());
    assert_eq!(&b[8..12], &1u32.to_ne_bytes());
    assert_eq!(&b[12..14], &1i16.to_ne_bytes());
    // types[0]
    assert_eq!(&b[14..18], &20u32.to_ne_bytes());
}

#[test]
fn roundtrip_byval_single_dim() {
    install_byval_seams();
    let ctx = MemoryContext::new("mcv_test");
    let mcx = ctx.mcx();

    let original = list(
        1,
        vec![
            item(0.5, 0.25, &[(42, false)]),
            item(0.3, 0.10, &[(7, false)]),
            item(0.2, 0.05, &[(0, true)]), // NULL value
        ],
    );
    let bytes = statext_mcv_serialize(mcx, &original, &byval_stats(1)).unwrap();
    let back = statext_mcv_deserialize(mcx, Some(&bytes)).unwrap().unwrap();

    assert_eq!(back.magic, STATS_MCV_MAGIC);
    assert_eq!(back.r#type, STATS_MCV_TYPE_BASIC);
    assert_eq!(back.nitems, 3);
    assert_eq!(back.ndimensions, 1);
    assert_eq!(back.types[0], 20);
    assert_eq!(back.items.len(), 3);

    for (a, b) in original.items.iter().zip(back.items.iter()) {
        assert_eq!(a.frequency, b.frequency);
        assert_eq!(a.base_frequency, b.base_frequency);
        assert_eq!(a.isnull, b.isnull);
        // values only meaningful for non-NULL dims
        for d in 0..1 {
            if !a.isnull[d] {
                assert_eq!(a.values[d], b.values[d]);
            }
        }
    }
}

#[test]
fn roundtrip_byval_multi_dim_dedup() {
    install_byval_seams();
    let ctx = MemoryContext::new("mcv_test");
    let mcx = ctx.mcx();

    // 2 dims, repeated values across items (exercise dedup + index lookup).
    let original = list(
        2,
        vec![
            item(0.4, 0.2, &[(10, false), (100, false)]),
            item(0.3, 0.1, &[(10, false), (200, false)]), // dim0 value 10 repeats
            item(0.2, 0.05, &[(20, false), (100, false)]), // dim1 value 100 repeats
            item(0.1, 0.02, &[(0, true), (300, false)]),   // dim0 NULL
        ],
    );
    let bytes = statext_mcv_serialize(mcx, &original, &byval_stats(2)).unwrap();
    let back = statext_mcv_deserialize(mcx, Some(&bytes)).unwrap().unwrap();

    assert_eq!(back.nitems, 4);
    assert_eq!(back.ndimensions, 2);
    for (a, b) in original.items.iter().zip(back.items.iter()) {
        assert_eq!(a.frequency, b.frequency);
        assert_eq!(a.base_frequency, b.base_frequency);
        assert_eq!(a.isnull, b.isnull);
        for d in 0..2 {
            if !a.isnull[d] {
                assert_eq!(a.values[d], b.values[d]);
            }
        }
    }
}

/* ---------------------------------------------------------------------------
 * deserialize error paths (mcv.c:1022-1074, 1091, 1123)
 * ------------------------------------------------------------------------- */

#[test]
fn deserialize_none_is_none() {
    let ctx = MemoryContext::new("mcv_test");
    let mcx = ctx.mcx();
    assert!(statext_mcv_deserialize(mcx, None).unwrap().is_none());
}

#[test]
fn deserialize_too_short() {
    let ctx = MemoryContext::new("mcv_test");
    let mcx = ctx.mcx();
    // a long-header varlena claiming size 8 (< MinSizeOfMCVList = 18)
    let mut buf = vec![0u8; 8];
    set_varsize(&mut buf, 8);
    let err = statext_mcv_deserialize(mcx, Some(&buf)).unwrap_err();
    assert!(format!("{err:?}").contains("invalid MCV size"));
}

#[test]
fn deserialize_bad_magic() {
    install_byval_seams();
    let ctx = MemoryContext::new("mcv_test");
    let mcx = ctx.mcx();
    let l = list(1, vec![item(1.0, 1.0, &[(1, false)])]);
    let mut bytes = statext_mcv_serialize(mcx, &l, &byval_stats(1)).unwrap();
    // corrupt the magic (first body u32 after the 4-byte varlena header)
    bytes[VARHDRSZ] ^= 0xFF;
    let err = statext_mcv_deserialize(mcx, Some(&bytes)).unwrap_err();
    assert!(format!("{err:?}").contains("invalid MCV magic"));
}

#[test]
fn deserialize_bad_type() {
    install_byval_seams();
    let ctx = MemoryContext::new("mcv_test");
    let mcx = ctx.mcx();
    let mut l = list(1, vec![item(1.0, 1.0, &[(1, false)])]);
    l.r#type = 99; // invalid type, but keep magic valid
    let bytes = statext_mcv_serialize(mcx, &l, &byval_stats(1)).unwrap();
    let err = statext_mcv_deserialize(mcx, Some(&bytes)).unwrap_err();
    assert!(format!("{err:?}").contains("invalid MCV type"));
}

/* ---------------------------------------------------------------------------
 * type I/O surface (mcv.c:1471-1525)
 * ------------------------------------------------------------------------- */

#[test]
fn in_and_recv_disallowed() {
    let e1 = pg_mcv_list_in().unwrap_err();
    let e2 = pg_mcv_list_recv().unwrap_err();
    assert!(format!("{e1:?}").contains("cannot accept a value of type pg_mcv_list"));
    assert!(format!("{e2:?}").contains("cannot accept a value of type pg_mcv_list"));
}

#[test]
fn send_delegates_to_byteasend() {
    let ctx = MemoryContext::new("mcv_test");
    let mcx = ctx.mcx();
    // byteasend wraps the bytes in a varlena; just check it returns the payload.
    let payload = [1u8, 2, 3, 4];
    let out = pg_mcv_list_send(mcx, &payload).unwrap();
    // byteasend returns a verbatim copy of the (already-framed) payload.
    assert_eq!(&out[..], &payload);
}
