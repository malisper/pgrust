//! SEAL-SPEED-2 D2 — the inherited-dictionary arm's canonical-form proof
//! at unit grain: a column sealed WITH the side channel (merge/remap) must
//! elect identically and emit byte-identical dict sections + code streams
//! to the same column sealed WITHOUT it (the rebuild path's observe walk).
//! The rig's dirsha gates prove the same law at bank grain (the serial arm
//! rebuilds by design); these pin it in-tree with multi-source overlap,
//! unreferenced entries, PLAIN-fallback hybrid rows, and the cap demotion.

use crate::dict::TextSemantics;
use crate::elect::{
    CodecCandidates, ColumnPosture, DictPolicy, ElectPlan, FullElectInput, FullElection,
};
use crate::ingest::{ColBuffer, DictEntries, DICT_ROW_PLAIN};
use crate::seal::band_count_u64;
use pgrc2_format::class::{ColSchema, CollationClass, StorageClass, TypeSemantics};
use pgrc2_format::geom::{self, GranuleGrain, FRAME_VALUES};
use std::sync::Arc;

struct TestDict(Vec<Vec<u8>>);

impl DictEntries for TestDict {
    fn entry_count(&self) -> u32 {
        self.0.len() as u32
    }
    fn entry(&self, code: u32) -> &[u8] {
        &self.0[code as usize]
    }
}

fn text_schema() -> ColSchema {
    ColSchema {
        attno: 1,
        class: StorageClass::VarlenaVerbatim,
        typlen: -1,
        typbyval: false,
        typalign: b'i',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::TextCollated,
    }
}

fn dict_cands(ndv_cap: u64) -> CodecCandidates {
    CodecCandidates::new(ColumnPosture {
        cold: true,
        dict: Some(DictPolicy {
            ndv_cap,
            exec_ok: true,
            sem: TextSemantics::Utf8Chars,
        }),
        ..Default::default()
    })
}

/// Run the full-registry election exactly the way the seal driver does
/// (default-grain granule inputs over the one materialized flat array).
fn elect(col: &ColBuffer, cands: &CodecCandidates) -> FullElection {
    let stats = col.stream_stats();
    let rows = col.rows();
    let shape = crate::elect::ExtentShape {
        extent_count: band_count_u64(rows),
        frame_count: rows.div_ceil(FRAME_VALUES as u64),
    };
    let flat = col.part_ptrs();
    let dgc = geom::granule_count(rows);
    let ginputs: Vec<pgrc2_format::abi::EncodeInput<'_>> = (0..dgc)
        .map(|g| {
            let start = g as usize * GranuleGrain::DEFAULT.rows() as usize;
            let rows_g = geom::rows_in_granule(rows, g);
            col.encode_input(g, GranuleGrain::DEFAULT, &flat[start..start + rows_g as usize], rows_g)
        })
        .collect();
    let fin = FullElectInput {
        attno: 1,
        path_ord: 0,
        stats: &stats,
        shape: &shape,
        granules: &ginputs,
        col: Some(col),
    };
    use crate::elect::CandidateSource;
    cands.elect_full(&fin).expect("full source answers").expect("election ok")
}

/// The shared fixture: two overlapping source dictionaries (both carrying
/// UNREFERENCED entries), a code stream alternating sources, and a plain
/// hybrid tail whose values partly duplicate dict entries.
fn fixture() -> (Vec<Arc<dyn DictEntries>>, Vec<(u64, Vec<u8>)>) {
    let d0 = Arc::new(TestDict(vec![
        b"amber".to_vec(),
        b"citrus".to_vec(),
        b"unreferenced-zero".to_vec(), // never used by any row
        b"delta".to_vec(),
        b"".to_vec(), // empty string entry
    ])) as Arc<dyn DictEntries>;
    let d1 = Arc::new(TestDict(vec![
        b"citrus".to_vec(), // overlaps d0 code 1 — must collapse in the merge
        b"bravo".to_vec(),
        b"echo".to_vec(),
        b"unreferenced-one".to_vec(), // never used
    ])) as Arc<dyn DictEntries>;
    // (slot, payload): slot = (source << 32) | code, or DICT_ROW_PLAIN.
    let mut rows: Vec<(u64, Vec<u8>)> = Vec::new();
    let d0e = |c: u32| -> Vec<u8> {
        match c {
            0 => b"amber".to_vec(),
            1 => b"citrus".to_vec(),
            3 => b"delta".to_vec(),
            4 => Vec::new(),
            _ => unreachable!(),
        }
    };
    let d1e = |c: u32| -> Vec<u8> {
        match c {
            0 => b"citrus".to_vec(),
            1 => b"bravo".to_vec(),
            2 => b"echo".to_vec(),
            _ => unreachable!(),
        }
    };
    for i in 0..9000u32 {
        match i % 6 {
            0 => rows.push((0u64 << 32 | 0, d0e(0))),
            1 => rows.push((0u64 << 32 | 1, d0e(1))),
            2 => rows.push((1u64 << 32 | 0, d1e(0))),
            3 => rows.push((1u64 << 32 | 1, d1e(1))),
            4 => rows.push((0u64 << 32 | 3, d0e(3))),
            _ => rows.push((1u64 << 32 | 2, d1e(2))),
        }
    }
    // Empty-string dict rows (code 4 of d0).
    for _ in 0..64 {
        rows.push((0u64 << 32 | 4, Vec::new()));
    }
    // PLAIN hybrid tail: new values + values duplicating dict entries (the
    // merge must collapse those too).
    for i in 0..500u32 {
        let v: Vec<u8> = match i % 3 {
            0 => format!("plain-{:04}", i % 40).into_bytes(),
            1 => b"citrus".to_vec(),
            _ => b"plain-solo".to_vec(),
        };
        rows.push((DICT_ROW_PLAIN, v));
    }
    (vec![d0, d1], rows)
}

fn build_cols(
    sources: &[Arc<dyn DictEntries>],
    rows: &[(u64, Vec<u8>)],
) -> (ColBuffer, ColBuffer) {
    let mut rebuild = ColBuffer::new(text_schema());
    let mut inherit = ColBuffer::new(text_schema());
    for (_, payload) in rows {
        rebuild.append_varlena_payload(payload).expect("append");
        inherit.append_varlena_payload(payload).expect("append");
    }
    inherit
        .attach_dict_side(sources.to_vec(), rows.iter().map(|(s, _)| *s).collect())
        .expect("attach");
    (rebuild, inherit)
}

#[test]
fn inherit_equals_rebuild_multisource_hybrid() {
    let (sources, rows) = fixture();
    let (rebuild, inherit) = build_cols(&sources, &rows);
    assert!(inherit.dict_side().is_some(), "side channel covers the part");
    let cands = dict_cands(1 << 20);
    let a = elect(&rebuild, &cands);
    let b = elect(&inherit, &cands);
    assert_eq!(a.encoding, b.encoding, "same election");
    assert_eq!(
        a.encoding,
        pgrc2_format::enc::EncodingId::DictCodes.as_u16(),
        "fixture must dict-elect"
    );
    assert_eq!(a.width, b.width, "same max code width");
    assert_eq!(a.extra_flags, b.extra_flags, "same DICT_EXEC posture");
    assert_eq!(a.witness, b.witness, "same priced witness");
    let (ElectPlan::Dict(da), ElectPlan::Dict(db)) = (&a.plan, &b.plan) else {
        panic!("both plans must be dict");
    };
    assert_eq!(da.entry_count, db.entry_count, "same NDV");
    assert_eq!(
        da.images.index_section, db.images.index_section,
        "byte-identical DictIndex (the canonical-form law)"
    );
    assert_eq!(
        da.images.payload_section, db.images.payload_section,
        "byte-identical DictPayload"
    );
    assert_eq!(da.codes, db.codes, "identical translated code streams");
}

#[test]
fn inherit_ndv_cap_demotes_identically() {
    let (sources, rows) = fixture();
    let (rebuild, inherit) = build_cols(&sources, &rows);
    // Cap below the fixture's NDV: both arms must take the same demotion
    // (dict refused → the FSST/verbatim loser path) with equal outcomes.
    let cands = dict_cands(4);
    let a = elect(&rebuild, &cands);
    let b = elect(&inherit, &cands);
    assert_ne!(
        a.encoding,
        pgrc2_format::enc::EncodingId::DictCodes.as_u16(),
        "cap must demote the dict arm"
    );
    assert_eq!(a.encoding, b.encoding, "same loser-path election");
    assert_eq!(a.witness, b.witness, "same loser-path witness");
}

#[test]
fn inherit_out_of_range_code_refuses_typed() {
    let (sources, mut rows) = fixture();
    // Corrupt one slot: code 99 does not exist in source 0.
    rows[7].0 = 99;
    let mut col = ColBuffer::new(text_schema());
    for (_, payload) in &rows {
        col.append_varlena_payload(payload).expect("append");
    }
    col.attach_dict_side(sources, rows.iter().map(|(s, _)| *s).collect())
        .expect("attach");
    let cands = dict_cands(1 << 20);
    let stats = col.stream_stats();
    let nrows = col.rows();
    let shape = crate::elect::ExtentShape {
        extent_count: band_count_u64(nrows),
        frame_count: nrows.div_ceil(FRAME_VALUES as u64),
    };
    let flat = col.part_ptrs();
    let dgc = geom::granule_count(nrows);
    let ginputs: Vec<pgrc2_format::abi::EncodeInput<'_>> = (0..dgc)
        .map(|g| {
            let start = g as usize * GranuleGrain::DEFAULT.rows() as usize;
            let rows_g = geom::rows_in_granule(nrows, g);
            col.encode_input(g, GranuleGrain::DEFAULT, &flat[start..start + rows_g as usize], rows_g)
        })
        .collect();
    let fin = FullElectInput {
        attno: 1,
        path_ord: 0,
        stats: &stats,
        shape: &shape,
        granules: &ginputs,
        col: Some(&col),
    };
    use crate::elect::CandidateSource;
    let r = cands.elect_full(&fin).expect("source answers");
    assert!(r.is_err(), "out-of-range inherited code must refuse typed");
}

#[test]
fn splice_drops_channel_when_a_chunk_lacks_it() {
    let (sources, rows) = fixture();
    // Chunk A carries the channel; chunk B does not.
    let mut a = ColBuffer::new(text_schema());
    for (_, payload) in rows.iter().take(4096) {
        a.append_varlena_payload(payload).expect("append");
    }
    a.attach_dict_side(
        sources.clone(),
        rows.iter().take(4096).map(|(s, _)| *s).collect(),
    )
    .expect("attach");
    let mut b = ColBuffer::new(text_schema());
    for (_, payload) in rows.iter().skip(4096).take(4096) {
        b.append_varlena_payload(payload).expect("append");
    }
    let mut part = ColBuffer::new(text_schema());
    part.splice_chunk(&a).expect("splice a");
    assert!(part.dict_side().is_some(), "first chunk's channel adopted");
    part.splice_chunk(&b).expect("splice b");
    assert!(
        part.dict_side().is_none(),
        "coverage law: one channel-free chunk drops the part's channel"
    );
}

#[test]
fn splice_translates_source_ordinals() {
    let (sources, rows) = fixture();
    // Chunk A uses only source 1 (locally ordinal 0); chunk B uses both.
    let a_rows: Vec<(u64, Vec<u8>)> = (0..2048)
        .map(|i| {
            let c = [0u32, 1, 2][i % 3];
            let payload = match c {
                0 => b"citrus".to_vec(),
                1 => b"bravo".to_vec(),
                _ => b"echo".to_vec(),
            };
            (u64::from(c), payload) // source LOCAL ordinal 0 = global d1
        })
        .collect();
    let mut a = ColBuffer::new(text_schema());
    for (_, payload) in &a_rows {
        a.append_varlena_payload(payload).expect("append");
    }
    a.attach_dict_side(
        vec![sources[1].clone()],
        a_rows.iter().map(|(s, _)| *s).collect(),
    )
    .expect("attach");
    let mut b = ColBuffer::new(text_schema());
    for (_, payload) in rows.iter().take(2048) {
        b.append_varlena_payload(payload).expect("append");
    }
    b.attach_dict_side(
        sources.clone(),
        rows.iter().take(2048).map(|(s, _)| *s).collect(),
    )
    .expect("attach");

    let mut part = ColBuffer::new(text_schema());
    part.splice_chunk(&a).expect("splice a");
    part.splice_chunk(&b).expect("splice b");
    let side = part.dict_side().expect("channel covers");
    // Part source table: [d1 (from a), d0 (new in b)]; b's rows referencing
    // its local source 0 (d0) must translate to part ordinal 1, and its
    // local source 1 (d1) back to part ordinal 0.
    assert_eq!(side.sources.len(), 2, "deduped by identity");
    assert!(Arc::ptr_eq(&side.sources[0], &sources[1]));
    assert!(Arc::ptr_eq(&side.sources[1], &sources[0]));
    for (k, (slot, payload)) in rows.iter().take(2048).enumerate() {
        let got = side.rows[2048 + k];
        if *slot == DICT_ROW_PLAIN {
            assert_eq!(got, DICT_ROW_PLAIN);
            continue;
        }
        let (src, code) = ((slot >> 32) as usize, *slot as u32);
        let want_src = if src == 0 { 1u64 } else { 0u64 };
        assert_eq!(got, (want_src << 32) | u64::from(code), "row {k}");
        // And the translated entry still names the row's value.
        let e = side.sources[(got >> 32) as usize].entry(got as u32);
        assert_eq!(e, payload.as_slice(), "row {k} entry/value equality");
    }
}

#[test]
fn permuted_carries_side_channel() {
    let (sources, rows) = fixture();
    let (_, inherit) = build_cols(&sources, &rows);
    let n = inherit.rows() as u32;
    // Reverse permutation (the cluster-sort apply's currency).
    let perm: Vec<u32> = (0..n).rev().collect();
    let out = inherit.permuted(&perm).expect("permute");
    let side = out.dict_side().expect("channel follows the permutation");
    for (k, &r) in perm.iter().enumerate() {
        assert_eq!(side.rows[k], rows[r as usize].0, "slot {k} follows its row");
    }
}
