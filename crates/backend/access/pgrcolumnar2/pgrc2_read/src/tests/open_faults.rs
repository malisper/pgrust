//! The O(streams-touched) born-RED gate (§5 M3-F): opening one column of a
//! wide shredded part faults only that column's streams, witnessed by the
//! part's fault log. The 2.9 ms/MiB cold-open slope is the law's reason —
//! never fault sections you weren't asked for (spec §6.2).
//!
//! Two teeth:
//! - **fires**: `prefault_all_sections` (a real API — cache warming) makes
//!   the same gate check FAIL, proving the witness detects a whole-part
//!   fault;
//! - **ran**: the gate check REJECTS an empty/short fault log, so a witness
//!   that never recorded cannot pass.

use pgrc2_format::abi::{ByteArena, DecodeOut};
use pgrc2_format::meta::MetaProbe;
use pgrc2_format::part::{SectionKind, StreamRole};

use crate::cursor::{reference_binding_leaked, StreamCursor};
use crate::openpart::{FaultEntry, FaultTag};
use crate::testpart::{build_part, seq_i64_col, text_col, BuiltPart, PartSpec};

use super::{open_built, ArenaBuf};

/// The wide shredded part: a root varlena column with six shred lanes, a
/// plain i64 column, a nulled text column — stats/psma/bloom/ndv on all of
/// them — plus part-level path table, sort key, sidecar dir.
fn wide_part() -> BuiltPart {
    let rows = 17_000u64; // 3 granules, short last
    let mut cols = Vec::new();
    let mut root = text_col(1, rows, None);
    root.with_stats = true;
    root.with_psma = true;
    root.with_bloom = true;
    root.with_ndv = true;
    cols.push(root);
    for p in 1..=6u32 {
        let mut lane = text_col(1, rows, None);
        lane.path_ord = p;
        lane.with_stats = true;
        cols.push(lane);
    }
    let mut ints = seq_i64_col(2, rows);
    ints.with_stats = true;
    ints.with_psma = true;
    cols.push(ints);
    let mut nulled = text_col(3, rows, Some(5));
    nulled.with_stats = true;
    cols.push(nulled);
    let mut spec = PartSpec::new(rows, cols);
    spec.path_table = (1..=6).map(|p| format!("$.lane{p}")).collect();
    spec.with_sort_key = true;
    spec.with_sidecar_dir = true;
    build_part(&spec)
}

/// The gate: the log shows a completed open (4 fixed faults) and touches no
/// stream payload outside `allowed` (attno, path_ord) pairs and no aux
/// metadata sections at all. Returns false on an empty/short log (tooth 2 —
/// a witness that did not run cannot pass).
fn only_touches(faults: &[FaultEntry], allowed: &[(u32, u32)]) -> bool {
    if faults.len() < 4 {
        return false;
    }
    for f in faults {
        match f.tag {
            FaultTag::Tail | FaultTag::Footer | FaultTag::SectionTable | FaultTag::Header => {}
            FaultTag::ListedSection {
                kind,
                attno,
                path_ord,
            } => {
                if kind == SectionKind::StreamDir.as_u16() {
                    continue;
                }
                if kind == SectionKind::Stream.as_u16() {
                    if !allowed.contains(&(attno, path_ord)) {
                        return false;
                    }
                    continue;
                }
                // Stats/Psma/Bloom/Ndv/PathTable/SortKey/SidecarDir: never
                // faulted by a column read.
                return false;
            }
            FaultTag::StreamExtent {
                attno, path_ord, ..
            } => {
                if !allowed.contains(&(attno, path_ord)) {
                    return false;
                }
            }
        }
    }
    true
}

fn stream_faults(faults: &[FaultEntry]) -> Vec<(u32, u32, u8, u32)> {
    faults
        .iter()
        .filter_map(|f| match f.tag {
            FaultTag::StreamExtent {
                attno,
                path_ord,
                role,
                extent,
            } => Some((attno, path_ord, role, extent)),
            _ => None,
        })
        .collect()
}

#[test]
fn open_faults_exactly_the_fixed_set() {
    let b = wide_part();
    let part = open_built(&b, 100, 1);
    let tags: Vec<_> = part.faults().iter().map(|f| f.tag).collect();
    assert_eq!(
        tags,
        vec![
            FaultTag::Tail,
            FaultTag::Footer,
            FaultTag::SectionTable,
            FaultTag::Header
        ],
        "open must fault tail, footer, section table, header — nothing else"
    );
}

#[test]
fn one_column_read_faults_only_that_columns_streams() {
    let b = wide_part();
    let part = open_built(&b, 101, 1);
    let binding = reference_binding_leaked();
    let mut cur = StreamCursor::open(part.clone(), binding, 2, 0).expect("cursor attno 2");
    // Cursor open adds exactly the StreamDir section.
    let after_open: Vec<_> = part.faults();
    assert_eq!(after_open.len(), 5);
    assert!(matches!(
        after_open[4].tag,
        FaultTag::ListedSection { kind, .. } if kind == SectionKind::StreamDir.as_u16()
    ));

    let mut datums = vec![0u64; 8192];
    let mut ab = ArenaBuf::new(1 << 20);
    let mut out = DecodeOut {
        datums: &mut datums,
        arena: ByteArena::new(ab.bytes_mut()),
    };
    cur.decode_full(1, &mut out).expect("decode granule 1");
    let faults = part.faults();
    // Exactly one new fault: attno 2's values extent.
    assert_eq!(faults.len(), 6);
    assert_eq!(
        stream_faults(&faults),
        vec![(2, 0, StreamRole::Values.as_u8(), 0)]
    );
    assert!(only_touches(&faults, &[(2, 0)]), "the gate passes a clean read");

    // Byte accounting: the faulted total is the fixed open set + StreamDir +
    // the one values section — nothing proportional to the part.
    let sd = b
        .find_section(SectionKind::StreamDir, 0, 0)
        .expect("stream dir");
    let vs = b
        .find_stream_section(2, 0, StreamRole::Values)
        .expect("attno2 values");
    let fixed: u64 = 16 + 96 + 64 + faults[2].len; // tail + footer + header + section table
    let expected_bytes = fixed + b.sections[sd].len + b.sections[vs].len;
    let total: u64 = faults.iter().map(|f| f.len).sum();
    assert_eq!(total, expected_bytes, "byte-exact fault accounting");
    assert!(
        total < b.bytes.len() as u64 / 4,
        "one narrow column must not fault a meaningful fraction of the part \
         ({} of {})",
        total,
        b.bytes.len()
    );
}

#[test]
fn nulled_column_adds_exactly_the_validity_extent() {
    let b = wide_part();
    let part = open_built(&b, 102, 1);
    let binding = reference_binding_leaked();
    let mut cur = StreamCursor::open(part.clone(), binding, 3, 0).expect("cursor attno 3");
    let mut datums = vec![0u64; 8192];
    let mut ab = ArenaBuf::new(4 << 20);
    let mut out = DecodeOut {
        datums: &mut datums,
        arena: ByteArena::new(ab.bytes_mut()),
    };
    cur.decode_full(0, &mut out).expect("decode");
    let sf = stream_faults(&part.faults());
    assert_eq!(
        sf,
        vec![
            (3, 0, StreamRole::Values.as_u8(), 0),
            (3, 0, StreamRole::Validity.as_u8(), 0),
        ],
        "values + validity, in fault order"
    );
    assert!(only_touches(&part.faults(), &[(3, 0)]));
}

#[test]
fn validity_face_faults_only_the_validity_extent() {
    // The per-face policy pin: a bitmap question never touches value bytes.
    let b = wide_part();
    let part = open_built(&b, 103, 1);
    let binding = reference_binding_leaked();
    let mut cur = StreamCursor::open(part.clone(), binding, 3, 0).expect("cursor");
    let mut mask = vec![0u64; 8192 / 64];
    cur.validity(0, &mut mask).expect("validity face");
    let sf = stream_faults(&part.faults());
    assert_eq!(
        sf,
        vec![(3, 0, StreamRole::Validity.as_u8(), 0)],
        "validity face must not fault the values extent"
    );
}

#[test]
fn meta_probe_faults_values_and_validity_only() {
    let b = wide_part();
    let part = open_built(&b, 104, 1);
    let binding = reference_binding_leaked();
    let mut cur = StreamCursor::open(part.clone(), binding, 3, 0).expect("cursor");
    cur.meta_probe(0, &MetaProbe::NonNullCount).expect("meta");
    let sf = stream_faults(&part.faults());
    assert_eq!(
        sf,
        vec![
            (3, 0, StreamRole::Values.as_u8(), 0),
            (3, 0, StreamRole::Validity.as_u8(), 0),
        ]
    );
    assert!(only_touches(&part.faults(), &[(3, 0)]));
}

#[test]
fn born_red_tooth_1_gate_fires_on_whole_part_fault() {
    // The witness must DETECT an eager whole-part reader: prefault
    // everything and prove the same gate check now fails.
    let b = wide_part();
    let part = open_built(&b, 105, 1);
    let n = part.prefault_all_sections().expect("prefault");
    assert!(n as usize == b.sections.len(), "prefault covered the part");
    assert!(
        !only_touches(&part.faults(), &[(2, 0)]),
        "the O(streams-touched) gate MUST fire on a whole-part fault"
    );
}

#[test]
fn born_red_tooth_2_gate_rejects_a_witness_that_did_not_run() {
    assert!(
        !only_touches(&[], &[(2, 0)]),
        "an empty fault log (witness never ran) must not pass the gate"
    );
}
