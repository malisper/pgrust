//! SB-10 byte-bounded granule geometry, end to end through the QA corpus
//! rig (lanev4; OD-13 RULED): a jb_widerow-shaped fixture (~8 KiB values)
//! seals at a NON-DEFAULT grain through the real writer, publishes through
//! the real manifest path (whose PartRecord geometry echo stays the frozen
//! default-grain rows-echo — the footer carries the grain truth), and
//! decodes back byte-exactly through the FULL codec binding (the hot
//! verbatim kernels' gcount-driven addressing arm).

use pgrc2_format::part::{FooterFixed, PartTail};
use pgrc2_qa::corpus::{build_fixture, verify_manifest, Fixture, OracleVal};
use pgrc2_qa::{int8_col, text_col};
use pgrc2_write::writer::PartCutPolicy;

fn wide_payload(i: u64) -> Vec<u8> {
    let mut v = vec![0u8; 8 * 1024];
    for (k, b) in v.iter_mut().enumerate() {
        *b = (i.wrapping_mul(131).wrapping_add(k as u64) & 0xFF) as u8;
    }
    v
}

fn wide_fixture(rows: u64) -> Fixture {
    Fixture {
        name: "widerow_grain_ladder",
        dir: "/qa/t9001".to_string(),
        spc: 1663,
        db: 5,
        relfilenumber: 9001,
        schema: vec![int8_col(1), text_col(2)],
        oracle: vec![
            (0..rows)
                .map(|i| {
                    if i % 11 == 3 {
                        None
                    } else {
                        Some(OracleVal::Word(i.wrapping_mul(7)))
                    }
                })
                .collect(),
            (0..rows)
                .map(|i| Some(OracleVal::Bytes(wide_payload(i))))
                .collect(),
        ],
        plans: Vec::new(),
        policy: PartCutPolicy::default(),
    }
}

#[test]
fn widerow_fixture_seals_at_grain_1024_and_round_trips() {
    // 2,048 rows × ~8 KiB ≈ 16 MiB of value bytes: only the 1024 ladder
    // floor clears the provisional 10 MiB per-(column,granule) bound.
    let rows = 2_048u64;
    let built = build_fixture(wide_fixture(rows));

    // Footer carries the elected grain; since the M3-L2 SB-10 ripple the
    // manifest record ECHOES it (granule_rows + grain-true geometry) —
    // no more default-grain fiction for non-default parts.
    let part_bytes = built.files.get("part-0.pgrc2").expect("part file");
    let tail = PartTail::decode_at_eof(part_bytes).expect("tail");
    let footer =
        FooterFixed::decode(&part_bytes[tail.footer_off as usize..]).expect("footer");
    assert_eq!(footer.granule_rows, 1_024, "SB-10 elected grain");
    assert_eq!(footer.granule_count, 2);
    let rec = &built.manifest.parts[0];
    assert_eq!(rec.rows, rows);
    assert_eq!(rec.granule_rows, 1_024, "manifest echoes the footer grain");
    assert_eq!(
        rec.granule_count, footer.granule_count,
        "manifest geometry is grain-true, never default-grain fiction"
    );

    // Full decode-vs-oracle round trip through the REAL registry binding.
    let verified =
        verify_manifest(&built.files, &built.manifest, &built.fx).expect("round trip");
    assert_eq!(verified, rows);
}

#[test]
fn narrow_fixture_stays_at_the_default_grain() {
    let rows = 12_500u64;
    let fx = Fixture {
        name: "narrow_grain_default",
        dir: "/qa/t9002".to_string(),
        spc: 1663,
        db: 5,
        relfilenumber: 9002,
        schema: vec![int8_col(1)],
        oracle: vec![(0..rows)
            .map(|i| Some(OracleVal::Word(i)))
            .collect()],
        plans: Vec::new(),
        policy: PartCutPolicy::default(),
    };
    let built = build_fixture(fx);
    let part_bytes = built.files.get("part-0.pgrc2").expect("part file");
    let tail = PartTail::decode_at_eof(part_bytes).expect("tail");
    let footer =
        FooterFixed::decode(&part_bytes[tail.footer_off as usize..]).expect("footer");
    assert_eq!(footer.granule_rows, pgrc2_format::geom::GRANULE_ROWS);
    let verified =
        verify_manifest(&built.files, &built.manifest, &built.fx).expect("round trip");
    assert_eq!(verified, rows);
}
