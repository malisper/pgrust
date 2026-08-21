//! Composed pin for the dict-varlena ZERO-COPY gather (dekern phase 2):
//! a REAL low-NDV text part — written by D's writer with the dict arm
//! actually ELECTED (posture-supplied `CodecCandidates`, the production
//! election face, exact pricing) — read back through F's cursor by the QA
//! verifier, whose widened containment gate (arena ∪ dict payload region)
//! must both ADMIT the zero-copy views and still FIRE elsewhere (the unit
//! teeth live in `corpus::containment_teeth`).
//!
//! Green-in-isolation ≠ green-composed: the codec-crate pins prove the
//! kernel over hand-built `DictSections`; THIS test proves the whole read
//! stack — writer election → sealed part → segment-cache-resident dict
//! sections → cursor decode → containment gate → oracle equality.
//!
//! The election witness is load-bearing (gate-blindness law): if the dict
//! arm silently stops electing on this corpus, the `dict_payload_bounds`
//! assert fails — the test can never pass vacuously on a verbatim part.

use pgrc2_qa::adapters::{full_binding, Probe};
use pgrc2_qa::corpus::{
    append_rows_with_sources, decode_column, decode_sel_granule, finish_with_sources,
    open_part_bytes, open_writer, Fixture, OracleVal,
};
use pgrc2_read::cursor::StreamCursor;
use pgrc2_write::dict::TextSemantics;
use pgrc2_write::elect::{
    CandidateSource, CodecCandidates, ColumnPosture, DictPolicy, ReferenceCandidates,
};
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::writer::PartCutPolicy;
use pgrc2_write::wvfs::WriteVfs;

/// ~40 distinct values over 20k rows (some > 12 bytes — StrView pointer
/// form; some <= 12 — inline form; some nulls): the dict arm's pricing win
/// is enormous, so the election is stable, not knife-edge.
fn low_ndv_fixture() -> Fixture {
    let val = |i: u64| -> Vec<u8> {
        let k = i % 40;
        if k % 3 == 0 {
            format!("v{k:02}").into_bytes() // inline-form length
        } else {
            format!("value-{k:02}-{}", "x".repeat((k % 7) as usize + 10)).into_bytes()
        }
    };
    Fixture {
        name: "text_dict_low_ndv",
        dir: "/qa/t190".to_string(),
        spc: 1663,
        db: 5,
        relfilenumber: 190,
        schema: vec![pgrc2_qa::text_col(1)],
        oracle: vec![(0..20_000u64)
            .map(|i| {
                if i % 11 == 7 {
                    None
                } else {
                    Some(OracleVal::Bytes(val(i)))
                }
            })
            .collect()],
        plans: Vec::new(),
        policy: PartCutPolicy::default(),
    }
}

#[test]
fn dict_elected_part_reads_zero_copy_through_the_qa_gate() {
    let fx = low_ndv_fixture();
    let fxid = 900 + fx.relfilenumber;

    // Write through the production seal path with the dict arm unlocked
    // for the text column (posture is catalog knowledge — supplied here
    // the way the AM supplies it).
    let dict_posture = ColumnPosture {
        dict: Some(DictPolicy {
            ndv_cap: 4096,
            exec_ok: false,
            sem: TextSemantics::Utf8Chars,
        }),
        ..Default::default()
    };
    let codec = CodecCandidates::new(ColumnPosture::default()).with_column(1, 0, dict_posture);
    let reference = ReferenceCandidates;
    let sources: Vec<&dyn CandidateSource> = vec![&codec, &reference];

    let mut vfs = pgrc2_qa::simvfs::SimVfs::new();
    vfs.mkdir_path(&fx.dir).expect("mkdir");
    let mut probe = Probe::new(TxnVerdict::Aborted);
    probe.mark(fxid, TxnVerdict::InProgress);
    let mut w = open_writer(&fx, fxid).expect("open writer");
    append_rows_with_sources(&mut vfs, &mut w, &fx, 0, fx.rows(), &sources).expect("append");
    finish_with_sources(&mut vfs, &mut w, &sources).expect("finish");
    w.publish(&mut vfs, &probe).expect("publish");
    probe.mark(fxid, TxnVerdict::Committed);

    let files = vfs.snapshot_dir(&fx.dir);
    let parts: Vec<&Vec<u8>> = files
        .iter()
        .filter(|(n, _)| pgrc2_format::dirlayout::parse_part_file_name(n).is_some())
        .map(|(_, b)| b)
        .collect();
    assert!(!parts.is_empty(), "fixture produced no parts");

    let binding = full_binding();
    let mut rows_seen = 0usize;
    for (pi, bytes) in parts.iter().enumerate() {
        let part = open_part_bytes(bytes, 500 + pi as u64).expect("open part");

        // ELECTION WITNESS: the text column carries dict streams — a
        // verbatim demotion cannot pass this test vacuously.
        let mut cur =
            StreamCursor::open(std::sync::Arc::clone(&part), binding, 1, 0).expect("cursor");
        let (dict_lo, dict_len) = cur
            .dict_payload_bounds()
            .expect("bounds face")
            .expect("dict arm must ELECT on this corpus (election witness)");
        assert!(dict_len > 0, "empty dict payload region");
        // Absolute-alignment pin: SegBuf base (8) + 32-byte section header
        // ⇒ the payload region base — and with it every 8-aligned entry —
        // is absolutely 8-aligned (StrView §7b zero-copy dependency).
        assert_eq!(dict_lo % 8, 0, "dict payload region base not 8-aligned");
        drop(cur);

        // The production QA verifier: decode + widened containment gate.
        let col = decode_column(&part, binding, &fx.schema[0])
            .expect("decode_column under the widened containment gate");

        // Oracle equality. (The kernel-grain range pin — datums INSIDE the
        // dict payload region, arena untouched — lives in the codec suite;
        // at this grain the gate ADMITTING the decode plus the election
        // witness above is the composed evidence.)
        for (r, got) in col.iter().enumerate() {
            let row = rows_seen + r;
            let want = &fx.oracle[0][row];
            match (got, want) {
                (None, None) => {}
                (Some(OracleVal::Bytes(g)), Some(OracleVal::Bytes(w))) => {
                    assert_eq!(g, w, "row {row}: payload bytes diverged");
                }
                other => panic!("row {row}: oracle shape mismatch {other:?}"),
            }
        }

        // decode_sel ≡ decode_full ∘ select at the QA grain on granule 0.
        let sel_rows: Vec<u16> = (0..64u16).map(|k| k * 7).collect();
        let sel = decode_sel_granule(&part, binding, &fx.schema[0], 0, &sel_rows)
            .expect("decode_sel under the widened gate");
        for (k, &r) in sel_rows.iter().enumerate() {
            let row = rows_seen + r as usize;
            assert_eq!(
                sel[k], fx.oracle[0][row],
                "decode_sel diverged at part {pi} row {r}"
            );
        }

        rows_seen += col.len();
    }
    assert_eq!(rows_seen as u64, fx.rows(), "row coverage");
}
