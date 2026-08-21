//! Election gates (M3-D slice legs 2–3): the ≥10%-win law, the
//! incompressible guard, and the recorded election witnesses (report +
//! `StreamEntry.encoding` on disk).

use super::*;
use crate::elect::{
    elect_stream, wins_ten_pct, Candidate, CandidateSource, Elected, EncoderFactory, ExtentShape,
    FullElectInput, StreamStats,
};
use crate::publish::TxnVerdict;
use pgrc2_format::abi::{GranuleEncoder, KernelKey};
use pgrc2_format::enc::EncodingId;
use pgrc2_format::verbatim::VerbatimEncoder;

#[test]
fn ten_pct_gate_is_integer_exact() {
    // Boundary: candidate exactly 10% smaller wins; one byte over loses.
    assert!(wins_ten_pct(90, 100));
    assert!(!wins_ten_pct(91, 100));
    assert!(wins_ten_pct(9, 10));
    assert!(!wins_ten_pct(10, 10)); // 0% win is not a win
    assert!(wins_ten_pct(0, 0)); // degenerate: both empty
    // Sweep: the gate must agree with exact rational arithmetic.
    for base in 0u64..500 {
        for cand in 0..=base {
            let exact = (base - cand) as u128 * 10 >= base as u128;
            assert_eq!(wins_ten_pct(cand, base), exact, "cand={cand} base={base}");
        }
    }
    // Overflow guard: enormous candidate sizes never win by wraparound.
    assert!(!wins_ten_pct(u64::MAX, 100));
}

struct ScriptedFactory {
    encoding: u16,
}

impl EncoderFactory for ScriptedFactory {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: self.encoding,
            class: 0,
            width: 8,
        }
    }
    fn make(&self) -> Box<dyn GranuleEncoder> {
        Box::new(VerbatimEncoder {
            class: pgrc2_format::class::StorageClass::ByvalWord {
                width: 8,
                signed: true,
            },
        })
    }
}

struct ScriptedSource {
    encoding: u16,
    len: u64,
}

impl CandidateSource for ScriptedSource {
    fn propose(&self, _input: &FullElectInput<'_>) -> Vec<Candidate> {
        vec![Candidate {
            factory: Box::new(ScriptedFactory {
                encoding: self.encoding,
            }),
            encoded_len: self.len,
        }]
    }
}

/// Stats-only election input (identity + stats + shape; no value data —
/// the issue-#463 seam carries identity even on the stats path).
fn stats_input<'a>(stats: &'a StreamStats, shape: &'a ExtentShape) -> FullElectInput<'a> {
    FullElectInput {
        attno: 1,
        path_ord: 0,
        stats,
        shape,
        granules: &[],
        col: None,
    }
}

fn int8_stats(rows: u64) -> StreamStats {
    StreamStats {
        class: pgrc2_format::class::StorageClass::ByvalWord {
            width: 8,
            signed: true,
        },
        rows,
        nonnull: rows,
        constant: false,
        value_bytes: rows * 8,
        oversize_values: 0,
    }
}

#[test]
fn incompressible_guard_demotes_to_verbatim_at_the_boundary() {
    let stats = int8_stats(1000);
    let shape = ExtentShape {
        extent_count: 1,
        frame_count: 1,
    };
    let baseline = crate::elect::verbatim_baseline_len(&stats, &shape); // 32 + 8000
    // Exactly at the win threshold: elected.
    let winning = ScriptedSource {
        encoding: 999,
        len: baseline * 9 / 10,
    };
    let (elected, w) = elect_stream(&stats_input(&stats, &shape), &[&winning]);
    assert!(matches!(elected, Elected::Candidate(_)));
    assert_eq!(w.encoding, 999);
    assert_eq!(w.baseline_len, baseline);
    // One byte past the threshold: the guard holds VERBATIM.
    let losing = ScriptedSource {
        encoding: 999,
        len: baseline * 9 / 10 + 1,
    };
    let (elected, w) = elect_stream(&stats_input(&stats, &shape), &[&losing]);
    assert!(matches!(elected, Elected::Verbatim));
    assert_eq!(w.encoding, EncodingId::Verbatim.as_u16());
    assert_eq!(w.chosen_len, w.baseline_len);
}

#[test]
fn smallest_winning_candidate_wins_and_first_wins_ties() {
    let stats = int8_stats(1000);
    let shape = ExtentShape {
        extent_count: 1,
        frame_count: 1,
    };
    let a = ScriptedSource {
        encoding: 7,
        len: 500,
    };
    let b = ScriptedSource {
        encoding: 8,
        len: 400,
    };
    let c = ScriptedSource {
        encoding: 9,
        len: 400,
    };
    let (_, w) = elect_stream(&stats_input(&stats, &shape), &[&a, &b, &c]);
    assert_eq!(w.encoding, 8, "smallest wins; first at the size wins ties");
    assert_eq!(w.chosen_len, 400);
}

/// Constant column over MULTIPLE bands: CONST elected, one record per
/// extent (fresh encoder per extent — the factory law), the on-disk
/// StreamEntry.encoding is the witness, and every granule round-trips.
#[test]
fn constant_column_elects_const_across_bands() {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut w = open_writer(vec![int8_col(1)], stamp(9, 1));
    // 2 bands + change: 3 extents.
    append_int8_rows(&mut w, &mut vfs, &mut kit, 140_000, |_| Some(7));
    let probe = Probe::new(TxnVerdict::InProgress).set(9, TxnVerdict::Committed);
    finish_and_publish(&mut w, &mut vfs, &mut kit, &probe);
    let reports = w.seal_reports();
    assert_eq!(reports.len(), 1);
    let e = &reports[0].elections[0];
    assert_eq!(e.encoding, EncodingId::Const.as_u16());
    assert!(wins_ten_pct(e.chosen_len, e.baseline_len));
    // 140k rows = 18 granules; every one verified through decode_full.
    assert_eq!(reports[0].granules_verified, 18);
    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let (entry, extents) = pv.stream(1, 0, StreamRole::Values).expect("values stream");
    assert_eq!(entry.encoding, EncodingId::Const.as_u16());
    assert_eq!(extents.len(), 3, "one extent per band");
}

#[test]
fn varied_column_stays_verbatim() {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut w = open_writer(vec![int8_col(1)], stamp(9, 1));
    append_int8_rows(&mut w, &mut vfs, &mut kit, 5_000, |i| Some(i as i64));
    let probe = Probe::new(TxnVerdict::InProgress).set(9, TxnVerdict::Committed);
    finish_and_publish(&mut w, &mut vfs, &mut kit, &probe);
    let e = &w.seal_reports()[0].elections[0];
    assert_eq!(e.encoding, EncodingId::Verbatim.as_u16());
    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let (entry, _) = pv.stream(1, 0, StreamRole::Values).expect("values stream");
    assert_eq!(entry.encoding, EncodingId::Verbatim.as_u16());
}

/// All-null column: CONST's ALL_NULL record wins (an exact stat election).
#[test]
fn all_null_column_elects_const_all_null() {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut w = open_writer(vec![int8_col(1)], stamp(9, 1));
    append_int8_rows(&mut w, &mut vfs, &mut kit, 10_000, |_| None);
    let probe = Probe::new(TxnVerdict::InProgress).set(9, TxnVerdict::Committed);
    finish_and_publish(&mut w, &mut vfs, &mut kit, &probe);
    let e = &w.seal_reports()[0].elections[0];
    assert_eq!(e.encoding, EncodingId::Const.as_u16());
    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    // Validity stream present (all rows null ⇒ has_null).
    assert!(pv.stream(1, 0, StreamRole::Validity).is_some());
}
