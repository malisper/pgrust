//! Seal determinism (M3-D slice leg 1): same input partition ⇒
//! byte-identical part files — the two-run diff witness (charter §1).

use super::*;
use crate::publish::TxnVerdict;

/// One full run: multi-band mixed data (nulls, text incl. multi-granule),
/// publish, return (part file bytes, manifest bytes).
fn run_once() -> (Vec<Vec<u8>>, Vec<u8>) {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    // Two columns: int8 with a null stripe + text with varied lengths.
    let schema = vec![int8_col(1), text_col(2)];
    let mut w = open_writer_policy(
        schema,
        stamp(42, 1),
        crate::writer::PartCutPolicy {
            max_rows: 70_000, // forces a 2-part cut on 100k rows (multi-band)
            max_bytes: u64::MAX,
            // Granule == the row budget, so the cut lands exactly at 70_000
            // as it did before the M3-I granule gate.
            cut_granule_rows: 70_000,
        },
    );
    for i in 0..100_000u64 {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        let payload = format!("value-{}", i % 977);
        let d1 = if i % 13 == 0 {
            RawDatum::Null
        } else {
            RawDatum::Word((i as i64 - 5000) as u64)
        };
        let img = img_4b_u(payload.as_bytes());
        let d2 = if i % 17 == 0 {
            RawDatum::Null
        } else {
            RawDatum::Bytes(&img)
        };
        w.append_row(&[d1, d2], &mut kit.ext, &mut env).expect("append");
    }
    let probe = Probe::new(TxnVerdict::InProgress).set(42, TxnVerdict::Committed);
    let out = finish_and_publish(&mut w, &mut vfs, &mut kit, &probe);
    assert_eq!(out.part_nos, vec![0, 1]);
    let parts = out
        .part_nos
        .iter()
        .map(|pn| {
            vfs.read_full(&format!(
                "{DIR}/{}",
                pgrc2_format::dirlayout::part_file_name(*pn)
            ))
            .expect("part bytes")
        })
        .collect();
    let manifest = vfs
        .read_full(&format!(
            "{DIR}/{}",
            pgrc2_format::dirlayout::manifest_file_name(out.gen)
        ))
        .expect("manifest bytes");
    (parts, manifest)
}

#[test]
fn two_runs_produce_byte_identical_parts_and_manifest() {
    let (parts_a, manifest_a) = run_once();
    let (parts_b, manifest_b) = run_once();
    assert_eq!(parts_a.len(), 2);
    for (a, b) in parts_a.iter().zip(&parts_b) {
        assert!(a == b, "part bytes diverged between identical runs");
    }
    assert!(manifest_a == manifest_b, "manifest bytes diverged");
}

/// Every storage class the reference codec serves seals + verifies through
/// the same one-pass pipeline (bool bitmap-placeholder arm, fixed images,
/// signed words, varlena) with nulls in every column.
#[test]
fn all_storage_classes_seal_and_verify() {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let schema = vec![int8_col(1), bool_col(2), fixed16_col(3), text_col(4)];
    let mut w = open_writer(schema, stamp(43, 1));
    let fixed_imgs: Vec<[u8; 16]> = (0..3u8).map(|i| [i; 16]).collect();
    for i in 0..10_000u64 {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        let img = img_4b_u(format!("s{}", i % 41).as_bytes());
        let row = [
            if i % 5 == 0 {
                RawDatum::Null
            } else {
                RawDatum::Word((i as i64 - 100) as u64)
            },
            if i % 7 == 0 {
                RawDatum::Null
            } else {
                RawDatum::Word((i % 2 != 0) as u64)
            },
            if i % 9 == 0 {
                RawDatum::Null
            } else {
                RawDatum::Bytes(&fixed_imgs[(i % 3) as usize])
            },
            if i % 11 == 0 {
                RawDatum::Null
            } else {
                RawDatum::Bytes(&img)
            },
        ];
        w.append_row(&row, &mut kit.ext, &mut env).expect("append");
    }
    let probe = Probe::new(crate::publish::TxnVerdict::InProgress)
        .set(43, crate::publish::TxnVerdict::Committed);
    finish_and_publish(&mut w, &mut vfs, &mut kit, &probe);
    let r = &w.seal_reports()[0];
    // 10k rows = 2 granules × 4 value streams, every one verified.
    assert_eq!(r.granules_verified, 8);
    // 4 streams × (2 granules + 1 band + 1 part) cross-checks.
    assert_eq!(r.nonnull_crosschecks, 4 * 4);
    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    // All four columns have nulls: 4 value + 4 validity streams.
    assert_eq!(pv.footer().stream_count, 8);
}

#[test]
fn sealed_part_validates_structurally() {
    let (parts, _) = run_once();
    for bytes in parts {
        let mut vfs = MemVfs::new();
        vfs.mkdir_path(DIR).unwrap();
        // Re-house the bytes to reuse PartView's parser.
        let fd = vfs.create_rw(&format!("{DIR}/part-0.pgrc2")).unwrap();
        vfs.pwrite_at(&fd, 0, &bytes).unwrap();
        vfs.close_file(fd).unwrap();
        let pv = PartView::open(&mut vfs, "part-0.pgrc2");
        let f = pv.footer();
        assert!(f.rows == 70_000 || f.rows == 30_000);
        // Streams: 2 value + 2 validity (both columns have nulls).
        assert_eq!(f.stream_count, 4);
        // Every section's CRC validates.
        for s in pv.sections() {
            let body = &pv.bytes[s.off as usize..(s.off + s.len) as usize];
            assert_eq!(s.crc, pgrc2_format::wire::crc32c(body), "section crc");
        }
    }
}
