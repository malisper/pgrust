//! Overflow-region round-trip on oversize values (M3-D slice leg 6; spec
//! §6.8): values ≥ OVERSIZE_THRESHOLD route through the overflow stream and
//! still round-trip (the seal's mandatory verify decodes THROUGH the
//! OverflowRef indirection), with the stream facts on disk.

use super::*;
use crate::publish::TxnVerdict;
use pgrc2_format::geom::OVERSIZE_THRESHOLD;
use pgrc2_format::part::{StreamRole, STREAMF_HAS_OVERFLOW};

#[test]
fn oversize_values_route_through_overflow_and_roundtrip() {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut w = open_writer(vec![text_col(1)], stamp(11, 1));
    // Two oversize values (one exactly at threshold), plus small values and
    // nulls around them.
    let big_a: Vec<u8> = (0..OVERSIZE_THRESHOLD as usize).map(|i| (i % 251) as u8).collect();
    let big_b: Vec<u8> = (0..(OVERSIZE_THRESHOLD as usize) * 2).map(|i| (i % 249) as u8).collect();
    let imgs: Vec<Option<Vec<u8>>> = vec![
        Some(img_4b_u(b"small-1")),
        Some(img_4b_u(&big_a)),
        None,
        Some(img_4b_u(b"small-2")),
        Some(img_4b_u(&big_b)),
        Some(img_4b_u(b"")),
    ];
    for img in &imgs {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        let d = match img {
            None => RawDatum::Null,
            Some(i) => RawDatum::Bytes(i),
        };
        w.append_row(&[d], &mut kit.ext, &mut env).expect("append");
    }
    let probe = Probe::new(TxnVerdict::InProgress).set(11, TxnVerdict::Committed);
    finish_and_publish(&mut w, &mut vfs, &mut kit, &probe);

    // The seal's mandatory verify already decoded every granule (incl. the
    // OverflowRef arm) and compared canonical bytes; the report witnesses
    // it ran.
    let r = &w.seal_reports()[0];
    assert_eq!(r.granules_verified, 1);

    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let (values, _) = pv.stream(1, 0, StreamRole::Values).expect("values");
    assert_ne!(values.flags & STREAMF_HAS_OVERFLOW, 0, "HAS_OVERFLOW flag");
    let (ovf, ovf_exts) = pv.stream(1, 0, StreamRole::Overflow).expect("overflow stream");
    assert_eq!(ovf.values, 2, "two oversize entries");
    assert_eq!(ovf_exts.len(), 1);
    // Validity stream present (one null).
    assert!(pv.stream(1, 0, StreamRole::Validity).is_some());
    // Logical hash saw all six rows (5 values + 1 null).
    let (_, _, digest) = r.col_hashes[0];
    assert_eq!(digest.2, 6);
}
