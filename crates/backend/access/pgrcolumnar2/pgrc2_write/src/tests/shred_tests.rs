//! Shredded dual-store emission (M3-D slice leg 12; O-4/O-9): typed lanes
//! ride as row-aligned `path_ord != 0` substreams mapped by the PathTable,
//! the IMAGE lane stays byte-exact (dual-store law), and the O-9 path
//! budget is enforced. The lanes here come from a scripted
//! [`ShredLaneSource`] — the vendored `jsonb_shred` walker is M3-B's
//! not-yet-vendored deliverable and plugs into the SAME seam (stated
//! plainly in the lane report).

use super::*;
use crate::ingest::ColBuffer;
use crate::publish::TxnVerdict;
use crate::shred::{decode_path_table, encode_path_table, ShredLane, ShredLaneSource};
use crate::WriteError;
use pgrc2_format::part::{SectionKind, StreamRole};
use pgrc2_format::relopt::ShredOptions;

/// Scripted source: lane 1 = payload byte length (int8), lane 2 = first
/// byte (text). Null parent rows shred to null lane rows.
struct FakeShred;

impl ShredLaneSource for FakeShred {
    fn shred(
        &mut self,
        parent: &ColBuffer,
        _opts: &ShredOptions,
    ) -> crate::WriteResult<Vec<ShredLane>> {
        let attno = parent.schema.attno;
        let mut len_col = ColBuffer::new(int8_col(attno));
        let mut first_col = ColBuffer::new(text_col(attno));
        for r in 0..parent.rows() {
            match parent.varlena_payload(r)? {
                None => {
                    len_col.append_null();
                    first_col.append_null();
                }
                Some(p) => {
                    len_col.append_word(p.len() as u64)?;
                    first_col.append_varlena_payload(&p[..p.len().min(1)])?;
                }
            }
        }
        Ok(vec![
            ShredLane {
                scale: None,
                parent_attno: attno,
                path: "$.len".to_string(),
                col: len_col,
            },
            ShredLane {
                scale: None,
                parent_attno: attno,
                path: "$.first".to_string(),
                col: first_col,
            },
        ])
    }
}

fn run(shred: bool) -> (crate::wvfs::MemVfs, Vec<crate::seal::SealReport>) {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut fake = FakeShred;
    let mut w = open_writer(vec![int8_col(1), text_col(2)], stamp(21, 1));
    for i in 0..500u64 {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: if shred { &mut fake } else { &mut kit.shred },
            shred_opts: &kit.opts,
        };
        let img = img_4b_u(format!("doc-{}", i % 37).as_bytes());
        let d2 = if i % 11 == 0 {
            RawDatum::Null
        } else {
            RawDatum::Bytes(&img)
        };
        w.append_row(&[RawDatum::Word(i), d2], &mut kit.ext, &mut env)
            .expect("append");
    }
    let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
    let mut env = SealEnv {
        vfs: &mut vfs,
        sources: &sources,
        resolver: &kit.resolver,
        shred: if shred { &mut fake } else { &mut kit.shred },
        shred_opts: &kit.opts,
    };
    w.finish(&mut env).expect("finish");
    let probe = Probe::new(TxnVerdict::InProgress).set(21, TxnVerdict::Committed);
    w.publish(&mut vfs, &probe).expect("publish");
    let reports = w.seal_reports().to_vec();
    (vfs, reports)
}

#[test]
fn dual_store_emits_lanes_path_table_and_keeps_image_lane_byte_exact() {
    let (mut vfs_s, reports_s) = run(true);
    let (mut vfs_p, _) = run(false);

    let pv_s = PartView::open(&mut vfs_s, "part-0.pgrc2");
    let pv_p = PartView::open(&mut vfs_p, "part-0.pgrc2");

    // PathTable present iff shredded; decodes to the lane paths in
    // path_ord order.
    let pt = pv_s
        .section_bytes(SectionKind::PathTable, 0, 0)
        .expect("PathTable");
    assert_eq!(
        decode_path_table(pt).expect("decode"),
        vec!["$.len".to_string(), "$.first".to_string()]
    );
    assert!(pv_p.section_bytes(SectionKind::PathTable, 0, 0).is_none());

    // Lane streams exist, row-aligned, with validity (parent nulls shred
    // to lane nulls).
    let (lane1, _) = pv_s.stream(2, 1, StreamRole::Values).expect("lane 1");
    assert_eq!(lane1.values, 500);
    assert!(pv_s.stream(2, 1, StreamRole::Validity).is_some());
    let (lane2, _) = pv_s.stream(2, 2, StreamRole::Values).expect("lane 2");
    assert_eq!(lane2.values, 500);

    // Stats sections exist per lane too.
    assert!(pv_s.section_bytes(SectionKind::Stats, 2, 1).is_some());
    assert!(pv_s.section_bytes(SectionKind::Stats, 2, 2).is_some());

    // DUAL-STORE: the parent IMAGE lane's value extent bytes are identical
    // with and without shredding — byte-exact reconstruction truth.
    let (_, exts_s) = pv_s.stream(2, 0, StreamRole::Values).expect("parent s");
    let (_, exts_p) = pv_p.stream(2, 0, StreamRole::Values).expect("parent p");
    assert_eq!(exts_s.len(), exts_p.len());
    for (a, b) in exts_s.iter().zip(&exts_p) {
        let sa = &pv_s.bytes[a.file_off as usize..(a.file_off + a.len) as usize];
        let sb = &pv_p.bytes[b.file_off as usize..(b.file_off + b.len) as usize];
        assert!(sa == sb, "image lane bytes diverged under shredding");
    }

    // Every value stream (2 roots + 2 lanes) was round-trip verified.
    assert_eq!(reports_s[0].granules_verified, 4);
    // Lane hashes recorded (O-10 identity per stream).
    assert_eq!(reports_s[0].col_hashes.len(), 4);
}

#[test]
fn path_budget_enforced_typed() {
    let mut vfs = mem_with_dir();
    let kit = Kit::new();
    let mut fake = FakeShred;
    let tight = ShredOptions {
        max_paths: 1,
        ..ShredOptions::default()
    };
    let mut w = open_writer(vec![text_col(1)], stamp(22, 1));
    let img = img_4b_u(b"doc");
    {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut fake,
            shred_opts: &tight,
        };
        let mut ext = crate::ingest::NoExternalDetoast;
        w.append_row(&[RawDatum::Bytes(&img)], &mut ext, &mut env)
            .expect("append");
        let err = w.finish(&mut env).unwrap_err();
        assert_eq!(
            err,
            WriteError::Contract {
                detail: "shred path budget exceeded (pgrc2_shred_max_paths)"
            }
        );
    }
}

#[test]
fn path_table_encoding_golden() {
    let bytes = encode_path_table(&["a", "bc"]);
    assert_eq!(
        bytes,
        vec![2, 0, 0, 0, 1, 0, 0x61, 0, 2, 0, 0x62, 0x63],
        "spec §6.5 framing: count u32, 4-aligned {{len u16, bytes}} entries"
    );
    assert_eq!(
        decode_path_table(&bytes).expect("decode"),
        vec!["a".to_string(), "bc".to_string()]
    );
}
