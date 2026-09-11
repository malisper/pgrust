//! Election metamorphic composite (§5 M3-K: "permuted elections ⇒ identical
//! answers across the full type corpus"): the SAME oracle data written under
//! different (real) codec elections must decode to identical logical
//! answers; permuting the candidate-source order must not change the part
//! BYTES (the deterministic-election law); and the variants must actually
//! elect DIFFERENT encodings (the it-really-took witness — otherwise the
//! leg proves nothing).

use pgrc2_qa::adapters::{ForcedElection, ForcedPlan, Probe};
use pgrc2_qa::corpus::{
    append_fixture_rows, append_rows_with_sources, finish, finish_with_sources, int8_fixture,
    open_writer, verify_manifest, Fixture, OracleVal,
};
use pgrc2_qa::simvfs::SimVfs;
use pgrc2_qa::{bool_col, f64_col};
use pgrc2_format::dirlayout::part_file_name;
use pgrc2_format::part::{FooterFixed, PartTail, SectionEntry, SectionKind, StreamEntry};
use pgrc2_format::wire::Cur;
use pgrc2_write::elect::{CandidateSource, ReferenceCandidates};
use pgrc2_write::publish::{effective_manifest, TxnVerdict};
use pgrc2_write::writer::PartCutPolicy;
use pgrc2_write::wvfs::WriteVfs;

const ROWS: u64 = 17_000; // crosses granule 8192 and band-internal edges

/// Values valid for EVERY int arm at once (per-frame range ≤ u16 for the
/// w2 arm, gentle deltas for DELTA_FOR).
fn int_val(i: u64) -> Option<i64> {
    if i % 7 == 4 {
        None
    } else {
        Some(1_000_000 + (i as i64) * 3)
    }
}

fn publish_and_decode(fx: &Fixture) -> (Vec<(u32, u16)>, u64, Vec<u8>) {
    let mut vfs = SimVfs::new();
    vfs.mkdir_path(&fx.dir).expect("mkdir");
    let fxid = 500 + fx.relfilenumber;
    let mut probe = Probe::new(TxnVerdict::Aborted);
    probe.mark(fxid, TxnVerdict::InProgress);
    let mut w = open_writer(fx, fxid).expect("open");
    append_fixture_rows(&mut vfs, &mut w, fx, 0, fx.rows()).expect("rows");
    finish(&mut vfs, &mut w, fx).expect("finish");
    w.publish(&mut vfs, &probe).expect("publish");
    probe.mark(fxid, TxnVerdict::Committed);
    let eff = effective_manifest(&mut vfs, &fx.dir, &probe, None)
        .expect("effective")
        .expect("gen1");
    let files = vfs.snapshot_dir(&fx.dir);
    // The identical-answers arm: decode vs the shared oracle.
    let rows = verify_manifest(&files, &eff, fx).expect("decode-vs-oracle");
    assert_eq!(rows, fx.rows(), "{}: row count", fx.name);
    // The election witness: value-stream encodings from the part bytes.
    let part = files
        .get(&part_file_name(eff.parts[0].part_no))
        .expect("part bytes")
        .clone();
    (stream_encodings(&part), rows, part)
}

/// Parse (attno, encoding) for every VALUES-role stream straight from the
/// part image (independent of reader API surface).
fn stream_encodings(bytes: &[u8]) -> Vec<(u32, u16)> {
    let tail = PartTail::decode_at_eof(bytes).expect("tail");
    let footer = FooterFixed::decode(&bytes[tail.footer_off as usize..]).expect("footer");
    let st = footer.section_table_off as usize;
    let mut c = Cur::new(&bytes[st..]);
    let mut streamdir = None;
    for _ in 0..footer.section_count {
        let e = SectionEntry::decode(&mut c).expect("entry");
        if e.kind == SectionKind::StreamDir.as_u16() {
            streamdir = Some((e.off as usize, e.len as usize));
        }
    }
    let (off, len) = streamdir.expect("StreamDir present");
    let body = &bytes[off..off + len];
    let mut sc = Cur::new(body);
    let mut out = Vec::new();
    for _ in 0..footer.stream_count {
        let e = StreamEntry::decode(&mut sc).expect("stream entry");
        if e.role == pgrc2_format::part::StreamRole::Values.as_u8() {
            out.push((e.attno, e.encoding));
        }
    }
    out
}

#[test]
fn permuted_int_elections_answer_identically() {
    // Full arm set (bytefor_w2 + deltafor re-armed: #465 fixed by M3-A2).
    let arms: [(&'static str, u64, Vec<ForcedPlan>); 4] = [
        ("verbatim", 401, vec![]),
        (
            "bytefor_w2",
            402,
            vec![ForcedPlan::ByteFor {
                delta_width: 2,
                signed: true,
            }],
        ),
        (
            "bytefor_w8",
            403,
            vec![ForcedPlan::ByteFor {
                delta_width: 8,
                signed: true,
            }],
        ),
        ("deltafor", 404, vec![ForcedPlan::DeltaFor]),
    ];
    let mut encodings = Vec::new();
    for (name, relf, plans) in arms {
        let fx = int8_fixture(name, relf, ROWS, plans, PartCutPolicy::default(), int_val);
        let (encs, _rows, _part) = publish_and_decode(&fx);
        assert_eq!(encs.len(), 1, "{name}: one value stream");
        encodings.push(encs[0].1);
    }
    // The it-really-took witness: the arms elected DIFFERENT encodings.
    let distinct: std::collections::BTreeSet<u16> = encodings.iter().copied().collect();
    assert!(
        distinct.len() >= 3,
        "elections did not diversify: {encodings:?} — the metamorphic leg is vacuous"
    );
}

#[test]
fn permuted_float_and_bool_elections_answer_identically() {
    // f64 arms share one oracle.
    let fval = |i: u64| {
        if i % 11 == 6 {
            None
        } else {
            Some(OracleVal::Word(((i as f64) * 0.25 - 900.0).to_bits()))
        }
    };
    // ALP / ALP_RD arms re-armed (#465 fixed by M3-A2).
    let mut f_encodings = Vec::new();
    for (name, relf, plans) in [
        ("f64_verbatim", 411, vec![]),
        ("f64_alp", 412, vec![ForcedPlan::Alp]),
        ("f64_alprd", 413, vec![ForcedPlan::AlpRd]),
    ] {
        let fx = Fixture {
            name,
            dir: format!("/qa/t{relf}"),
            spc: 1663,
            db: 5,
            relfilenumber: relf,
            schema: vec![f64_col(1)],
            oracle: vec![(0..ROWS).map(fval).collect()],
            plans,
            policy: PartCutPolicy::default(),
        };
        let (encs, _, _) = publish_and_decode(&fx);
        f_encodings.push(encs[0].1);
    }
    let f_distinct: std::collections::BTreeSet<u16> = f_encodings.iter().copied().collect();
    assert!(
        f_distinct.len() >= 3,
        "float elections did not diversify: {f_encodings:?}"
    );

    // bool arms.
    let bval = |i: u64| {
        if i % 5 == 3 {
            None
        } else {
            Some(OracleVal::Word((i / 3) & 1))
        }
    };
    let mut b_encodings = Vec::new();
    for (name, relf, plans) in [
        ("bool_verbatim", 421, vec![]),
        ("bool_bitmap", 422, vec![ForcedPlan::Bool]),
    ] {
        let fx = Fixture {
            name,
            dir: format!("/qa/t{relf}"),
            spc: 1663,
            db: 5,
            relfilenumber: relf,
            schema: vec![bool_col(1)],
            oracle: vec![(0..ROWS).map(bval).collect()],
            plans,
            policy: PartCutPolicy::default(),
        };
        let (encs, _, _) = publish_and_decode(&fx);
        b_encodings.push(encs[0].1);
    }
    assert_ne!(b_encodings[0], b_encodings[1], "bool elections vacuous");
}

/// Candidate-source ORDER must not change part bytes (smallest-wins +
/// first-wins-ties is deterministic across source order).
#[test]
fn source_order_permutation_is_byte_stable() {
    let fx = int8_fixture(
        "order_perm",
        431,
        9_000,
        Vec::new(), // sources supplied manually below
        PartCutPolicy::default(),
        int_val,
    );
    let forced = ForcedElection::new(ForcedPlan::ByteFor {
        delta_width: 2, // re-armed: #465 fixed by M3-A2
        signed: true,
    });
    let reference = ReferenceCandidates;
    let mut parts = Vec::new();
    for order in 0..2 {
        let sources: Vec<&dyn CandidateSource> = if order == 0 {
            vec![&forced, &reference]
        } else {
            vec![&reference, &forced]
        };
        let mut vfs = SimVfs::new();
        vfs.mkdir_path(&fx.dir).expect("mkdir");
        let fxid = 700 + order;
        let mut probe = Probe::new(TxnVerdict::Aborted);
        probe.mark(fxid, TxnVerdict::InProgress);
        let mut w = open_writer(&fx, fxid).expect("open");
        append_rows_with_sources(&mut vfs, &mut w, &fx, 0, fx.rows(), &sources).expect("rows");
        finish_with_sources(&mut vfs, &mut w, &sources).expect("finish");
        w.publish(&mut vfs, &probe).expect("publish");
        probe.mark(fxid, TxnVerdict::Committed);
        let eff = effective_manifest(&mut vfs, &fx.dir, &probe, None)
            .expect("effective")
            .expect("gen1");
        let files = vfs.snapshot_dir(&fx.dir);
        parts.push(
            files
                .get(&part_file_name(eff.parts[0].part_no))
                .expect("part")
                .clone(),
        );
    }
    assert_eq!(
        parts[0], parts[1],
        "candidate-source order changed part bytes — deterministic-election law broken"
    );
}
