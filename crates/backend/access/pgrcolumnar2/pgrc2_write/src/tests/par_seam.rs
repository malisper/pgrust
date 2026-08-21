//! Seam-replayed trackers (M3-I slice leg: "ordered-commit + seam-replayed
//! tracker tests"): [`ColBuffer::splice_chunk`] must reproduce EXACTLY the
//! state serial appends produce — proven at the strongest available oracle,
//! the frozen seal face's part BYTES, plus the direct tracker observables
//! (stats, logical hash). Both teeth: the diff catches a seeded seam defect
//! (out-of-order replay), and the guards refuse ill-formed seams typed.

use super::*;
use crate::ingest::ColBuffer;
use crate::seal::seal_part;
use pgrc2_format::abi::ColumnMetaBuilder;

/// Build chunk-local ColBuffers for rows [lo, hi) of the mixed corpus.
fn chunk_cols(lo: u64, hi: u64) -> Vec<ColBuffer> {
    let mut cols = vec![
        ColBuffer::new(int8_col(1)),
        ColBuffer::new(text_col(2)),
    ];
    let mut ext = crate::ingest::NoExternalDetoast;
    for i in lo..hi {
        with_mixed_row(i, |row| {
            for (c, d) in row.iter().enumerate() {
                match d {
                    RawDatum::Null => cols[c].append_null(),
                    RawDatum::Word(w) => cols[c].append_word(*w).expect("word"),
                    RawDatum::Bytes(b) => {
                        let mut scratch = Vec::new();
                        let (p, _) =
                            crate::ingest::normalize_varlena(b, &mut ext, &mut scratch)
                                .expect("normalize");
                        cols[c].append_varlena_payload(p).expect("varlena");
                    }
                }
            }
        });
    }
    cols
}

/// Seal a column set through the frozen face into a fresh MemVfs; return the
/// temp file bytes.
fn seal_bytes(cols: &[ColBuffer]) -> Vec<u8> {
    let mut vfs = mem_with_dir();
    let kit = Kit::new();
    let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
    let mut builders: Vec<Box<dyn ColumnMetaBuilder>> = (0..cols.len())
        .map(|_| Box::new(crate::meta_standin::StandinMetaBuilder::new()) as Box<dyn ColumnMetaBuilder>)
        .collect();
    let spec = crate::seal::PartSpec {
        spc: SPC,
        db: DB,
        relfilenumber: RELFILENUMBER,
        schema_fingerprint: pgrc2_format::ident::schema_fingerprint(&[int8_col(1), text_col(2)]),
    };
    let (sealed, _) = seal_part(
        &mut vfs,
        DIR,
        &spec,
        cols,
        &[],
        &mut builders,
        &sources,
        &kit.resolver,
        &crate::structural::StructuralPolicy::default(),
        42,
        0,
    )
    .expect("seal");
    vfs.read_full(&format!("{DIR}/{}", sealed.tmp_name)).expect("bytes")
}

/// Splice the given chunk ranges in order into part-level buffers.
fn spliced(ranges: &[(u64, u64)]) -> Vec<ColBuffer> {
    let mut part = vec![ColBuffer::new(int8_col(1)), ColBuffer::new(text_col(2))];
    for &(lo, hi) in ranges {
        let chunk = chunk_cols(lo, hi);
        for (i, cb) in chunk.iter().enumerate() {
            part[i].splice_chunk(cb).expect("splice");
        }
    }
    part
}

/// Leg 1: chunked splice ≡ serial appends — tracker observables AND sealed
/// bytes, partial tail chunk included.
#[test]
fn splice_replays_serial_state_exactly() {
    let serial = chunk_cols(0, 293); // one shot = the serial oracle
    let par = spliced(&[(0, 128), (128, 256), (256, 293)]); // 2 full + partial tail
    for (s, p) in serial.iter().zip(par.iter()) {
        assert_eq!(s.rows(), p.rows());
        assert_eq!(s.has_null(), p.has_null());
        assert_eq!(s.approx_bytes(), p.approx_bytes(), "heap/word/validity sizes");
        let ss = s.stream_stats();
        let ps = p.stream_stats();
        assert_eq!(ss.rows, ps.rows);
        assert_eq!(ss.nonnull, ps.nonnull);
        assert_eq!(ss.constant, ps.constant);
        assert_eq!(ss.value_bytes, ps.value_bytes);
        assert_eq!(ss.oversize_values, ps.oversize_values);
        assert_eq!(s.logical_hash().digest(), p.logical_hash().digest());
    }
    assert_eq!(seal_bytes(&serial), seal_bytes(&par), "frozen-face byte oracle");
}

/// Leg 2 (tooth): OUT-OF-ORDER replay is CAUGHT by the byte oracle — the
/// comparator provably discriminates (the seeded-defect tooth of the
/// determinism gate).
#[test]
fn out_of_order_replay_is_caught_by_the_byte_diff() {
    let serial = chunk_cols(0, 256);
    let swapped = spliced(&[(128, 256), (0, 128)]);
    assert_ne!(
        seal_bytes(&serial),
        seal_bytes(&swapped),
        "a mis-ordered seam replay must not slip past the byte diff"
    );
}

/// Leg 3 (guard tooth): a seam off the 64-row validity-word boundary is a
/// typed refusal — the structural defect class the splice contract excludes.
#[test]
fn splice_seam_alignment_guard_fires() {
    let mut part = vec![ColBuffer::new(int8_col(1)), ColBuffer::new(text_col(2))];
    let a = chunk_cols(0, 37); // deliberately unaligned
    for (i, cb) in a.iter().enumerate() {
        part[i].splice_chunk(cb).expect("first splice lands anywhere");
    }
    let b = chunk_cols(37, 101);
    let err = part[0].splice_chunk(&b[0]).expect_err("must refuse");
    assert!(
        matches!(err, crate::WriteError::Contract { detail } if detail.contains("64-row")),
        "typed seam guard, got {err:?}"
    );
}

/// Leg 4 (guard): foreign-schema chunks refused.
#[test]
fn splice_foreign_schema_refused() {
    let mut part = ColBuffer::new(int8_col(1));
    let chunk = ColBuffer::new(int8_col(2)); // different attno
    let err = part.splice_chunk(&chunk).expect_err("must refuse");
    assert!(matches!(err, crate::WriteError::Contract { .. }));
}

/// Leg 5: constancy tracker across seams — the four seam cases.
#[test]
fn constancy_replays_across_seams() {
    let text = |s: &str| -> ColBuffer {
        let mut c = ColBuffer::new(text_col(2));
        for _ in 0..64 {
            c.append_varlena_payload(s.as_bytes()).expect("append");
        }
        c
    };
    let nulls = || -> ColBuffer {
        let mut c = ColBuffer::new(text_col(2));
        for _ in 0..64 {
            c.append_null();
        }
        c
    };

    // (a) constant across chunks stays constant.
    let mut p = ColBuffer::new(text_col(2));
    p.splice_chunk(&text("k")).unwrap();
    p.splice_chunk(&text("k")).unwrap();
    assert!(p.stream_stats().constant);

    // (b) constancy broken exactly AT the seam.
    let mut p = ColBuffer::new(text_col(2));
    p.splice_chunk(&text("k")).unwrap();
    p.splice_chunk(&text("m")).unwrap();
    assert!(!p.stream_stats().constant);

    // (c) all-null chunk is constancy-neutral.
    let mut p = ColBuffer::new(text_col(2));
    p.splice_chunk(&text("k")).unwrap();
    p.splice_chunk(&nulls()).unwrap();
    p.splice_chunk(&text("k")).unwrap();
    assert!(p.stream_stats().constant);

    // (d) null-LEADING part takes its first canonical from the first
    // value-bearing chunk.
    let mut p = ColBuffer::new(text_col(2));
    p.splice_chunk(&nulls()).unwrap();
    p.splice_chunk(&text("k")).unwrap();
    assert!(p.stream_stats().constant);
    p.splice_chunk(&text("m")).unwrap();
    assert!(!p.stream_stats().constant);
}
