//! Record-format goldens vs the donor byte contracts (charter §5 M2-B).
//! Record formats are OPERATOR-owned contracts written THROUGH the
//! substrate, not substrate features (m3.5 §2) — these tests prove the
//! substrate carries each frozen format byte-exactly, so the M2 storage
//! swap changes STORAGE, never bytes.

use std::sync::Arc;

use crate::set::SpillFile;

/// The join-batch record contract (M2-L's format): reference images built
/// by the C-ported writer (`nodehashjoin::batch` —
/// `[u32 hashvalue][u32 len][payload][pad to 8]`, torn records fail
/// closed). The lx_join spill.rs face documents that the M2 BufFile/
/// SpillSet implementor "changes STORAGE, never bytes" — this golden is
/// that sentence as a test.
#[test]
fn join_batch_records_golden() {
    let (set, _dir, _cwd) = super::rig("records-join");

    // Reference stream via the donor writer.
    let mut reference: Vec<u8> = Vec::new();
    let recs: Vec<(u32, Vec<u8>)> = vec![
        (0xDEAD_BEEF, vec![1, 2, 3, 4, 5, 6, 7, 8]),
        (77, vec![9u8; 16]),
        (0xFFFF_FFFF, vec![42u8; 13]), // unpadded length: exercises pad-to-8
        (0, vec![]),
    ];
    for (h, img) in &recs {
        nodehashjoin::batch::batch_record_push(&mut reference, *h, img);
    }

    // Through the substrate and back: BYTE-EXACT.
    let mut file = SpillFile::new(Arc::clone(&set), "join-batch".to_string());
    let mut w = file.append().unwrap();
    w.begin_extent();
    w.write(&reference).unwrap();
    let ext = w.end_extent();
    w.finish().unwrap();

    let mut r = file.open_read();
    let mut got = Vec::new();
    r.read_extent(ext, &mut got).unwrap();
    assert_eq!(got, reference, "the substrate must carry the donor bytes verbatim");

    // And the donor READER parses the read-back stream to the same records.
    let mut br = nodehashjoin::batch::BatchRecords::new(&got);
    for (h, img) in &recs {
        let (gh, gimg) = br.next_rec().unwrap().expect("record present");
        assert_eq!((gh, gimg), (*h, &img[..]));
    }
    assert!(br.next_rec().unwrap().is_none());
}

/// The DistinctSet record contract (the FROZEN serial-spill formats,
/// distinctset.rs — stringhash inc-3 precedent, reused verbatim by the
/// m3.5 donor map): ints are raw `i64::to_ne_bytes`; byte values are
/// `u32 len + content`. NULLs never touch tapes (no record exists for
/// them — the seen_null-stays-in-memory rule).
#[test]
fn distinctset_records_golden() {
    let (set, _dir, _cwd) = super::rig("records-distinct");

    // Frozen fixtures, composed by the contract.
    let ints: Vec<i64> = vec![0, 1, -1, i64::MAX, i64::MIN, 42];
    let byteses: Vec<&[u8]> = vec![b"", b"a", b"hello world", &[0xFF; 300]];
    let mut reference: Vec<u8> = Vec::new();
    for v in &ints {
        reference.extend_from_slice(&v.to_ne_bytes());
    }
    for b in &byteses {
        reference.extend_from_slice(&(b.len() as u32).to_ne_bytes());
        reference.extend_from_slice(b);
    }

    let mut file = SpillFile::new(Arc::clone(&set), "distinct".to_string());
    let mut w = file.append().unwrap();
    w.begin_extent();
    w.write(&reference).unwrap();
    let ext = w.end_extent();
    w.finish().unwrap();

    let mut r = file.open_read();
    let mut got = Vec::new();
    r.read_extent(ext, &mut got).unwrap();
    assert_eq!(got, reference);

    // Decode back by the contract.
    let mut at = 0usize;
    for v in &ints {
        let g = i64::from_ne_bytes(got[at..at + 8].try_into().unwrap());
        assert_eq!(g, *v);
        at += 8;
    }
    for b in &byteses {
        let len = u32::from_ne_bytes(got[at..at + 4].try_into().unwrap()) as usize;
        at += 4;
        assert_eq!(&got[at..at + len], *b);
        at += len;
    }
    assert_eq!(at, got.len());
}
