//! [fmt-land] Seal-time bank-grain stats plane: birth, carry-forward,
//! kill switch, and refusal semantics (the crash story rides
//! `crash_matrix.rs`'s op-boundary sweep, extended over the plane window).

use super::*;
use crate::bankplane::PlaneOutcome;
use crate::publish::TxnVerdict;
use pgrc2_format::bankstats as fb;

fn plane_path(gen: u64) -> String {
    format!("{DIR}/{}", fb::bankstats_file_name(gen))
}

fn read_plane(
    vfs: &mut MemVfs,
    gen: u64,
) -> (fb::BankStatsHeader, Vec<fb::PartIdent>, Vec<fb::ColDirEntry>, Vec<u8>) {
    let bytes = vfs.read_full(&plane_path(gen)).expect("plane bytes");
    let (h, p, c) = fb::decode_meta(&bytes).expect("plane decodes");
    (h, p, c, bytes)
}

/// Fresh table: the FIRST publish writes a valid plane bound to gen 1 —
/// no offline builder involved (the freshly-written-banks law).
#[test]
fn fresh_publish_writes_valid_plane() {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let probe = Probe::new(TxnVerdict::InProgress).set(7, TxnVerdict::Committed);
    let mut w = open_writer(vec![int8_col(1)], stamp(7, 1));
    append_int8_rows(&mut w, &mut vfs, &mut kit, 1000, |i| Some(i as i64));
    let out = finish_and_publish(&mut w, &mut vfs, &mut kit, &probe);
    assert!(matches!(out.bankstats_plane, PlaneOutcome::Written { .. }));

    let m = read_manifest(&mut vfs, 1);
    let (h, pidents, cols, bytes) = read_plane(&mut vfs, 1);
    assert_eq!(h.gen, 1);
    assert_eq!(h.schema_fingerprint, m.header.schema_fingerprint);
    assert_eq!(h.part_count, 1);
    assert_eq!(cols.len(), 1);
    assert_eq!(pidents[0].part_no, m.parts[0].part_no);
    assert_eq!(pidents[0].rows, m.parts[0].rows);
    assert_eq!(pidents[0].footer_off, m.parts[0].footer_off);
    assert_eq!(pidents[0].granule_count, m.parts[0].granule_count);
    assert_eq!(pidents[0].band_count, m.parts[0].band_count);
    // Every column payload validates + serves a non-empty §8.1 body.
    for e in &cols {
        let payload = &bytes[e.off as usize..(e.off + e.len) as usize];
        let v = fb::ColPayloadRef::new(e, h.part_count, payload).expect("col validates");
        assert!(v.stats_body(0).expect("stats body present").len() >= 80);
    }
}

/// Append publish: gen 2's plane carries gen 1's slices forward
/// byte-for-byte and appends the new part's.
#[test]
fn append_publish_carries_prior_slices_forward() {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let probe = Probe::new(TxnVerdict::InProgress)
        .set(7, TxnVerdict::Committed)
        .set(9, TxnVerdict::Committed);
    let mut w1 = open_writer(vec![int8_col(1)], stamp(7, 1));
    append_int8_rows(&mut w1, &mut vfs, &mut kit, 1000, |i| Some(i as i64));
    finish_and_publish(&mut w1, &mut vfs, &mut kit, &probe);
    let (h1, _, c1, b1) = read_plane(&mut vfs, 1);

    let mut w2 = open_writer(vec![int8_col(1)], stamp(9, 1));
    append_int8_rows(&mut w2, &mut vfs, &mut kit, 500, |i| Some(i as i64 * 3));
    let out2 = finish_and_publish(&mut w2, &mut vfs, &mut kit, &probe);
    assert!(matches!(out2.bankstats_plane, PlaneOutcome::Written { .. }));

    let (h2, pi2, c2, b2) = read_plane(&mut vfs, 2);
    assert_eq!(h2.gen, 2);
    assert_eq!(h2.part_count, 2);
    assert_eq!(pi2.len(), 2);
    let v1 = fb::ColPayloadRef::new(&c1[0], h1.part_count, &b1[c1[0].off as usize..(c1[0].off + c1[0].len) as usize])
        .expect("gen1 col");
    let v2 = fb::ColPayloadRef::new(&c2[0], h2.part_count, &b2[c2[0].off as usize..(c2[0].off + c2[0].len) as usize])
        .expect("gen2 col");
    // Part 0's slice carried forward byte-for-byte; part 1's is new.
    assert_eq!(v1.stats_body(0), v2.stats_body(0), "carry-forward bytes");
    assert_eq!(v1.digest(0), v2.digest(0));
    assert!(v2.stats_body(1).is_some());
}

/// A missing prior plane on an append publish = NoPriorPlane (backfill
/// tool's job); the publish itself succeeds and gen 2 has no plane.
#[test]
fn append_without_prior_plane_skips() {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let probe = Probe::new(TxnVerdict::InProgress)
        .set(7, TxnVerdict::Committed)
        .set(9, TxnVerdict::Committed);
    let mut w1 = open_writer(vec![int8_col(1)], stamp(7, 1));
    append_int8_rows(&mut w1, &mut vfs, &mut kit, 1000, |i| Some(i as i64));
    finish_and_publish(&mut w1, &mut vfs, &mut kit, &probe);
    vfs.unlink_path(&plane_path(1)).expect("drop plane");
    vfs.fsync_dir(DIR).expect("dir");

    let mut w2 = open_writer(vec![int8_col(1)], stamp(9, 1));
    append_int8_rows(&mut w2, &mut vfs, &mut kit, 500, |i| Some(i as i64 * 3));
    let out2 = finish_and_publish(&mut w2, &mut vfs, &mut kit, &probe);
    assert_eq!(out2.bankstats_plane, PlaneOutcome::NoPriorPlane);
    assert!(!vfs.exists_path(&plane_path(2)).expect("exists"));
    // The table itself is fine.
    let m = read_manifest(&mut vfs, 2);
    assert_eq!(m.parts.len(), 2);
}

/// A STALE prior plane (valid bytes, wrong part-set — the forged-identity
/// class) is refused on carry-forward, not misread.
#[test]
fn stale_prior_plane_is_refused() {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let probe = Probe::new(TxnVerdict::InProgress)
        .set(7, TxnVerdict::Committed)
        .set(9, TxnVerdict::Committed);
    let mut w1 = open_writer(vec![int8_col(1)], stamp(7, 1));
    append_int8_rows(&mut w1, &mut vfs, &mut kit, 1000, |i| Some(i as i64));
    finish_and_publish(&mut w1, &mut vfs, &mut kit, &probe);
    // Forge: bump a PartIdent rows field + recompute the meta crc (a
    // structurally well-formed plane bound to a DIFFERENT part-set).
    let mut bytes = vfs.read_full(&plane_path(1)).expect("plane");
    let rows_off = fb::BANKSTATS_HEADER_LEN + 8; // PartIdent[0].rows
    let rows = u64::from_le_bytes(bytes[rows_off..rows_off + 8].try_into().unwrap());
    bytes[rows_off..rows_off + 8].copy_from_slice(&(rows + 1).to_le_bytes());
    let (h, _, _) = {
        // meta_len at header offset 32.
        let ml = u64::from_le_bytes(bytes[32..40].try_into().unwrap()) as usize;
        let crc = pgrc2_format::wire::crc32c(&bytes[..ml - 4]);
        bytes[ml - 4..ml].copy_from_slice(&crc.to_le_bytes());
        fb::decode_meta(&bytes).expect("forged plane still decodes")
    };
    assert_eq!(h.gen, 1);
    let fd = vfs.create_rw(&plane_path(1)).expect("rw");
    vfs.pwrite_at(&fd, 0, &bytes).expect("pwrite");
    vfs.fsync_file(&fd).expect("fsync");
    vfs.close_file(fd).expect("close");

    let mut w2 = open_writer(vec![int8_col(1)], stamp(9, 1));
    append_int8_rows(&mut w2, &mut vfs, &mut kit, 500, |i| Some(i as i64 * 3));
    let out2 = finish_and_publish(&mut w2, &mut vfs, &mut kit, &probe);
    assert_eq!(out2.bankstats_plane, PlaneOutcome::NoPriorPlane);
}
