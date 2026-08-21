//! Publish-ordering unit tests (M3-D slice leg 7; spec §13.3): data fsync →
//! manifest fsync → CURRENT (tmp+fsync+rename+fsync) → dir fsync, with the
//! clog fence present in the manifest bytes — witnessed through the MemVfs
//! op log (the sim_dirsync op-trace pattern).

use super::*;
use crate::publish::{effective_manifest, TxnVerdict};

fn idx(ops: &[String], needle: &str) -> usize {
    ops.iter()
        .position(|o| o.contains(needle))
        .unwrap_or_else(|| panic!("op containing {needle:?} not found in {ops:#?}"))
}

/// Exact-op matcher for ops whose string is fully determined (`fsync <path>`)
/// — a contains-match on `fsync …/CURRENT` would also hit `…/CURRENT.tmp`.
fn idx_eq(ops: &[String], op: &str) -> usize {
    ops.iter()
        .position(|o| o == op)
        .unwrap_or_else(|| panic!("op {op:?} not found in {ops:#?}"))
}

#[test]
fn five_step_ordering_holds_and_clog_fence_present() {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut w = open_writer(vec![int8_col(1)], stamp(7, 1));
    append_int8_rows(&mut w, &mut vfs, &mut kit, 1000, |i| Some(i as i64));
    let probe = Probe::new(TxnVerdict::InProgress).set(7, TxnVerdict::Committed);
    let out = finish_and_publish(&mut w, &mut vfs, &mut kit, &probe);
    assert_eq!(out.gen, 1);
    assert_eq!(out.part_nos, vec![0]);
    // The #253 fence: seam-guarded exactly like the cbstore precedent —
    // uninstalled outside a backend, so unit tests witness the FIELD (the
    // arm code path is the same `is_installed()` guard the old writer
    // carries; the installed arm is proven by M3-H/K's server batteries).
    assert!(!out.commit_fence_armed);

    let ops: Vec<String> = vfs.ops().to_vec();
    // Step 1: part data fsync'd, then renamed into the readable namespace.
    let part_fsync = idx(&ops, "fsync /tbl/pgrc2_777/tmp-7-0.pgrc2t");
    let part_rename = idx(&ops, "rename /tbl/pgrc2_777/tmp-7-0.pgrc2t -> /tbl/pgrc2_777/part-0.pgrc2");
    // Step 2: manifest written + fsync'd.
    let manifest_write = idx(&ops, "pwrite /tbl/pgrc2_777/manifest-1.pgrc2m");
    let manifest_fsync = idx(&ops, "fsync /tbl/pgrc2_777/manifest-1.pgrc2m");
    // Step 3: CURRENT via tmp + fsync + rename + file fsync.
    let cur_tmp_fsync = idx_eq(&ops, "fsync /tbl/pgrc2_777/CURRENT.tmp");
    let cur_rename = idx(&ops, "rename /tbl/pgrc2_777/CURRENT.tmp -> /tbl/pgrc2_777/CURRENT");
    let cur_fsync = idx_eq(&ops, "fsync /tbl/pgrc2_777/CURRENT");
    // Step 4: the directory fsync, last.
    let dir_fsync = idx(&ops, "fsyncdir /tbl/pgrc2_777");
    assert!(part_fsync < part_rename, "part durable before rename");
    assert!(part_rename < manifest_write, "parts precede the manifest");
    assert!(manifest_write < manifest_fsync);
    assert!(manifest_fsync < cur_tmp_fsync, "manifest durable before CURRENT");
    assert!(cur_tmp_fsync < cur_rename);
    assert!(cur_rename < cur_fsync);
    assert!(cur_fsync < dir_fsync, "dir fsync is step 4");
    // [fmt-land] The §13.3 window still ends at the dirent barrier; the
    // ONLY ops past it are the derived stats-plane suite (its own
    // tmp+fsync+rename+fsyncdir — best-effort, outside the crash story's
    // old-or-new core, witnessed here in order).
    let tail = &ops[dir_fsync + 1..];
    assert!(
        tail.iter().all(|o| o.contains("bankstats-1.pgrc2bs") || o.contains("fsyncdir")),
        "non-plane op past the dirent barrier: {tail:#?}"
    );
    let p_write = idx(&ops, "pwrite /tbl/pgrc2_777/bankstats-1.pgrc2bs.tmp");
    let p_fsync = idx(&ops, "fsync /tbl/pgrc2_777/bankstats-1.pgrc2bs.tmp");
    let p_rename = idx(
        &ops,
        "rename /tbl/pgrc2_777/bankstats-1.pgrc2bs.tmp -> /tbl/pgrc2_777/bankstats-1.pgrc2bs",
    );
    assert!(dir_fsync < p_write && p_write < p_fsync && p_fsync < p_rename);
    assert_eq!(
        ops.len() - 1,
        ops.iter().rposition(|o| o.contains("fsyncdir")).unwrap(),
        "the plane's own dirent barrier closes the publish"
    );
    assert!(matches!(
        out.bankstats_plane,
        crate::bankplane::PlaneOutcome::Written { .. }
    ));

    // Clog fence: the manifest carries the publisher's FullTransactionId.
    let m = read_manifest(&mut vfs, 1);
    assert_eq!(m.header.publisher_fxid, 7);
    assert_eq!(m.header.prev_gen, 0);
    assert_eq!(m.header.part_count, 1);
    assert_eq!(m.header.next_part_no, 1);
    assert_eq!(m.parts[0].rows, 1000);

    // CURRENT hint: pointer decodes and matches the manifest image.
    let cp_bytes = vfs.read_full(&format!("{DIR}/CURRENT")).expect("CURRENT");
    let cp = pgrc2_format::manifest::CommitPointer::decode(&cp_bytes).expect("pointer");
    assert_eq!(cp.gen, 1);
    let m_bytes = vfs
        .read_full(&format!("{DIR}/manifest-1.pgrc2m"))
        .expect("manifest bytes");
    assert_eq!(cp.manifest_len, m_bytes.len() as u64);
    // The pointer pins the manifest's OWN trailing crc (body-only) — the
    // exact check pgrc2_read::resolve_effective enforces (M3-H composition
    // reconciliation; the whole-file form was never readable).
    assert_eq!(
        cp.manifest_crc,
        pgrc2_format::wire::crc32c(&m_bytes[..m_bytes.len() - 4])
    );
    assert_eq!(
        cp.manifest_crc,
        u32::from_le_bytes(m_bytes[m_bytes.len() - 4..].try_into().expect("crc word"))
    );

    // Effectiveness is clog-fenced: committed ⇒ effective; in-progress or
    // aborted ⇒ structurally invisible.
    let eff = effective_manifest(&mut vfs, DIR, &probe).expect("eff");
    assert_eq!(eff.expect("some").header.gen, 1);
    let cold = Probe::new(TxnVerdict::InProgress);
    assert!(effective_manifest(&mut vfs, DIR, &cold).expect("eff").is_none());
    let aborted = Probe::new(TxnVerdict::Aborted);
    assert!(effective_manifest(&mut vfs, DIR, &aborted).expect("eff").is_none());
}

/// Generation chaining: a second committed publish chains prev_gen and
/// carries the base's parts forward; part_no stays monotone.
#[test]
fn generations_chain_and_part_numbers_are_monotone() {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let probe = Probe::new(TxnVerdict::InProgress)
        .set(7, TxnVerdict::Committed)
        .set(8, TxnVerdict::Committed);
    let mut w1 = open_writer(vec![int8_col(1)], stamp(7, 1));
    append_int8_rows(&mut w1, &mut vfs, &mut kit, 100, |i| Some(i as i64));
    let o1 = finish_and_publish(&mut w1, &mut vfs, &mut kit, &probe);
    assert_eq!((o1.gen, o1.part_nos.clone()), (1, vec![0]));
    let mut w2 = open_writer(vec![int8_col(1)], stamp(8, 1));
    append_int8_rows(&mut w2, &mut vfs, &mut kit, 200, |i| Some(i as i64 + 7));
    let o2 = finish_and_publish(&mut w2, &mut vfs, &mut kit, &probe);
    assert_eq!((o2.gen, o2.part_nos.clone()), (2, vec![1]));
    let m = read_manifest(&mut vfs, 2);
    assert_eq!(m.header.prev_gen, 1);
    assert_eq!(m.header.part_count, 2);
    assert_eq!(m.parts.iter().map(|p| p.part_no).collect::<Vec<_>>(), vec![0, 1]);
    assert_eq!(m.parts.iter().map(|p| p.rows).sum::<u64>(), 300);
}
