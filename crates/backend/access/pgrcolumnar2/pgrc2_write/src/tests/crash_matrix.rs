//! The kill-9-shaped crash matrix (the M3-D non-negotiable; #253 law): the
//! publish-window crash sweep — crash at EVERY vfs-op boundary of a second
//! publish, revive, and prove exactly old-or-new, never an error, never a
//! third state:
//!
//! - crashed-before-commit (publisher resolves Aborted): the previous
//!   generation stays effective and its acked bytes are intact;
//! - completed publish + committed publisher: the new generation is fully
//!   durable after the crash (acked writes survive kill -9);
//! - recovery cleanup removes exactly the residue, and a retry publish
//!   succeeds afterward.
//!
//! This is the in-crate, default-CI arm; M3-K's batteries compose the same
//! publish path with the tree's SimVfs (sector tearing, seeded dirent
//! subsets) and the real-postmaster kill -9 ladders.

use super::*;
use crate::publish::{effective_manifest, recover_and_clean, TxnVerdict};
use crate::WriteResult;

struct Scenario {
    vfs: MemVfs,
    gen1_part: Vec<u8>,
    publish_completed: bool,
    ops_before_window: u64,
}

/// Gen 1 by committed txn 100 (1000 rows), then txn 200 attempts gen 2
/// (500 rows) with an optional crash armed `k` ops into its window.
fn scenario(crash_at: Option<u64>) -> Scenario {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let probe = Probe::new(TxnVerdict::InProgress).set(100, TxnVerdict::Committed);
    let mut w1 = open_writer(vec![int8_col(1)], stamp(100, 1));
    append_int8_rows(&mut w1, &mut vfs, &mut kit, 1000, |i| Some(i as i64));
    finish_and_publish(&mut w1, &mut vfs, &mut kit, &probe);
    let gen1_part = vfs.read_full(&format!("{DIR}/part-0.pgrc2")).expect("part");

    let mut w2 = open_writer(vec![int8_col(1)], stamp(200, 1));
    append_int8_rows(&mut w2, &mut vfs, &mut kit, 500, |i| Some(i as i64 * 3));
    let ops_before_window = vfs.op_count();
    if let Some(k) = crash_at {
        vfs.crash_at_op(k);
    }
    let publish_completed = (|| -> WriteResult<()> {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w2.finish(&mut env)?;
        w2.publish(&mut vfs, &probe)?;
        Ok(())
    })()
    .is_ok();
    Scenario {
        vfs,
        gen1_part,
        publish_completed,
        ops_before_window,
    }
}

#[test]
fn publish_window_crash_sweep() {
    // Rehearse to measure the window (seal + publish op span).
    let rehearsal = scenario(None);
    assert!(rehearsal.publish_completed);
    let window = rehearsal.vfs.op_count() - rehearsal.ops_before_window;
    assert!(window > 10, "window too small to be a real sweep: {window}");
    // [fmt-land] The §13.3 CORE ends at the step-4 dir fsync; the ops past
    // it are the best-effort stats-plane suite (bankplane.rs swallows its
    // own failures by doctrine), so a kill inside the plane window returns
    // a COMPLETED publish with a skipped plane — old-or-new still holds.
    let core_end = rehearsal.vfs.ops()[rehearsal.ops_before_window as usize..]
        .iter()
        .position(|o| o.starts_with("fsyncdir"))
        .expect("step-4 fsyncdir in window") as u64
        + 1;
    assert!(core_end < window, "plane ops follow the core");

    for k in 1..=window {
        let mut s = scenario(Some(k));
        assert_eq!(
            s.publish_completed,
            k > core_end,
            "crash at op {k} (core_end {core_end}) publish-completed skew"
        );
        s.vfs.crash_and_revive();
        // Crash resolution: txn 200 never committed ⇒ Aborted.
        let probe = Probe::new(TxnVerdict::Aborted).set(100, TxnVerdict::Committed);

        // OLD state exactly: gen 1 effective, acked bytes intact.
        let eff = effective_manifest(&mut s.vfs, DIR, &probe)
            .expect("recovery must never error")
            .expect("gen 1 must stay effective");
        assert_eq!(eff.header.gen, 1, "crash at op {k}");
        assert_eq!(
            eff.parts.iter().map(|p| p.part_no).collect::<Vec<_>>(),
            vec![0]
        );
        let part0 = s.vfs.read_full(&format!("{DIR}/part-0.pgrc2")).expect("part-0");
        assert!(part0 == s.gen1_part, "acked part bytes damaged (crash at op {k})");

        // [fmt-land] The plane crash tooth: at every kill point, a file at
        // the FINAL plane name is complete + decodable (fsync-before-
        // rename law) — a torn image can only ever exist under `.tmp`.
        if let Ok(pb) = s.vfs.read_full(&format!("{DIR}/bankstats-2.pgrc2bs")) {
            let (h, _, _) =
                pgrc2_format::bankstats::decode_meta(&pb).expect("final-name plane must decode");
            assert_eq!(h.gen, 2);
        }

        // Cleanup removes exactly the residue.
        let rep = recover_and_clean(&mut s.vfs, DIR, &probe).expect("clean");
        assert_eq!(rep.effective_gen, 1);
        let names = s.vfs.list_dir(DIR).expect("list");
        for n in &names {
            assert!(
                n == "CURRENT"
                    || n == "manifest-1.pgrc2m"
                    || n == "part-0.pgrc2"
                    || n == "bankstats-1.pgrc2bs",
                "residue survived cleanup at op {k}: {n} (all: {names:?})"
            );
        }
        // [fmt-land] the effective generation's plane survived the crash +
        // cleanup intact (it was fsync'd durable inside gen 1's publish)
        // and still validates against the effective manifest.
        {
            let pb = s
                .vfs
                .read_full(&format!("{DIR}/bankstats-1.pgrc2bs"))
                .expect("gen-1 plane survives");
            let (h, pi, _) = pgrc2_format::bankstats::decode_meta(&pb).expect("valid plane");
            assert_eq!(h.gen, 1);
            assert_eq!(pi.len(), eff.parts.len());
        }

        // Retry: a fresh txn publishes gen 2 successfully.
        let mut kit = Kit::new();
        let probe_retry = Probe::new(TxnVerdict::InProgress)
            .set(100, TxnVerdict::Committed)
            .set(300, TxnVerdict::Committed);
        let mut w3 = open_writer(vec![int8_col(1)], stamp(300, 1));
        append_int8_rows(&mut w3, &mut s.vfs, &mut kit, 500, |i| Some(i as i64 * 5));
        let out = finish_and_publish(&mut w3, &mut s.vfs, &mut kit, &probe_retry);
        assert_eq!(out.gen, 2, "retry after crash at op {k}");
        let eff2 = effective_manifest(&mut s.vfs, DIR, &probe_retry)
            .expect("eff")
            .expect("gen 2 effective");
        assert_eq!(eff2.parts.iter().map(|p| p.rows).sum::<u64>(), 1500);
    }
}

/// The acked arm: a COMPLETED publish whose transaction committed survives
/// kill -9 wholesale — every byte of the new generation is durable.
#[test]
fn acked_publish_survives_crash() {
    let mut s = scenario(None);
    assert!(s.publish_completed);
    let part1_before = s.vfs.read_full(&format!("{DIR}/part-1.pgrc2")).expect("part-1");
    s.vfs.crash_and_revive();
    let probe = Probe::new(TxnVerdict::Aborted)
        .set(100, TxnVerdict::Committed)
        .set(200, TxnVerdict::Committed); // 200's commit record made it
    let eff = effective_manifest(&mut s.vfs, DIR, &probe)
        .expect("eff")
        .expect("gen 2 effective");
    assert_eq!(eff.header.gen, 2);
    assert_eq!(
        eff.parts.iter().map(|p| p.part_no).collect::<Vec<_>>(),
        vec![0, 1]
    );
    let part0 = s.vfs.read_full(&format!("{DIR}/part-0.pgrc2")).expect("part-0");
    let part1 = s.vfs.read_full(&format!("{DIR}/part-1.pgrc2")).expect("part-1");
    assert!(part0 == s.gen1_part, "gen-1 bytes damaged");
    assert!(part1 == part1_before, "acked gen-2 bytes not durable");
    // [fmt-land] The one legal removal: gen 1's stats plane (derived,
    // superseded by gen 2's — its witness can never match again).
    let rep = recover_and_clean(&mut s.vfs, DIR, &probe).expect("clean");
    assert_eq!(rep.effective_gen, 2);
    assert_eq!(rep.removed, vec!["bankstats-1.pgrc2bs".to_string()]);
}

/// The #254-shaped window: publish COMPLETED (all four steps durable) but
/// the transaction's commit record never made it — the generation is
/// structurally invisible (clog fence), the old generation stays effective,
/// and a retry RECLAIMS the durable dead residue (manifest name via
/// create-truncate, part name via rename-overwrite) without any cleanup
/// pass.
#[test]
fn uncommitted_completed_publish_is_invisible_and_residue_reclaims() {
    let mut s = scenario(None);
    assert!(s.publish_completed);
    s.vfs.crash_and_revive();
    // Commit record lost: txn 200 resolves Aborted. All gen-2 FILES are
    // durable (dir fsync ran) — the clog fence alone hides them.
    let probe = Probe::new(TxnVerdict::Aborted).set(100, TxnVerdict::Committed);
    assert!(
        s.vfs
            .exists_path(&format!("{DIR}/manifest-2.pgrc2m"))
            .expect("stat"),
        "dead gen-2 residue should be durable in this window"
    );
    let eff = effective_manifest(&mut s.vfs, DIR, &probe)
        .expect("eff")
        .expect("gen 1 effective");
    assert_eq!(eff.header.gen, 1, "aborted publish structurally invisible");
    let part0 = s.vfs.read_full(&format!("{DIR}/part-0.pgrc2")).expect("part-0");
    assert!(part0 == s.gen1_part);

    // Retry WITHOUT cleanup: gen 2 and part-1 names are reclaimed.
    let mut kit = Kit::new();
    let probe_retry = Probe::new(TxnVerdict::Aborted)
        .set(100, TxnVerdict::Committed)
        .set(300, TxnVerdict::Committed);
    let mut w3 = open_writer(vec![int8_col(1)], stamp(300, 1));
    append_int8_rows(&mut w3, &mut s.vfs, &mut kit, 10, |i| Some(i as i64));
    let out = finish_and_publish(&mut w3, &mut s.vfs, &mut kit, &probe_retry);
    assert_eq!(out.gen, 2);
    assert_eq!(out.part_nos, vec![1]);
    let eff2 = effective_manifest(&mut s.vfs, DIR, &probe_retry)
        .expect("eff")
        .expect("gen 2");
    assert_eq!(eff2.header.publisher_fxid, 300, "residue reclaimed");
    assert_eq!(eff2.parts.iter().map(|p| p.rows).sum::<u64>(), 1010);
}
