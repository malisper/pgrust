//! PINNED product defects found by this battery (filed, minimized, NOT
//! fixed here — product internals are out of this lane's write set). Each
//! pin asserts the exact defect signature on the RAW product output, so the
//! moment a fix lands this file goes RED and the corresponding workaround
//! in pgrc2_qa (named in each pin) must be deleted along with the pin —
//! flipping the batteries to full strength.
//!
//! - issue #461 (CommitPointer crc convention) was FIXED on lanev3 by
//!   #473 — its pin fired as designed 2026-08-09 and the repin workaround
//!   was deleted; the reader-walk assertions run at full strength now.
//! - issue #462 (recover_and_clean left CURRENT dangling at a removed dead
//!   generation) was FIXED on this branch — its pin fired as designed and
//!   was replaced by `regress_issue_462_*` below; the tolerated
//!   `ManifestMissing` arms in `harness::check_dir` and the SimVfs battery
//!   became STRICT post-recovery walk-agreement checks, and the battery
//!   gained the crash-during-recovery sweep (tests/simvfs_battery.rs).
//! - issue #465 (seal width-key defect) was FIXED on lanev3 by the M3-A2
//!   amendment batch — seal keys entry/header width AND the verify vtable
//!   on the elected `EncoderFactory::key()` through the format-crate
//!   `stream_kernel_key` normalization. Its two pins fired as designed and
//!   were deleted; the parked corpus arms (BYTE_FOR w2, ALP, ALP_RD,
//!   DELTA_FOR) are re-armed at full strength.

use pgrc2_qa::adapters::{memdir_of, Probe};
use pgrc2_qa::corpus::{append_fixture_rows, finish, int8_fixture, open_writer, Fixture};
use pgrc2_qa::simvfs::SimVfs;
use pgrc2_format::dirlayout::{manifest_file_name, CURRENT_FILE_NAME};
use pgrc2_format::manifest::CommitPointer;
use pgrc2_format::wire::crc32c;
use pgrc2_read::manifest_walk::{resolve_effective, TableExpect};
use pgrc2_write::publish::{effective_manifest, recover_and_clean, TxnVerdict};
use pgrc2_write::writer::PartCutPolicy;
use pgrc2_write::wvfs::WriteVfs;

fn tiny_fixture(relf: u64) -> Fixture {
    int8_fixture(
        "known_issue",
        relf,
        64,
        Vec::new(),
        PartCutPolicy::default(),
        |i| Some(i as i64),
    )
}

fn publish_one(vfs: &mut SimVfs, fx: &Fixture, fxid: u64, probe: &Probe) {
    let mut w = open_writer(fx, fxid).expect("open");
    append_fixture_rows(vfs, &mut w, fx, 0, fx.rows()).expect("rows");
    finish(vfs, &mut w, fx).expect("finish");
    w.publish(vfs, probe).expect("publish");
}

/// Issue #462 regression (the exact minimized repro of the filed pin — the
/// pin fired RED against the fix as designed, per the #461 precedent, and
/// became this positive test): cleanup of a durable-but-uncommitted gen2
/// must repoint CURRENT to gen1, and BOTH walks must agree afterwards.
#[test]
fn regress_issue_462_recovery_repoints_current() {
    let fx = tiny_fixture(802);
    let mut vfs = SimVfs::new();
    vfs.mkdir_path(&fx.dir).expect("mkdir");
    // gen1 committed.
    let mut probe = Probe::new(TxnVerdict::Aborted);
    probe.mark(9_802, TxnVerdict::InProgress);
    publish_one(&mut vfs, &fx, 9_802, &probe);
    probe.mark(9_802, TxnVerdict::Committed);
    // gen2 durably published, never committed.
    probe.mark(9_803, TxnVerdict::InProgress);
    publish_one(&mut vfs, &fx, 9_803, &probe);
    probe.mark(9_803, TxnVerdict::Aborted);

    // Cleanup removes the dead manifest-2 AND repoints CURRENT (#462 law).
    let report = recover_and_clean(&mut vfs, &fx.dir, &probe).expect("clean");
    assert!(
        report.removed.iter().any(|n| n == &manifest_file_name(2)),
        "premise: dead gen2 reclaimed"
    );
    assert!(report.current_repointed, "CURRENT was not repointed");
    // CURRENT itself now pins gen1 with the reader-enforced facts.
    let cur = vfs
        .read_full(&format!("{}/{}", fx.dir, CURRENT_FILE_NAME))
        .expect("CURRENT");
    let cp = CommitPointer::decode(&cur).expect("pointer decodes");
    assert_eq!(cp.gen, 1);
    let mb = vfs
        .read_full(&format!("{}/{}", fx.dir, manifest_file_name(1)))
        .expect("manifest-1");
    assert_eq!(cp.manifest_len, mb.len() as u64);
    assert_eq!(cp.manifest_crc, crc32c(&mb[..mb.len() - 4]));
    // Writer walk: gen1 straight from the hint (no scan needed).
    let eff = effective_manifest(&mut vfs, &fx.dir, &probe)
        .expect("effective")
        .expect("gen1");
    assert_eq!(eff.header.gen, 1);
    // Reader walk AGREES — the #462 exit criterion.
    let files = vfs.snapshot_dir(&fx.dir);
    let rdir = memdir_of(&files);
    match resolve_effective(&rdir, &probe, &TableExpect::default()) {
        Ok(Some(r)) => assert_eq!(r.manifest.header.gen, 1),
        other => panic!("reader walk must agree on gen1, got {other:?}"),
    }
    // Idempotent: a second recovery is quiescent.
    let again = recover_and_clean(&mut vfs, &fx.dir, &probe).expect("re-clean");
    assert!(!again.current_repointed, "second recovery repointed again");
    assert!(again.removed.is_empty(), "second recovery removed {:?}", again.removed);
}

/// Issue #462, empty-table shape: the ONLY generation is durable but
/// uncommitted. Recovery must unlink the dangling CURRENT (absent CURRENT
/// IS the reader's empty-table posture), not leave a typed refusal.
#[test]
fn regress_issue_462_empty_table_unlinks_current() {
    let fx = tiny_fixture(805);
    let mut vfs = SimVfs::new();
    vfs.mkdir_path(&fx.dir).expect("mkdir");
    let mut probe = Probe::new(TxnVerdict::Aborted);
    probe.mark(9_806, TxnVerdict::InProgress);
    publish_one(&mut vfs, &fx, 9_806, &probe);
    probe.mark(9_806, TxnVerdict::Aborted);

    let report = recover_and_clean(&mut vfs, &fx.dir, &probe).expect("clean");
    assert!(
        report.removed.iter().any(|n| n == &manifest_file_name(1)),
        "premise: dead gen1 reclaimed"
    );
    assert!(report.current_repointed, "CURRENT was not unlinked");
    assert_eq!(report.effective_gen, 0);
    assert!(
        !vfs.exists_path(&format!("{}/{}", fx.dir, CURRENT_FILE_NAME)).expect("stat"),
        "CURRENT survived on an empty table"
    );
    // Both walks agree on EMPTY.
    assert!(effective_manifest(&mut vfs, &fx.dir, &probe).expect("effective").is_none());
    let files = vfs.snapshot_dir(&fx.dir);
    let rdir = memdir_of(&files);
    match resolve_effective(&rdir, &probe, &TableExpect::default()) {
        Ok(None) => {}
        other => panic!("reader walk must see the empty table, got {other:?}"),
    }
}

/// The legal state the #462 fix must NOT touch: CURRENT naming an intact,
/// genuinely in-progress candidate (the §13.3 pre-commit posture).
#[test]
fn issue_462_fix_leaves_in_progress_candidate_alone() {
    let fx = tiny_fixture(806);
    let mut vfs = SimVfs::new();
    vfs.mkdir_path(&fx.dir).expect("mkdir");
    let mut probe = Probe::new(TxnVerdict::Aborted);
    probe.mark(9_807, TxnVerdict::InProgress);
    publish_one(&mut vfs, &fx, 9_807, &probe);
    probe.mark(9_807, TxnVerdict::Committed);
    probe.mark(9_808, TxnVerdict::InProgress);
    publish_one(&mut vfs, &fx, 9_808, &probe);
    // 9_808 stays IN PROGRESS: gen2 is the legal pre-commit candidate.
    let report = recover_and_clean(&mut vfs, &fx.dir, &probe).expect("clean");
    assert!(!report.current_repointed, "repointed a legal pre-commit CURRENT");
    assert_eq!(report.effective_gen, 1);
    let cur = vfs
        .read_full(&format!("{}/{}", fx.dir, CURRENT_FILE_NAME))
        .expect("CURRENT");
    assert_eq!(CommitPointer::decode(&cur).expect("pointer").gen, 2);
    // Reader walks past the uncommitted candidate to gen1.
    let files = vfs.snapshot_dir(&fx.dir);
    let rdir = memdir_of(&files);
    match resolve_effective(&rdir, &probe, &TableExpect::default()) {
        Ok(Some(r)) => {
            assert_eq!(r.manifest.header.gen, 1);
            assert_eq!(r.walked_past, 1);
        }
        other => panic!("reader walk must reach gen1 past the candidate, got {other:?}"),
    }
}
