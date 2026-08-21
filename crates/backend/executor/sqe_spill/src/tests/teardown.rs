//! The teardown contract, exercised path by path (crate docs, "The
//! teardown contract"): delete-all on NORMAL, ERROR, CANCEL, FATAL, and
//! CRASH-RESTART. Every test asserts the same postcondition — no
//! `pgsql_tmp*` entry survives — so the five paths are provably
//! IDENTICAL in outcome.

use std::sync::Arc;

use crate::set::{SpillFile, SpillSet};

fn fileset_dirs() -> Vec<String> {
    super::tmp_entries()
        .into_iter()
        .filter(|e| e.starts_with("pgsql_tmp"))
        .collect()
}

/// Build a set with real on-disk state: two files, one committed epoch
/// each. Returns (set, files).
fn populate(set: &Arc<SpillSet>) -> Vec<SpillFile> {
    let mut files = Vec::new();
    for w in 0..2u32 {
        let mut f = SpillFile::new(Arc::clone(set), crate::spill_file_name(9, 1, "td", w));
        let mut wr = f.append().unwrap();
        wr.write(&vec![w as u8; 100_000]).unwrap();
        wr.finish().unwrap();
        files.push(f);
    }
    assert_eq!(fileset_dirs().len(), 1, "populate leaves one fileset dir");
    files
}

/// Path 1 — NORMAL: payload drop deletes the tree.
#[test]
fn normal_completion_deletes_all() {
    let (set, _dir, _cwd) = super::rig("td-normal");
    let files = populate(&set);
    drop(files); // descriptors first (they hold Arc clones)
    drop(set); // last Arc: FileSet::drop = delete_all
    assert!(fileset_dirs().is_empty(), "normal drop must delete the fileset dir");
}

/// Path 2 — ERROR: an unwind out of a mid-event write. The writer's Drop
/// closes its handle (commits nothing); the payload drop deletes files.
#[test]
fn error_unwind_deletes_all_identically() {
    let (set, _dir, _cwd) = super::rig("td-error");
    let set2 = Arc::clone(&set);
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let mut f = SpillFile::new(set2, "err-file".to_string());
        let mut w = f.append().unwrap();
        w.write(&[7u8; 200_000]).unwrap(); // real flushed bytes
        panic!("simulated ERROR-path unwind mid-event");
    }));
    assert!(r.is_err(), "the unwind must have happened");
    assert_eq!(fileset_dirs().len(), 1, "files exist until the payload drops");
    drop(set);
    assert!(fileset_dirs().is_empty(), "error path must delete identically");
}

/// Path 3 — CANCEL: an ERROR-class unwind by construction; enumerated
/// separately because the acceptance bar names it. Same shape, same
/// postcondition — this test IS the "identically" witness for cancel.
#[test]
fn cancel_unwind_deletes_all_identically() {
    let (set, _dir, _cwd) = super::rig("td-cancel");
    let set2 = Arc::clone(&set);
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let mut f = SpillFile::new(set2, "cancel-file".to_string());
        let mut w = f.append().unwrap();
        w.write(&[8u8; 100_000]).unwrap();
        panic!("simulated statement-cancel unwind mid-event");
    }));
    assert!(r.is_err());
    drop(set);
    assert!(fileset_dirs().is_empty(), "cancel path must delete identically");
}

/// Path 4 — FATAL / proc_exit: handle Drops must SKIP FileClose (the
/// resowner release already freed temp VFDs; flushing through dead Files
/// is the aborted-postmaster precedent) while delete_all still unlinks by
/// path.
#[test]
fn fatal_proc_exit_skips_handles_but_deletes_files() {
    let (set, _dir, _cwd) = super::rig("td-fatal");
    let mut f = SpillFile::new(Arc::clone(&set), "fatal-file".to_string());
    let mut w = f.append().unwrap();
    w.write(&[9u8; 100_000]).unwrap();

    ::elog::config::set_proc_exit_inprogress(true);
    // Drop the writer mid-event under proc_exit: MUST NOT touch the VFD
    // (the kernel fd is deliberately left to die with the process).
    drop(w);
    drop(f);
    drop(set); // delete_all unlinks by path — no VFD dependency
    let gone = fileset_dirs().is_empty();
    ::elog::config::set_proc_exit_inprogress(false);
    assert!(gone, "FATAL path must delete the fileset tree");
}

/// Path 5 — CRASH-RESTART: no destructor ran; the startup reaper removes
/// every pgsql_tmp* entry (O-M2-3: crash cleanup rides the existing
/// reaper).
#[test]
fn crash_restart_reaper_removes_fileset_dirs() {
    let (set, _dir, _cwd) = super::rig("td-crash");
    let files = populate(&set);

    // Simulate the crash: nothing gets dropped.
    for f in files {
        core::mem::forget(f);
    }
    core::mem::forget(set);
    assert_eq!(fileset_dirs().len(), 1, "the crash left the tree behind");

    // Next boot: RemovePgTempFiles (fd's ported reaper) sweeps pgsql_tmp.
    fd::RemovePgTempFiles().unwrap();
    assert!(fileset_dirs().is_empty(), "the reaper must remove fileset dirs recursively");
}

/// The resowner belt behind path 2: a leaked mid-event VFD is closed by
/// transaction-abort cleanup (`AtEOXact_Files(false)`), and delete_all
/// still succeeds afterwards.
#[test]
fn resowner_belt_closes_leaked_vfds_at_abort() {
    let (set, _dir, _cwd) = super::rig("td-belt");
    let mut f = SpillFile::new(Arc::clone(&set), "belt-file".to_string());
    let w = {
        let mut w = f.append().unwrap();
        w.write(&[5u8; 100_000]).unwrap();
        w
    };
    // Strand the writer WITHOUT running its Drop: the open VFD leaks into
    // the abort path, exactly what a skipped destructor in an unwind chain
    // would leave.
    core::mem::forget(w);
    fd::AtEOXact_Files(false).unwrap();
    // f's committed watermark never moved (the abandoned event committed
    // nothing); the tree deletes normally.
    assert_eq!(f.committed(), 0);
    drop(f);
    drop(set);
    assert!(fileset_dirs().is_empty());
}
