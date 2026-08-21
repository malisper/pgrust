//! O-M2-3 parity pins: temp_file_limit enforcement rides the fd substrate
//! (error identity + recovery), and spill admission FAIL-CLOSES without
//! temp-file access — release-effective, per the debug-assert-masking law.

use std::sync::Arc;

use crate::page::PAGE_SIZE;
use crate::set::SpillFile;

/// temp_file_limit: crossing it inside a spill flush raises C's exact
/// error class (53400, "temporary file size exceeds temp_file_limit"),
/// accounted on the creating thread; the engagement recovers once the
/// limit is lifted.
#[test]
fn temp_file_limit_pinned() {
    let (set, _dir, _cwd) = super::rig("limits-tfl");
    let mut file = SpillFile::new(Arc::clone(&set), "tfl".to_string());

    let saved = guc_tables::vars::temp_file_limit.read();
    guc_tables::vars::temp_file_limit.write(1); // 1 kB

    // The writer buffers PAGE_SIZE then flushes: the flush must trip the
    // limit with C's identity.
    let mut w = file.append().unwrap();
    let err = w.write(&vec![1u8; PAGE_SIZE + 1]).unwrap_err();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED);
    let msg = format!("{err:?}");
    assert!(msg.contains("temp_file_limit"), "unexpected message: {msg}");
    drop(w); // abandon: commits nothing

    guc_tables::vars::temp_file_limit.write(saved);

    // Recovery: with the limit lifted the same engagement proceeds.
    assert_eq!(file.committed(), 0);
    let mut w = file.append().unwrap();
    w.write(&vec![2u8; PAGE_SIZE + 1]).unwrap();
    w.finish().unwrap();
    assert_eq!(file.committed(), PAGE_SIZE as u64 + 1);
}

/// The pool's unload path is under the same limit (its writes go through
/// the identical fd choke).
#[test]
fn temp_file_limit_governs_pool_unloads_too() {
    let (set, _dir, _cwd) = super::rig("limits-pool");
    let file = SpillFile::new(Arc::clone(&set), "tfl-pool".to_string());
    let mut pool = crate::SpillPool::new(2 * PAGE_SIZE, file);

    let saved = guc_tables::vars::temp_file_limit.read();
    guc_tables::vars::temp_file_limit.write(1);

    let pin = pool.alloc_var().unwrap();
    let id = pin.id();
    pool.unpin(pin);
    let err = pool.unload(id).unwrap_err();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED);
    // Failed write-back leaves the page RESIDENT (no torn unload).
    assert!(pool.is_resident(id));

    guc_tables::vars::temp_file_limit.write(saved);
    pool.unload(id).unwrap();
}

/// The admission probe: a thread whose fd substrate has no temp-file
/// access gets a typed ERROR from `SpillSet::create` — in RELEASE builds
/// too (the probe is a real branch, not a debug_assert).
#[test]
fn admission_fails_closed_without_temp_file_access() {
    // Set up the DATADIR on the main thread (holds the CWD lock).
    super::setup_thread();
    let (_dir, _cwd) = {
        let (d, g) = super::scratch_datadir("limits-probe");
        (d, g)
    };

    let err_is_err = std::thread::spawn(|| {
        super::setup_process();
        // VFD cache yes, InitTemporaryFileAccess NO — the M0-pool-worker
        // shape from m3.5 §6.2.
        fd::InitFileAccess();
        assert!(!fd::TempFileAccessReady());
        crate::SpillSet::create().is_err()
    })
    .join()
    .expect("probe thread");
    assert!(err_is_err, "SpillSet::create must fail closed without temp-file access");

    // And the probe reports ready where access exists (this thread).
    assert!(fd::TempFileAccessReady());
    let set = crate::SpillSet::create().unwrap();
    drop(set);
}
