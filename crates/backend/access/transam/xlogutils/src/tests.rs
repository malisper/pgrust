//! Unit tests for the in-crate (non-seamed) logic of the xlogutils port: the
//! invalid-page table, the drop/truncate hooks, the file-name / path
//! formatting, and the LSN formatting.
//!
//! The handful of seams these paths touch (`relpathbackend`,
//! `reached_consistency`, `smgrdestroyall`) are process-global `OnceLock`
//! slots, so these tests must run single-threaded (`--test-threads=1`). The
//! invalid-page table and `ignore_invalid_pages` are per-backend
//! `thread_local`s, shared across tests on the same thread, so each test
//! resets them first.

use super::*;

use core::sync::atomic::{AtomicBool, Ordering};
use ::types_core::primitive::{ProcNumber, FSM_FORKNUM, MAIN_FORKNUM};

/// `reachedConsistency` test global.
static REACHED_CONSISTENCY: AtomicBool = AtomicBool::new(false);
/// Records whether `smgrdestroyall` was called.
static SMGRDESTROYALL_CALLED: AtomicBool = AtomicBool::new(false);

/// Install the small set of seams the in-crate logic consults, plus a faithful
/// `relpathbackend` (common/relpath.c `GetRelationPath`). Installed once per
/// process via `OnceLock::set`; subsequent calls are no-ops.
fn install_seams() {
    use core::sync::atomic::AtomicBool as Once;
    static DONE: Once = Once::new(false);
    if DONE.swap(true, Ordering::SeqCst) {
        return;
    }
    recovery_seam::reached_consistency::set(|| REACHED_CONSISTENCY.load(Ordering::SeqCst));
    smgr_seam::smgrdestroyall::set(|| {
        SMGRDESTROYALL_CALLED.store(true, Ordering::SeqCst);
        Ok(())
    });
    relpath_seam::relpathbackend::set(test_relpathbackend);
}

/// A faithful-enough `relpathbackend(rlocator, INVALID_PROC_NUMBER, forkno)` for
/// a permanent relation in the default tablespace (`GetRelationPath`): the
/// `base/<db>/<rel>` form, with `_init` / `_fsm` / `_vm` suffixes for the
/// non-MAIN forks.
fn test_relpathbackend(
    rlocator: RelFileLocator,
    _backend: ProcNumber,
    forkno: ForkNumber,
) -> String {
    let base = format!("base/{}/{}", rlocator.dbOid, rlocator.relNumber);
    match forkno as i32 {
        0 => base,                   // MAIN_FORKNUM
        1 => format!("{base}_fsm"),  // FSM_FORKNUM
        2 => format!("{base}_vm"),   // VISIBILITYMAP_FORKNUM
        3 => format!("{base}_init"), // INIT_FORKNUM
        n => format!("{base}_{n}"),
    }
}

fn loc(spc: Oid, db: Oid, rel: u32) -> RelFileLocator {
    RelFileLocator {
        spcOid: spc,
        dbOid: db,
        relNumber: rel,
    }
}

fn clear_table() {
    INVALID_PAGE_TAB.with(|t| *t.borrow_mut() = None);
}

fn tab_has(key: XlInvalidPageKey) -> bool {
    INVALID_PAGE_TAB.with(|t| t.borrow().as_ref().is_some_and(|m| m.contains_key(&key)))
}

fn tab_get(key: XlInvalidPageKey) -> Option<bool> {
    INVALID_PAGE_TAB.with(|t| t.borrow().as_ref().and_then(|m| m.get(&key).copied()))
}

#[test]
fn xlogfilename_matches_c_format() {
    // wal_segment_size = 16 MB => XLogSegmentsPerXLogId = 0x100000000 / 16MB = 256.
    let segsz: i32 = 16 * 1024 * 1024;
    assert_eq!(XLogFileName(1, 0, segsz), "000000010000000000000000");
    assert_eq!(XLogFileName(1, 256, segsz), "000000010000000100000000");
    assert_eq!(XLogFileName(1, 255, segsz), "0000000100000000000000FF");
}

#[test]
fn xlogfilepath_prefixes_pg_wal() {
    let segsz: i32 = 16 * 1024 * 1024;
    assert_eq!(
        XLogFilePath(2, 256, segsz),
        "pg_wal/000000020000000100000000"
    );
}

#[test]
fn lsn_format_args_splits_hi_lo() {
    assert_eq!(lsn_format_args(0), "0/0");
    assert_eq!(lsn_format_args(0x0000_0001_ABCD_1234), "1/ABCD1234");
}

#[test]
fn relpathperm_renders_base_path() {
    install_seams();
    let path = relpathperm(loc(1663, 5, 1259), MAIN_FORKNUM);
    assert_eq!(path, "base/5/1259");
    let path = relpathperm(loc(1663, 5, 1259), INIT_FORKNUM);
    assert_eq!(path, "base/5/1259_init");
}

#[test]
fn log_and_have_invalid_pages_roundtrip() {
    install_seams();
    clear_table();
    REACHED_CONSISTENCY.store(false, Ordering::SeqCst);

    assert!(!XLogHaveInvalidPages());

    log_invalid_page(loc(1663, 5, 100), MAIN_FORKNUM, 7, false).unwrap();
    assert!(XLogHaveInvalidPages());

    // A repeat reference must not flip "present" from false to true.
    log_invalid_page(loc(1663, 5, 100), MAIN_FORKNUM, 7, true).unwrap();
    let key = XlInvalidPageKey {
        locator: loc(1663, 5, 100),
        forkno: MAIN_FORKNUM,
        blkno: 7,
    };
    assert_eq!(tab_get(key), Some(false));
}

#[test]
fn forget_invalid_pages_drops_at_or_above_minblkno() {
    install_seams();
    clear_table();
    REACHED_CONSISTENCY.store(false, Ordering::SeqCst);

    let rel = loc(1663, 5, 100);
    log_invalid_page(rel, MAIN_FORKNUM, 3, false).unwrap();
    log_invalid_page(rel, MAIN_FORKNUM, 5, false).unwrap();
    log_invalid_page(rel, MAIN_FORKNUM, 9, false).unwrap();
    // A different fork must be unaffected.
    log_invalid_page(rel, FSM_FORKNUM, 9, false).unwrap();

    // XLogTruncateRelation(rel, fork 0, nblocks=5) forgets blocks >= 5.
    XLogTruncateRelation(rel, MAIN_FORKNUM, 5).unwrap();

    assert!(tab_has(XlInvalidPageKey { locator: rel, forkno: MAIN_FORKNUM, blkno: 3 }));
    assert!(!tab_has(XlInvalidPageKey { locator: rel, forkno: MAIN_FORKNUM, blkno: 5 }));
    assert!(!tab_has(XlInvalidPageKey { locator: rel, forkno: MAIN_FORKNUM, blkno: 9 }));
    // fork 1 / block 9 survives.
    assert!(tab_has(XlInvalidPageKey { locator: rel, forkno: FSM_FORKNUM, blkno: 9 }));
}

#[test]
fn xlog_drop_relation_forgets_from_block_zero() {
    install_seams();
    clear_table();
    REACHED_CONSISTENCY.store(false, Ordering::SeqCst);

    let rel = loc(1663, 5, 100);
    log_invalid_page(rel, MAIN_FORKNUM, 0, false).unwrap();
    log_invalid_page(rel, MAIN_FORKNUM, 42, false).unwrap();

    XLogDropRelation(rel, MAIN_FORKNUM).unwrap();

    assert!(!XLogHaveInvalidPages());
}

#[test]
fn xlog_drop_database_forgets_db_and_destroys_smgr() {
    install_seams();
    clear_table();
    REACHED_CONSISTENCY.store(false, Ordering::SeqCst);
    SMGRDESTROYALL_CALLED.store(false, Ordering::SeqCst);

    log_invalid_page(loc(1663, 5, 100), MAIN_FORKNUM, 1, false).unwrap();
    log_invalid_page(loc(1663, 6, 200), MAIN_FORKNUM, 1, false).unwrap();

    XLogDropDatabase(5).unwrap();

    assert!(SMGRDESTROYALL_CALLED.load(Ordering::SeqCst));
    // db 5 gone, db 6 stays.
    assert!(!tab_has(XlInvalidPageKey { locator: loc(1663, 5, 100), forkno: MAIN_FORKNUM, blkno: 1 }));
    assert!(tab_has(XlInvalidPageKey { locator: loc(1663, 6, 200), forkno: MAIN_FORKNUM, blkno: 1 }));
}

#[test]
fn check_invalid_pages_ignored_is_warning() {
    install_seams();
    clear_table();
    REACHED_CONSISTENCY.store(false, Ordering::SeqCst);

    // The PANIC path (ignore_invalid_pages = false) calls std::process::abort()
    // in this repo's error crate (faithful to C), so it can't be exercised in a
    // unit test. Cover the WARNING path: ignore_invalid_pages = true makes a
    // remaining entry a WARNING -> Ok, and the table is destroyed regardless.
    set_ignore_invalid_pages(true);
    log_invalid_page(loc(1663, 5, 100), MAIN_FORKNUM, 1, false).unwrap();
    XLogCheckInvalidPages().unwrap();
    assert!(!XLogHaveInvalidPages());

    set_ignore_invalid_pages(false);
}

#[test]
fn check_invalid_pages_empty_is_noop() {
    install_seams();
    clear_table();
    XLogCheckInvalidPages().unwrap();
}

#[test]
fn standby_state_roundtrip() {
    set_standby_state(wal::STANDBY_SNAPSHOT_READY);
    assert_eq!(standby_state(), wal::STANDBY_SNAPSHOT_READY);
    set_standby_state(STANDBY_DISABLED);
    assert_eq!(standby_state(), STANDBY_DISABLED);
}

#[test]
fn ignore_invalid_pages_roundtrip() {
    set_ignore_invalid_pages(true);
    assert!(ignore_invalid_pages());
    set_ignore_invalid_pages(false);
    assert!(!ignore_invalid_pages());
}

#[test]
fn in_recovery_roundtrip() {
    set_in_recovery(true);
    assert!(in_recovery());
    set_in_recovery(false);
    assert!(!in_recovery());
}
