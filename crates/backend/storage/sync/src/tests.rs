//! Unit tests for the `sync.c` port.
//!
//! The cross-subsystem dependencies are function-pointer seams (`OnceLock`
//! slots installed once). The test "runtime" lives in a thread-local [`TestRt`]
//! that the installed seam functions read/mutate; each test resets the runtime
//! and threads a fresh [`SyncState`] explicitly into the internal entry points
//! (the public seams read the crate's `thread_local!` state, which the tests
//! bypass to keep each test's state isolated).

use std::cell::RefCell;
use std::sync::Once;

use super::*;
use types_core::primitive::MAIN_FORKNUM;
use types_storage::RelFileLocator;

#[derive(Default)]
struct TestRt {
    enable_fsync: bool,
    data_sync_retry: bool,

    absorbs: i32,
    synced: Vec<FileTag>,
    sync_fail_then_ok: bool,
    sync_failed_once: bool,
    unlinks: Vec<FileTag>,
    ckpt_rels: i32,

    forward_full_n: i32,
    forwarded: Vec<(FileTag, SyncRequestType)>,
    waits: i32,
}

thread_local! {
    static RT: RefCell<TestRt> = RefCell::new(TestRt::default());
}

fn with_rt<R>(f: impl FnOnce(&mut TestRt) -> R) -> R {
    RT.with(|cell| f(&mut cell.borrow_mut()))
}

static INSTALL: Once = Once::new();

/// Install every owner seam once (idempotent across tests) with a function that
/// reads the per-thread [`TestRt`], then reset the runtime and return a fresh
/// [`SyncState`].
fn setup(configure: impl FnOnce(&mut TestRt)) -> SyncState {
    INSTALL.call_once(|| {
        checkpointer_seams::absorb_sync_requests::set(|| {
            with_rt(|rt| rt.absorbs += 1);
            Ok(())
        });
        checkpointer_seams::forward_sync_request::set(|ftag, t| {
            with_rt(|rt| {
                if rt.forward_full_n > 0 {
                    rt.forward_full_n -= 1;
                    return Ok(false);
                }
                rt.forwarded.push((ftag, t));
                Ok(true)
            })
        });
        checkpointer_seams::checkpoint_stats_set::set(|rels, _longest, _agg| {
            with_rt(|rt| rt.ckpt_rels = rels);
        });
        file_seams::data_sync_elevel::set(|elevel| {
            with_rt(|rt| {
                if rt.data_sync_retry {
                    elevel
                } else {
                    types_error::PANIC
                }
            })
        });
        latch_seams::wait_latch_register_sync_request::set(|| {
            with_rt(|rt| rt.waits += 1);
            Ok(())
        });
        md_seams::mdsyncfiletag::set(|ftag| {
            with_rt(|rt| {
                if rt.sync_fail_then_ok && !rt.sync_failed_once {
                    rt.sync_failed_once = true;
                    return Ok(FileTagOpResult {
                        result: -1,
                        path: "f".into(),
                        errno: ENOENT,
                    });
                }
                rt.synced.push(ftag);
                Ok(FileTagOpResult {
                    result: 0,
                    path: "f".into(),
                    errno: 0,
                })
            })
        });
        md_seams::mdunlinkfiletag::set(|ftag| {
            with_rt(|rt| rt.unlinks.push(ftag));
            Ok(FileTagOpResult {
                result: 0,
                path: "f".into(),
                errno: 0,
            })
        });
        md_seams::mdfiletagmatches::set(|ftag, candidate| {
            // mdfiletagmatches: same dbOid.
            Ok(ftag.rlocator.dbOid == candidate.rlocator.dbOid)
        });
    });

    with_rt(|rt| {
        *rt = TestRt {
            enable_fsync: true,
            ..TestRt::default()
        };
        configure(rt);
    });

    SyncState::new()
}

fn rt_enable_fsync() -> bool {
    with_rt(|rt| rt.enable_fsync)
}

fn locator(spc: u32, db: u32, rel: u32) -> RelFileLocator {
    RelFileLocator {
        spcOid: spc,
        dbOid: db,
        relNumber: rel,
    }
}

fn tag(seg: u64) -> FileTag {
    FileTag::new(SyncRequestHandler::SYNC_HANDLER_MD, MAIN_FORKNUM, locator(1, 2, 3), seg)
}

fn pending_entry(s: &SyncState, t: FileTag) -> PendingFsyncEntry {
    *s.pending_ops.as_ref().unwrap().get(&t).expect("entry present")
}

#[test]
fn init_creates_pending_ops_when_requested() {
    let mut s = setup(|_| {});
    init_sync(&mut s, true);
    assert!(s.pending_ops.is_some());
}

#[test]
fn init_skips_pending_ops_when_not_requested() {
    let mut s = setup(|_| {});
    init_sync(&mut s, false);
    assert!(s.pending_ops.is_none());
}

#[test]
fn remember_and_process_sync_request() {
    let mut s = setup(|_| {});
    init_sync(&mut s, true);
    remember_sync_request(&mut s, &tag(7), SyncRequestType::SYNC_REQUEST).unwrap();
    process_sync_requests(&mut s, rt_enable_fsync(), false).unwrap();
    assert_eq!(with_rt(|rt| rt.synced.clone()), vec![tag(7)]);
    assert_eq!(with_rt(|rt| rt.ckpt_rels), 1);
    assert_eq!(s.pending_ops.as_ref().unwrap().len(), 0);
}

#[test]
fn forget_request_cancels_entry() {
    let mut s = setup(|_| {});
    init_sync(&mut s, true);
    remember_sync_request(&mut s, &tag(7), SyncRequestType::SYNC_REQUEST).unwrap();
    remember_sync_request(&mut s, &tag(7), SyncRequestType::SYNC_FORGET_REQUEST).unwrap();
    process_sync_requests(&mut s, rt_enable_fsync(), false).unwrap();
    assert!(with_rt(|rt| rt.synced.is_empty()));
    assert_eq!(s.pending_ops.as_ref().unwrap().len(), 0);
}

#[test]
fn filter_request_cancels_matching_db() {
    let mut s = setup(|_| {});
    init_sync(&mut s, true);
    remember_sync_request(&mut s, &tag(7), SyncRequestType::SYNC_REQUEST).unwrap();
    let filter = FileTag::new(SyncRequestHandler::SYNC_HANDLER_MD, MAIN_FORKNUM, locator(0, 2, 0), 0);
    remember_sync_request(&mut s, &filter, SyncRequestType::SYNC_FILTER_REQUEST).unwrap();
    process_sync_requests(&mut s, rt_enable_fsync(), false).unwrap();
    assert!(with_rt(|rt| rt.synced.is_empty()));
}

#[test]
fn filter_request_keeps_other_db() {
    let mut s = setup(|_| {});
    init_sync(&mut s, true);
    remember_sync_request(&mut s, &tag(7), SyncRequestType::SYNC_REQUEST).unwrap();
    let filter = FileTag::new(SyncRequestHandler::SYNC_HANDLER_MD, MAIN_FORKNUM, locator(0, 999, 0), 0);
    remember_sync_request(&mut s, &filter, SyncRequestType::SYNC_FILTER_REQUEST).unwrap();
    process_sync_requests(&mut s, rt_enable_fsync(), false).unwrap();
    assert_eq!(with_rt(|rt| rt.synced.clone()), vec![tag(7)]);
}

#[test]
fn filter_request_cancels_matching_unlink() {
    let mut s = setup(|_| {});
    init_sync(&mut s, true);
    remember_sync_request(&mut s, &tag(7), SyncRequestType::SYNC_UNLINK_REQUEST).unwrap();
    let filter = FileTag::new(SyncRequestHandler::SYNC_HANDLER_MD, MAIN_FORKNUM, locator(0, 2, 0), 0);
    remember_sync_request(&mut s, &filter, SyncRequestType::SYNC_FILTER_REQUEST).unwrap();
    sync_pre_checkpoint(&mut s).unwrap();
    sync_post_checkpoint(&mut s).unwrap();
    assert!(with_rt(|rt| rt.unlinks.is_empty()));
}

#[test]
fn fsync_enoent_retry_then_success() {
    let mut s = setup(|rt| rt.sync_fail_then_ok = true);
    init_sync(&mut s, true);
    remember_sync_request(&mut s, &tag(7), SyncRequestType::SYNC_REQUEST).unwrap();
    process_sync_requests(&mut s, rt_enable_fsync(), false).unwrap();
    assert_eq!(with_rt(|rt| rt.synced.clone()), vec![tag(7)]);
    assert!(with_rt(|rt| rt.sync_failed_once));
}

#[test]
fn fsync_disabled_skips_handler() {
    let mut s = setup(|rt| rt.enable_fsync = false);
    init_sync(&mut s, true);
    remember_sync_request(&mut s, &tag(7), SyncRequestType::SYNC_REQUEST).unwrap();
    process_sync_requests(&mut s, rt_enable_fsync(), false).unwrap();
    assert!(with_rt(|rt| rt.synced.is_empty()));
    assert_eq!(with_rt(|rt| rt.ckpt_rels), 0);
    assert_eq!(s.pending_ops.as_ref().unwrap().len(), 0);
}

#[test]
fn unlink_request_processed_after_cycle_advance() {
    let mut s = setup(|_| {});
    init_sync(&mut s, true);
    remember_sync_request(&mut s, &tag(7), SyncRequestType::SYNC_UNLINK_REQUEST).unwrap();
    sync_pre_checkpoint(&mut s).unwrap();
    sync_post_checkpoint(&mut s).unwrap();
    assert_eq!(with_rt(|rt| rt.unlinks.clone()), vec![tag(7)]);
    assert!(s.pending_unlinks.is_empty());
}

#[test]
fn unlink_request_deferred_when_same_cycle() {
    let mut s = setup(|_| {});
    init_sync(&mut s, true);
    sync_pre_checkpoint(&mut s).unwrap();
    remember_sync_request(&mut s, &tag(7), SyncRequestType::SYNC_UNLINK_REQUEST).unwrap();
    sync_post_checkpoint(&mut s).unwrap();
    assert!(with_rt(|rt| rt.unlinks.is_empty()));
    assert_eq!(s.pending_unlinks.len(), 1);
}

#[test]
fn register_forwards_when_no_local_ops() {
    let mut s = setup(|rt| rt.forward_full_n = 2);
    // No init_sync => pending_ops is None => forward path.
    let ok =
        register_sync_request(&mut s, &tag(1), SyncRequestType::SYNC_REQUEST, true).unwrap();
    assert!(ok);
    assert_eq!(with_rt(|rt| rt.waits), 2);
    assert_eq!(
        with_rt(|rt| rt.forwarded.clone()),
        vec![(tag(1), SyncRequestType::SYNC_REQUEST)]
    );
}

#[test]
fn register_no_retry_returns_false_when_full() {
    let mut s = setup(|rt| rt.forward_full_n = 1);
    let ok =
        register_sync_request(&mut s, &tag(1), SyncRequestType::SYNC_REQUEST, false).unwrap();
    assert!(!ok);
    assert_eq!(with_rt(|rt| rt.waits), 0);
}

#[test]
fn register_local_remembers_when_pending_ops_present() {
    let mut s = setup(|_| {});
    init_sync(&mut s, true);
    let ok =
        register_sync_request(&mut s, &tag(5), SyncRequestType::SYNC_REQUEST, false).unwrap();
    assert!(ok);
    assert!(with_rt(|rt| rt.forwarded.is_empty()));
    assert_eq!(s.pending_ops.as_ref().unwrap().len(), 1);
}

#[test]
fn process_without_pending_ops_errors() {
    let mut s = setup(|_| {});
    assert!(process_sync_requests(&mut s, rt_enable_fsync(), false).is_err());
}

#[test]
fn duplicate_sync_request_keeps_oldest_cycle_ctr() {
    let mut s = setup(|_| {});
    init_sync(&mut s, true);
    remember_sync_request(&mut s, &tag(7), SyncRequestType::SYNC_REQUEST).unwrap();
    let first = pending_entry(&s, tag(7)).cycle_ctr;
    s.sync_cycle_ctr = s.sync_cycle_ctr.wrapping_add(5);
    remember_sync_request(&mut s, &tag(7), SyncRequestType::SYNC_REQUEST).unwrap();
    let again = pending_entry(&s, tag(7)).cycle_ctr;
    assert_eq!(first, again);
}

#[test]
fn canceled_then_rerequested_reinitializes() {
    let mut s = setup(|_| {});
    init_sync(&mut s, true);
    remember_sync_request(&mut s, &tag(7), SyncRequestType::SYNC_REQUEST).unwrap();
    remember_sync_request(&mut s, &tag(7), SyncRequestType::SYNC_FORGET_REQUEST).unwrap();
    assert!(pending_entry(&s, tag(7)).canceled);
    remember_sync_request(&mut s, &tag(7), SyncRequestType::SYNC_REQUEST).unwrap();
    assert!(!pending_entry(&s, tag(7)).canceled);
    process_sync_requests(&mut s, rt_enable_fsync(), false).unwrap();
    assert_eq!(with_rt(|rt| rt.synced.clone()), vec![tag(7)]);
}

#[test]
fn handler_enum_discriminants_match_syncsw_indexes() {
    // `FileTag.handler` is a typed `SyncRequestHandler`, but its discriminants
    // are the raw `int16` indexes into the C `syncsw[]` vtable; verify they
    // match the C enum order exactly.
    assert_eq!(SyncRequestHandler::SYNC_HANDLER_MD as i16, 0);
    assert_eq!(SyncRequestHandler::SYNC_HANDLER_CLOG as i16, 1);
    assert_eq!(SyncRequestHandler::SYNC_HANDLER_COMMIT_TS as i16, 2);
    assert_eq!(SyncRequestHandler::SYNC_HANDLER_MULTIXACT_OFFSET as i16, 3);
    assert_eq!(SyncRequestHandler::SYNC_HANDLER_MULTIXACT_MEMBER as i16, 4);
    assert_eq!(SyncRequestHandler::SYNC_HANDLER_NONE as i16, 5);
}
