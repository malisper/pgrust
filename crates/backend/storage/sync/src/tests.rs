//! Unit tests for the `sync.c` port.
//!
//! The cross-subsystem dependencies are function-pointer seams (`OnceLock`
//! slots installed once). The test "runtime" lives in a thread-local [`TestRt`]
//! that the installed seam functions read/mutate. The checkpoint entry points
//! (`process_sync_requests` / `sync_pre_checkpoint` / `sync_post_checkpoint`)
//! deliberately do NOT hold the [`SYNC_STATE`] borrow across their re-entrant
//! seam callbacks (`absorb_sync_requests` re-enters `remember_sync_request`),
//! so they read the crate's `thread_local!` state in narrow `with_state`
//! scopes. The tests therefore seed that same `thread_local!` (via [`reset_state`]
//! / [`set_state`]) rather than threading a private `&mut SyncState`; each test
//! resets both the runtime and the shared state first to stay isolated.

use std::cell::RefCell;
use std::sync::Once;

use super::*;
use ::types_core::primitive::MAIN_FORKNUM;
use ::types_storage::RelFileLocator;

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
/// reads the per-thread [`TestRt`], then reset the runtime and the shared
/// [`SYNC_STATE`] to a fresh, empty state.
fn setup(configure: impl FnOnce(&mut TestRt)) {
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

    reset_state();
}

/// Replace the crate `thread_local!` [`SYNC_STATE`] with a fresh empty state so
/// each test starts clean (the checkpoint entry points operate on this shared
/// state).
fn reset_state() {
    SYNC_STATE.with(|cell| *cell.borrow_mut() = SyncState::new());
}

/// Run `f` against a momentary borrow of the shared [`SYNC_STATE`] (test-side
/// inspection / direct setup that mirrors what the production seams do).
fn with_shared<R>(f: impl FnOnce(&mut SyncState) -> R) -> R {
    SYNC_STATE.with(|cell| f(&mut cell.borrow_mut()))
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

fn pending_entry(t: FileTag) -> PendingFsyncEntry {
    with_shared(|s| *s.pending_ops.as_ref().unwrap().get(&t).expect("entry present"))
}

#[test]
fn init_creates_pending_ops_when_requested() {
    setup(|_| {});
    with_shared(|s| init_sync(s, true));
    assert!(with_shared(|s| s.pending_ops.is_some()));
}

#[test]
fn init_skips_pending_ops_when_not_requested() {
    setup(|_| {});
    with_shared(|s| init_sync(s, false));
    assert!(with_shared(|s| s.pending_ops.is_none()));
}

#[test]
fn remember_and_process_sync_request() {
    setup(|_| {});
    with_shared(|s| init_sync(s, true));
    with_shared(|s| remember_sync_request(s, &tag(7), SyncRequestType::SYNC_REQUEST)).unwrap();
    process_sync_requests(rt_enable_fsync(), false).unwrap();
    assert_eq!(with_rt(|rt| rt.synced.clone()), vec![tag(7)]);
    assert_eq!(with_rt(|rt| rt.ckpt_rels), 1);
    assert_eq!(with_shared(|s| s.pending_ops.as_ref().unwrap().len()), 0);
}

#[test]
fn forget_request_cancels_entry() {
    setup(|_| {});
    with_shared(|s| init_sync(s, true));
    with_shared(|s| remember_sync_request(s, &tag(7), SyncRequestType::SYNC_REQUEST)).unwrap();
    with_shared(|s| remember_sync_request(s, &tag(7), SyncRequestType::SYNC_FORGET_REQUEST))
        .unwrap();
    process_sync_requests(rt_enable_fsync(), false).unwrap();
    assert!(with_rt(|rt| rt.synced.is_empty()));
    assert_eq!(with_shared(|s| s.pending_ops.as_ref().unwrap().len()), 0);
}

#[test]
fn filter_request_cancels_matching_db() {
    setup(|_| {});
    with_shared(|s| init_sync(s, true));
    with_shared(|s| remember_sync_request(s, &tag(7), SyncRequestType::SYNC_REQUEST)).unwrap();
    let filter = FileTag::new(SyncRequestHandler::SYNC_HANDLER_MD, MAIN_FORKNUM, locator(0, 2, 0), 0);
    with_shared(|s| remember_sync_request(s, &filter, SyncRequestType::SYNC_FILTER_REQUEST))
        .unwrap();
    process_sync_requests(rt_enable_fsync(), false).unwrap();
    assert!(with_rt(|rt| rt.synced.is_empty()));
}

#[test]
fn filter_request_keeps_other_db() {
    setup(|_| {});
    with_shared(|s| init_sync(s, true));
    with_shared(|s| remember_sync_request(s, &tag(7), SyncRequestType::SYNC_REQUEST)).unwrap();
    let filter =
        FileTag::new(SyncRequestHandler::SYNC_HANDLER_MD, MAIN_FORKNUM, locator(0, 999, 0), 0);
    with_shared(|s| remember_sync_request(s, &filter, SyncRequestType::SYNC_FILTER_REQUEST))
        .unwrap();
    process_sync_requests(rt_enable_fsync(), false).unwrap();
    assert_eq!(with_rt(|rt| rt.synced.clone()), vec![tag(7)]);
}

#[test]
fn filter_request_cancels_matching_unlink() {
    setup(|_| {});
    with_shared(|s| init_sync(s, true));
    with_shared(|s| remember_sync_request(s, &tag(7), SyncRequestType::SYNC_UNLINK_REQUEST))
        .unwrap();
    let filter = FileTag::new(SyncRequestHandler::SYNC_HANDLER_MD, MAIN_FORKNUM, locator(0, 2, 0), 0);
    with_shared(|s| remember_sync_request(s, &filter, SyncRequestType::SYNC_FILTER_REQUEST))
        .unwrap();
    sync_pre_checkpoint().unwrap();
    sync_post_checkpoint().unwrap();
    assert!(with_rt(|rt| rt.unlinks.is_empty()));
}

#[test]
fn fsync_enoent_retry_then_success() {
    setup(|rt| rt.sync_fail_then_ok = true);
    with_shared(|s| init_sync(s, true));
    with_shared(|s| remember_sync_request(s, &tag(7), SyncRequestType::SYNC_REQUEST)).unwrap();
    process_sync_requests(rt_enable_fsync(), false).unwrap();
    assert_eq!(with_rt(|rt| rt.synced.clone()), vec![tag(7)]);
    assert!(with_rt(|rt| rt.sync_failed_once));
}

#[test]
fn fsync_disabled_skips_handler() {
    setup(|rt| rt.enable_fsync = false);
    with_shared(|s| init_sync(s, true));
    with_shared(|s| remember_sync_request(s, &tag(7), SyncRequestType::SYNC_REQUEST)).unwrap();
    process_sync_requests(rt_enable_fsync(), false).unwrap();
    assert!(with_rt(|rt| rt.synced.is_empty()));
    assert_eq!(with_rt(|rt| rt.ckpt_rels), 0);
    assert_eq!(with_shared(|s| s.pending_ops.as_ref().unwrap().len()), 0);
}

#[test]
fn unlink_request_processed_after_cycle_advance() {
    setup(|_| {});
    with_shared(|s| init_sync(s, true));
    with_shared(|s| remember_sync_request(s, &tag(7), SyncRequestType::SYNC_UNLINK_REQUEST))
        .unwrap();
    sync_pre_checkpoint().unwrap();
    sync_post_checkpoint().unwrap();
    assert_eq!(with_rt(|rt| rt.unlinks.clone()), vec![tag(7)]);
    assert!(with_shared(|s| s.pending_unlinks.is_empty()));
}

#[test]
fn unlink_request_deferred_when_same_cycle() {
    setup(|_| {});
    with_shared(|s| init_sync(s, true));
    sync_pre_checkpoint().unwrap();
    with_shared(|s| remember_sync_request(s, &tag(7), SyncRequestType::SYNC_UNLINK_REQUEST))
        .unwrap();
    sync_post_checkpoint().unwrap();
    assert!(with_rt(|rt| rt.unlinks.is_empty()));
    assert_eq!(with_shared(|s| s.pending_unlinks.len()), 1);
}

#[test]
fn register_forwards_when_no_local_ops() {
    setup(|rt| rt.forward_full_n = 2);
    // No init_sync => pending_ops is None => forward path.
    let ok = with_shared(|s| {
        register_sync_request(s, &tag(1), SyncRequestType::SYNC_REQUEST, true)
    })
    .unwrap();
    assert!(ok);
    assert_eq!(with_rt(|rt| rt.waits), 2);
    assert_eq!(
        with_rt(|rt| rt.forwarded.clone()),
        vec![(tag(1), SyncRequestType::SYNC_REQUEST)]
    );
}

#[test]
fn register_no_retry_returns_false_when_full() {
    setup(|rt| rt.forward_full_n = 1);
    let ok = with_shared(|s| {
        register_sync_request(s, &tag(1), SyncRequestType::SYNC_REQUEST, false)
    })
    .unwrap();
    assert!(!ok);
    assert_eq!(with_rt(|rt| rt.waits), 0);
}

#[test]
fn register_local_remembers_when_pending_ops_present() {
    setup(|_| {});
    with_shared(|s| init_sync(s, true));
    let ok = with_shared(|s| {
        register_sync_request(s, &tag(5), SyncRequestType::SYNC_REQUEST, false)
    })
    .unwrap();
    assert!(ok);
    assert!(with_rt(|rt| rt.forwarded.is_empty()));
    assert_eq!(with_shared(|s| s.pending_ops.as_ref().unwrap().len()), 1);
}

#[test]
fn process_without_pending_ops_errors() {
    setup(|_| {});
    assert!(process_sync_requests(rt_enable_fsync(), false).is_err());
}

#[test]
fn duplicate_sync_request_keeps_oldest_cycle_ctr() {
    setup(|_| {});
    with_shared(|s| init_sync(s, true));
    with_shared(|s| remember_sync_request(s, &tag(7), SyncRequestType::SYNC_REQUEST)).unwrap();
    let first = pending_entry(tag(7)).cycle_ctr;
    with_shared(|s| s.sync_cycle_ctr = s.sync_cycle_ctr.wrapping_add(5));
    with_shared(|s| remember_sync_request(s, &tag(7), SyncRequestType::SYNC_REQUEST)).unwrap();
    let again = pending_entry(tag(7)).cycle_ctr;
    assert_eq!(first, again);
}

#[test]
fn canceled_then_rerequested_reinitializes() {
    setup(|_| {});
    with_shared(|s| init_sync(s, true));
    with_shared(|s| remember_sync_request(s, &tag(7), SyncRequestType::SYNC_REQUEST)).unwrap();
    with_shared(|s| remember_sync_request(s, &tag(7), SyncRequestType::SYNC_FORGET_REQUEST))
        .unwrap();
    assert!(pending_entry(tag(7)).canceled);
    with_shared(|s| remember_sync_request(s, &tag(7), SyncRequestType::SYNC_REQUEST)).unwrap();
    assert!(!pending_entry(tag(7)).canceled);
    process_sync_requests(rt_enable_fsync(), false).unwrap();
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
