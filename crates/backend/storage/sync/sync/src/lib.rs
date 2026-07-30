#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! src/backend/storage/sync/sync.c — pending-fsync/unlink tracking for the
//! checkpointer and standalone backends.

use core::cell::{Cell, RefCell};

use ::elog::ereport;
use ::mcx::{MemoryContext, PgHashMap, PgVec};
use ::types_core::BackendType;
use ::types_error::{ErrorLocation, PgError, PgResult, ERRCODE_OUT_OF_MEMORY, ERROR, WARNING};
use ::types_storage::sync::{FileTag, FileTagOpResult, SyncRequestHandler, SyncRequestType};

type CycleCtr = u16;

const FSYNCS_PER_ABSORB: i32 = 10;
const UNLINKS_PER_ABSORB: i32 = 10;

// wait_event_names.txt Timeout section, index 5.
const WAIT_EVENT_REGISTER_SYNC_REQUEST: u32 = 0x0900_0005;

#[derive(Clone, Copy)]
struct PendingFsync {
    cycle_ctr: CycleCtr,
    canceled: bool,
}

#[derive(Clone, Copy)]
struct PendingUnlink {
    tag: FileTag,
    cycle_ctr: CycleCtr,
    canceled: bool,
}

struct PendingOps {
    cx: &'static MemoryContext,
    ops: PgHashMap<'static, FileTag, PendingFsync>,
    unlinks: PgVec<'static, PendingUnlink>,
}

thread_local! {
    static PENDING: RefCell<Option<PendingOps>> = const { RefCell::new(None) };
    static SYNC_CYCLE_CTR: Cell<CycleCtr> = const { Cell::new(0) };
    static CHECKPOINT_CYCLE_CTR: Cell<CycleCtr> = const { Cell::new(0) };
    static SYNC_IN_PROGRESS: Cell<bool> = const { Cell::new(false) };
}

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

#[track_caller]
#[cold]
#[inline(never)]
fn oom(what: &str) -> Box<PgError> {
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_OUT_OF_MEMORY)
            .errmsg_internal(format!("out of memory allocating {what}"))
            .into_error(),
    )
}

// Borrows are short-scoped: absorb and the filetag handlers re-enter
// RememberSyncRequest, so no borrow may be live across them.
fn with_pending<R>(f: impl FnOnce(&mut PendingOps) -> R) -> Option<R> {
    PENDING.with(|p| p.borrow_mut().as_mut().map(f))
}

fn syncfiletag(tag: &FileTag) -> PgResult<FileTagOpResult> {
    match tag.handler {
        SyncRequestHandler::SYNC_HANDLER_MD => smgr::mdsyncfiletag(*tag),
        SyncRequestHandler::SYNC_HANDLER_CLOG => {
            let (result, path) = clog::clogsyncfiletag(tag)?;
            Ok(FileTagOpResult {
                result,
                path: path.as_str().to_string(),
                errno: md::last_errno(),
            })
        }
        SyncRequestHandler::SYNC_HANDLER_COMMIT_TS => {
            let (result, path) = commit_ts::committssyncfiletag(tag)?;
            Ok(FileTagOpResult {
                result,
                path: path.as_str().to_string(),
                errno: md::last_errno(),
            })
        }
        SyncRequestHandler::SYNC_HANDLER_MULTIXACT_OFFSET => {
            let (result, path) = multixact::multixactoffsetssyncfiletag(tag)?;
            Ok(FileTagOpResult {
                result,
                path: path.as_str().to_string(),
                errno: md::last_errno(),
            })
        }
        SyncRequestHandler::SYNC_HANDLER_MULTIXACT_MEMBER => {
            let (result, path) = multixact::multixactmemberssyncfiletag(tag)?;
            Ok(FileTagOpResult {
                result,
                path: path.as_str().to_string(),
                errno: md::last_errno(),
            })
        }
        h => panic!("unported callee reached from sync.c syncsw[]: {h:?} sync_syncfiletag"),
    }
}

fn unlinkfiletag(tag: &FileTag) -> PgResult<FileTagOpResult> {
    match tag.handler {
        SyncRequestHandler::SYNC_HANDLER_MD => smgr::mdunlinkfiletag(*tag),
        // C's syncsw rows for the SLRU handlers have no unlinkfiletag.
        h => panic!("unported callee reached from sync.c syncsw[]: {h:?} sync_unlinkfiletag"),
    }
}

fn filetagmatches(tag: &FileTag, candidate: &FileTag) -> bool {
    match tag.handler {
        SyncRequestHandler::SYNC_HANDLER_MD => smgr::mdfiletagmatches(*tag, *candidate),
        h => panic!("unported callee reached from sync.c syncsw[]: {h:?} sync_filetagmatches"),
    }
}

fn absorb() -> PgResult<()> {
    // AbsorbSyncRequests (checkpointer.c) no-ops outside the checkpointer;
    // the checkpointer's queue drain installs this seam (aux-mains lane).
    if sync_seams::absorb_sync_requests::is_installed() {
        sync_seams::absorb_sync_requests::call()?;
    }
    Ok(())
}

pub fn InitSync() -> PgResult<()> {
    if !init_small::globals::IsUnderPostmaster()
        || miscinit::GetMyBackendType() == BackendType::Checkpointer
    {
        PENDING.with(|p| {
            let mut slot = p.borrow_mut();
            if slot.is_none() {
                let cx: &'static MemoryContext =
                    ::mcx::session_root("Pending ops context");
                // LIFO: empty the droppy TLS slot before its context is freed.
                ::mcx::register_session_cleanup(Box::new(|| {
                    PENDING.with(|p| drop(p.borrow_mut().take()));
                }));
                let mut ops = PgHashMap::new_in(cx.mcx());
                let _ = ops.try_reserve(100);
                *slot = Some(PendingOps {
                    cx,
                    ops,
                    unlinks: PgVec::new_in(cx.mcx()),
                });
            }
        });
    }
    Ok(())
}

pub fn SyncPreCheckpoint() -> PgResult<()> {
    absorb()?;
    CHECKPOINT_CYCLE_CTR.with(|c| c.set(c.get().wrapping_add(1)));
    Ok(())
}

pub fn SyncPostCheckpoint() -> PgResult<()> {
    let ckpt_ctr = CHECKPOINT_CYCLE_CTR.with(|c| c.get());
    let mut absorb_counter = UNLINKS_PER_ABSORB;
    let mut idx = 0usize;
    loop {
        let Some(entry) = with_pending(|p| p.unlinks.get(idx).copied()).flatten() else {
            break;
        };
        if entry.canceled {
            idx += 1;
            continue;
        }
        if entry.cycle_ctr == ckpt_ctr {
            break;
        }

        let r = unlinkfiletag(&entry.tag)?;
        if r.result < 0 && r.errno != libc::ENOENT {
            ereport(WARNING)
                .with_saved_errno(r.errno)
                .errcode_for_file_access()
                .errmsg(format!("could not remove file \"{}\": %m", r.path))
                .finish(loc("SyncPostCheckpoint"))?;
        }

        with_pending(|p| {
            if let Some(e) = p.unlinks.get_mut(idx) {
                e.canceled = true;
            }
        });
        idx += 1;

        absorb_counter -= 1;
        if absorb_counter <= 0 {
            absorb()?;
            absorb_counter = UNLINKS_PER_ABSORB;
        }
    }

    with_pending(|p| {
        let n = idx.min(p.unlinks.len());
        let rest = p.unlinks.len() - n;
        for i in 0..rest {
            p.unlinks[i] = p.unlinks[i + n];
        }
        p.unlinks.truncate(rest);
    });
    Ok(())
}

pub fn ProcessSyncRequests() -> PgResult<()> {
    if PENDING.with(|p| p.borrow().is_none()) {
        return Err(Box::new(
            PgError::new(ERROR, "cannot sync without a pendingOps table")
                .with_error_location(loc("ProcessSyncRequests")),
        ));
    }

    absorb()?;

    if SYNC_IN_PROGRESS.with(|c| c.get()) {
        let ctr = SYNC_CYCLE_CTR.with(|c| c.get());
        with_pending(|p| {
            for e in p.ops.values_mut() {
                e.cycle_ctr = ctr;
            }
        });
    }

    let new_ctr = SYNC_CYCLE_CTR.with(|c| {
        c.set(c.get().wrapping_add(1));
        c.get()
    });
    SYNC_IN_PROGRESS.with(|c| c.set(true));

    // Key snapshot replaces C's hash_seq: absorb may insert (new-cycle,
    // skipped) or cancel mid-loop, but only this loop removes.
    let tags = with_pending(|p| {
        let mut v: PgVec<'static, FileTag> = PgVec::new_in(p.cx.mcx());
        if v.try_reserve(p.ops.len()).is_err() {
            return Err(oom("pendingOps snapshot"));
        }
        v.extend(p.ops.keys().copied());
        Ok(v)
    })
    .expect("pendingOps checked above")?;

    let mut absorb_counter = FSYNCS_PER_ABSORB;
    for tag in tags {
        let is_new = with_pending(|p| p.ops.get(&tag).map(|e| e.cycle_ctr == new_ctr))
            .flatten()
            .unwrap_or(true);
        if is_new {
            continue;
        }

        if init_small::globals::enableFsync() {
            absorb_counter -= 1;
            if absorb_counter <= 0 {
                absorb()?;
                absorb_counter = FSYNCS_PER_ABSORB;
            }

            let mut failures = 0;
            loop {
                let canceled = with_pending(|p| p.ops.get(&tag).map(|e| e.canceled))
                    .flatten()
                    .unwrap_or(true);
                if canceled {
                    break;
                }
                let r = syncfiletag(&tag)?;
                if r.result == 0 {
                    break;
                }

                if r.errno != libc::ENOENT || failures > 0 {
                    // data_sync_retry=off promotes to PANIC; finish() aborts
                    // the process there — a caught Err would let the next
                    // cycle's fsync succeed after the kernel dropped the
                    // dirty pages (fsyncgate).
                    ereport(fd::data_sync_elevel(ERROR))
                        .with_saved_errno(r.errno)
                        .errcode_for_file_access()
                        .errmsg(format!("could not fsync file \"{}\": %m", r.path))
                        .finish(loc("ProcessSyncRequests"))?;
                    unreachable!("fsync-failure ereport returned");
                }
                absorb()?;
                absorb_counter = FSYNCS_PER_ABSORB;
                failures += 1;
            }
        }

        if with_pending(|p| p.ops.remove(&tag)).flatten().is_none() {
            return Err(Box::new(
                PgError::new(ERROR, "pendingOps corrupted")
                    .with_error_location(loc("ProcessSyncRequests")),
            ));
        }
    }

    // CheckpointStats.ckpt_sync_rels/longest/agg pend the stats unit.
    SYNC_IN_PROGRESS.with(|c| c.set(false));
    Ok(())
}

pub fn RememberSyncRequest(ftag: &FileTag, req_type: SyncRequestType) -> PgResult<()> {
    match req_type {
        SyncRequestType::SYNC_FORGET_REQUEST => {
            with_pending(|p| {
                if let Some(e) = p.ops.get_mut(ftag) {
                    e.canceled = true;
                }
            });
            Ok(())
        }
        SyncRequestType::SYNC_FILTER_REQUEST => {
            let Some((fsyncs, unlinks)) = with_pending(|p| {
                let mut fsyncs: PgVec<'static, FileTag> = PgVec::new_in(p.cx.mcx());
                let mut unlinks: PgVec<'static, FileTag> = PgVec::new_in(p.cx.mcx());
                if fsyncs.try_reserve(p.ops.len()).is_err()
                    || unlinks.try_reserve(p.unlinks.len()).is_err()
                {
                    return Err(oom("sync filter scratch"));
                }
                fsyncs.extend(p.ops.keys().filter(|t| t.handler == ftag.handler).copied());
                unlinks.extend(
                    p.unlinks
                        .iter()
                        .filter(|e| e.tag.handler == ftag.handler)
                        .map(|e| e.tag),
                );
                Ok((fsyncs, unlinks))
            })
            .transpose()?
            else {
                return Ok(());
            };
            for t in fsyncs {
                if filetagmatches(ftag, &t) {
                    with_pending(|p| {
                        if let Some(e) = p.ops.get_mut(&t) {
                            e.canceled = true;
                        }
                    });
                }
            }
            for t in unlinks {
                if filetagmatches(ftag, &t) {
                    with_pending(|p| {
                        for e in p.unlinks.iter_mut() {
                            if e.tag == t {
                                e.canceled = true;
                            }
                        }
                    });
                }
            }
            Ok(())
        }
        SyncRequestType::SYNC_UNLINK_REQUEST => {
            let ctr = CHECKPOINT_CYCLE_CTR.with(|c| c.get());
            with_pending(|p| {
                if p.unlinks.try_reserve(1).is_err() {
                    return Err(oom("pendingUnlinks"));
                }
                p.unlinks.push(PendingUnlink {
                    tag: *ftag,
                    cycle_ctr: ctr,
                    canceled: false,
                });
                Ok(())
            })
            .unwrap_or(Ok(()))
        }
        SyncRequestType::SYNC_REQUEST => {
            let ctr = SYNC_CYCLE_CTR.with(|c| c.get());
            with_pending(|p| {
                if p.ops.len() == p.ops.capacity() && p.ops.try_reserve(1).is_err() {
                    return Err(oom("pendingOps"));
                }
                let e = p.ops.entry(*ftag).or_insert(PendingFsync {
                    cycle_ctr: ctr,
                    canceled: true,
                });
                // cycle_ctr keeps the OLDEST live request; only a new or
                // previously-canceled entry takes the current counter.
                if e.canceled {
                    e.cycle_ctr = ctr;
                    e.canceled = false;
                }
                Ok(())
            })
            .unwrap_or(Ok(()))
        }
    }
}

pub fn RegisterSyncRequest(
    ftag: FileTag,
    req_type: SyncRequestType,
    retry_on_error: bool,
) -> PgResult<bool> {
    if PENDING.with(|p| p.borrow().is_some()) {
        RememberSyncRequest(&ftag, req_type)?;
        return Ok(true);
    }

    loop {
        let ret = checkpointer_seams::forward_sync_request::call(ftag, req_type)?;
        if ret || !retry_on_error {
            return Ok(ret);
        }
        latch::WaitLatch(
            None,
            types_storage::waiteventset::WL_EXIT_ON_PM_DEATH
                | types_storage::waiteventset::WL_TIMEOUT,
            10,
            WAIT_EVENT_REGISTER_SYNC_REQUEST,
        )?;
    }
}

pub fn init_seams() {
    sync_seams::register_sync_request::set(RegisterSyncRequest);
    sync_seams::init_sync::set(InitSync);
    sync_seams::sync_pre_checkpoint::set(SyncPreCheckpoint);
    sync_seams::sync_post_checkpoint::set(SyncPostCheckpoint);
    sync_seams::process_sync_requests::set(ProcessSyncRequests);
    // absorb_sync_requests: the checkpointer's queue drain (aux-mains lane).
}

#[cfg(test)]
pub(crate) fn pending_counts() -> (usize, usize) {
    with_pending(|p| (p.ops.len(), p.unlinks.len())).unwrap_or((0, 0))
}

#[cfg(test)]
mod tests;
