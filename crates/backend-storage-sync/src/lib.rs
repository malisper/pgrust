//! `sync.c` — file synchronization management code
//! (`src/backend/storage/sync/sync.c`).
//!
//! The data-file fsync/unlink request mechanism used by the checkpointer (and
//! standalone backends / the startup process). We remember every relation
//! segment written since the last checkpoint so it can be fsync'd before the
//! next checkpoint completes (`pendingOps`), and remember no-longer-needed
//! files to delete after the next checkpoint (`pendingUnlinks`). Regular
//! backends do not track pending operations locally; they forward them to the
//! checkpointer.
//!
//! # Process-local, NOT shared memory
//!
//! `pendingOps` / `pendingUnlinks` / `pendingOpsCxt` and the two cycle counters
//! (`sync_cycle_ctr`, `checkpoint_cycle_ctr`) plus `sync_in_progress` are
//! checkpointer-process-local in C — file-static globals living in that
//! process's own address space (the `pendingOpsCxt` dedicated context), NOT
//! shared memory. Per the AGENTS.md backend-global-state rule they become a
//! `thread_local!` [`SyncState`]; per `docs/mctx-design.md` decision 5 a
//! backend-lifetime structure stored in a `thread_local!` uses plain owned
//! collections (`HashMap`/`Vec`), reset only by wholesale teardown — exactly
//! C's `MemoryContextDelete(pendingOpsCxt)`. The data-derived growth points
//! (`pendingOps`/`pendingUnlinks`) reserve fallibly (`try_reserve`), so a
//! `palloc` OOM becomes an `Err(PgError)` rather than a process abort.
//!
//! The request forwarding to the checkpointer (`RegisterSyncRequest` ->
//! `ForwardSyncRequest`) is the genuine shared-memory path; that queue lives in
//! `checkpointer.c` and is reached through the checkpointer's seams, as are
//! `AbsorbSyncRequests` and the no-latch `WaitLatch` retry sleep.
//!
//! # The sync-handler switch table (`syncsw`)
//!
//! `syncsw[]` is the per-`SyncRequestHandler` vtable: `sync.c` OWNS the
//! dispatch on `FileTag.handler`, but the handler bodies
//! (`mdsyncfiletag`/`clogsyncfiletag`/…) belong to other subsystems. The
//! dispatch lives here ([`sync_filetag`]/[`unlink_filetag`]/[`filetag_matches`])
//! and routes each handler arm to that owner's seam.

#![allow(clippy::result_large_err)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Instant;

use backend_utils_error::ereport;
use types_error::{DEBUG1, ERROR, ErrorLocation, PgError, PgResult, WARNING};

use types_storage::sync::{FileTag, FileTagOpResult, SyncRequestHandler, SyncRequestType};

// ===========================================================================
// Constants transcribed 1:1 from sync.c.
// ===========================================================================

/// `FSYNCS_PER_ABSORB` (`sync.c:78`).
const FSYNCS_PER_ABSORB: i32 = 10;
/// `UNLINKS_PER_ABSORB` (`sync.c:79`).
const UNLINKS_PER_ABSORB: i32 = 10;

/// `typedef uint16 CycleCtr` (`sync.c:54`). The `(CycleCtr)(x + 1)` wraparound
/// arithmetic relies on this being exactly 16-bit.
type CycleCtr = u16;

/// `ENOENT` — "no such file or directory". `ProcessSyncRequests` /
/// `SyncPostCheckpoint` compare the handler's saved errno against it.
const ENOENT: i32 = libc::ENOENT;

// ===========================================================================
// Entry/list element types (sync.c:56-68).
// ===========================================================================

/// `PendingFsyncEntry` (`sync.c:56-61`) — the value in `pendingOps`. The C
/// struct's `FileTag tag` member is the map key (so it is not stored again in
/// the value).
#[derive(Clone, Copy, Debug)]
struct PendingFsyncEntry {
    /// `CycleCtr cycle_ctr` — `sync_cycle_ctr` of the oldest request.
    cycle_ctr: CycleCtr,
    /// `bool canceled` — true if we canceled this request "recently".
    canceled: bool,
}

/// `PendingUnlinkEntry` (`sync.c:63-68`) — an element of `pendingUnlinks`. The
/// tag is stored inline (unlinks live in a list, not a hash table).
#[derive(Clone, Copy, Debug)]
struct PendingUnlinkEntry {
    /// `FileTag tag` — identifies handler and file.
    tag: FileTag,
    /// `CycleCtr cycle_ctr` — `checkpoint_cycle_ctr` when the request was made.
    cycle_ctr: CycleCtr,
    /// `bool canceled` — true if the request has been canceled.
    canceled: bool,
}

// ===========================================================================
// Process-local pending-operations state (sync.c:70-75 + the function-static
// sync_in_progress at sync.c:288), gathered into one thread_local backend-global.
// ===========================================================================

/// All of `sync.c`'s checkpointer-process-local state. The collections are
/// plain owned (`docs/mctx-design.md` decision 5: a backend-lifetime structure
/// stored in a `thread_local!`), reset only by wholesale teardown.
struct SyncState {
    /// `static HTAB *pendingOps` (`sync.c:70`) — `Some` once `InitSync` decides
    /// this process tracks pending operations; `None` otherwise (the
    /// `pendingOps != NULL` test). The `HASH_BLOBS` whole-struct key equality
    /// is the derived `Hash`/`Eq` on [`FileTag`], which merges duplicate fsync
    /// requests exactly as the C hash table does.
    pending_ops: Option<HashMap<FileTag, PendingFsyncEntry>>,
    /// `static List *pendingUnlinks` (`sync.c:71`).
    pending_unlinks: Vec<PendingUnlinkEntry>,
    /// `static CycleCtr sync_cycle_ctr` (`sync.c:74`).
    sync_cycle_ctr: CycleCtr,
    /// `static CycleCtr checkpoint_cycle_ctr` (`sync.c:75`).
    checkpoint_cycle_ctr: CycleCtr,
    /// `ProcessSyncRequests`'s function-static `sync_in_progress`
    /// (`sync.c:288`), hoisted so it survives across calls like the C static.
    sync_in_progress: bool,
}

impl SyncState {
    const fn new() -> Self {
        Self {
            pending_ops: None,
            pending_unlinks: Vec::new(),
            sync_cycle_ctr: 0,
            checkpoint_cycle_ctr: 0,
            sync_in_progress: false,
        }
    }
}

thread_local! {
    static SYNC_STATE: RefCell<SyncState> = const { RefCell::new(SyncState::new()) };
}

fn with_state<R>(f: impl FnOnce(&mut SyncState) -> R) -> R {
    SYNC_STATE.with(|cell| f(&mut cell.borrow_mut()))
}

// ===========================================================================
// Allocation-safety helper (HARD RULE): pendingOps / pendingUnlinks grow with
// data, so each growth reserves fallibly, turning OOM into a PgError.
// ===========================================================================

fn oom() -> PgError {
    PgError::error("out of memory while tracking pending sync requests")
}

// ===========================================================================
// instr_time helpers (instr_time.h), used by ProcessSyncRequests timing.
// ===========================================================================

/// `INSTR_TIME_SET_CURRENT(t)` — we only ever subtract two readings, so an
/// [`Instant`] reproduces the elapsed interval faithfully.
#[inline]
fn instr_time_set_current() -> Instant {
    Instant::now()
}

/// `INSTR_TIME_SUBTRACT` + `INSTR_TIME_GET_MICROSEC` — elapsed `start`..now in
/// microseconds (saturating, like the C `uint64` cast).
#[inline]
fn elapsed_microsec(start: Instant) -> u64 {
    Instant::now()
        .duration_since(start)
        .as_micros()
        .min(u64::MAX as u128) as u64
}

/// `FILE_POSSIBLY_DELETED(err)` (`fd.h`) — on non-Windows this is
/// `err == ENOENT`. (The Windows variant also allows `EACCES`; this port
/// targets Unix-like platforms.)
#[inline]
fn file_possibly_deleted(err: i32) -> bool {
    err == ENOENT
}

/// The `__FILE__` / `__func__` location every `ereport` in `sync.c` carries.
fn sync_location(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("sync.c", 0, funcname)
}

// ===========================================================================
// syncsw[] vtable dispatch (sync.c:95-118). sync.c OWNS the dispatch; each
// handler body lives in its subsystem's crate, reached through that owner's
// seam. CLOG/COMMIT_TS/MULTIXACT_* define only sync_syncfiletag; only MD
// defines unlink/match. SYNC_HANDLER_NONE has no row, so it is an error to
// dispatch through it (C would index past the populated entries).
// ===========================================================================

fn sync_filetag(handler: SyncRequestHandler, ftag: FileTag) -> PgResult<FileTagOpResult> {
    match handler {
        SyncRequestHandler::SYNC_HANDLER_MD => {
            backend_storage_smgr_md_seams::mdsyncfiletag::call(ftag)
        }
        SyncRequestHandler::SYNC_HANDLER_CLOG => {
            backend_access_transam_clog_seams::clogsyncfiletag::call(ftag)
        }
        SyncRequestHandler::SYNC_HANDLER_COMMIT_TS => {
            backend_access_transam_commit_ts_seams::committssyncfiletag::call(ftag)
        }
        SyncRequestHandler::SYNC_HANDLER_MULTIXACT_OFFSET => {
            backend_access_transam_multixact_seams::multixactoffsetssyncfiletag::call(ftag)
        }
        SyncRequestHandler::SYNC_HANDLER_MULTIXACT_MEMBER => {
            backend_access_transam_multixact_seams::multixactmemberssyncfiletag::call(ftag)
        }
        SyncRequestHandler::SYNC_HANDLER_NONE => Err(PgError::error(
            "sync_filetag: SYNC_HANDLER_NONE has no syncsw row",
        )),
    }
}

fn unlink_filetag(handler: SyncRequestHandler, ftag: FileTag) -> PgResult<FileTagOpResult> {
    match handler {
        SyncRequestHandler::SYNC_HANDLER_MD => {
            backend_storage_smgr_md_seams::mdunlinkfiletag::call(ftag)
        }
        // Only SYNC_HANDLER_MD populates .sync_unlinkfiletag; the others leave
        // it NULL, so C would call through a null pointer if reached. sync.c
        // only ever issues unlink requests for md-handled tags.
        _ => Err(PgError::error(
            "unlink_filetag: handler has no sync_unlinkfiletag",
        )),
    }
}

fn filetag_matches(
    handler: SyncRequestHandler,
    ftag: FileTag,
    candidate: FileTag,
) -> PgResult<bool> {
    match handler {
        SyncRequestHandler::SYNC_HANDLER_MD => {
            backend_storage_smgr_md_seams::mdfiletagmatches::call(ftag, candidate)
        }
        // Only SYNC_HANDLER_MD populates .sync_filetagmatches.
        _ => Err(PgError::error(
            "filetag_matches: handler has no sync_filetagmatches",
        )),
    }
}

// ===========================================================================
// sync.c functions (1:1).
// ===========================================================================

/// `InitSync(void)` (`sync.c:123-158`) — initialize the file-sync tracking data
/// structures, but only if this process needs them.
///
/// `create_pending_ops` is C's `(!IsUnderPostmaster || AmCheckpointerProcess())`
/// decision, computed by the caller off its own environment (avoiding an
/// ambient-global getter seam).
fn init_sync(state: &mut SyncState, create_pending_ops: bool) {
    if create_pending_ops {
        // C creates `pendingOpsCxt` (allowed in critical sections) and a
        // 100-bucket HASH_BLOBS table keyed on FileTag, and sets
        // pendingUnlinks = NIL. Here the backend-global state holds owned
        // collections; the FileTag key's derived Hash/Eq reproduces HASH_BLOBS.
        state.pending_ops = Some(HashMap::new());
        state.pending_unlinks = Vec::new();
    }
}

/// `SyncPreCheckpoint(void)` (`sync.c:176-194`) — pre-checkpoint work.
fn sync_pre_checkpoint(state: &mut SyncState) -> PgResult<()> {
    // Ensure unlink requests forwarded before the checkpoint began are
    // processed in the current checkpoint.
    backend_postmaster_checkpointer_seams::absorb_sync_requests::call()?;

    // Any unlink requests arriving after this point get the next cycle counter
    // and won't be unlinked until the next checkpoint.
    state.checkpoint_cycle_ctr = state.checkpoint_cycle_ctr.wrapping_add(1);
    Ok(())
}

/// `SyncPostCheckpoint(void)` (`sync.c:201-280`) — remove lingering files that
/// can now be safely removed.
fn sync_post_checkpoint(state: &mut SyncState) -> PgResult<()> {
    let mut absorb_counter = UNLINKS_PER_ABSORB;

    // foreach(lc, pendingUnlinks). We iterate by index so we can compute the
    // C `lc` stop-position exactly as the list_cell_number/list_delete_first_n
    // logic does. New entries appended by an interleaved AbsorbSyncRequests are
    // added at the end and are NOT visited beyond the original length (matching
    // C: new entries carry the new cycle_ctr that the break below stops on).
    let checkpoint_cycle_ctr = state.checkpoint_cycle_ctr;
    let len = state.pending_unlinks.len();

    // `reached_end` mirrors C's `lc == NULL` after the loop; `stop_index` is the
    // index of the first not-yet-processed old entry (the C `lc`).
    let mut reached_end = true;
    let mut stop_index = len;

    let mut i = 0;
    while i < len {
        let entry = state.pending_unlinks[i];

        // Skip over any canceled entries.
        if entry.canceled {
            i += 1;
            continue;
        }

        // A new entry means we've reached the end of the old entries.
        if entry.cycle_ctr == checkpoint_cycle_ctr {
            reached_end = false;
            stop_index = i;
            break;
        }

        // Unlink the file. `FileTag.handler` is a typed `SyncRequestHandler`
        // (the C `int16` index into `syncsw[]`, now a checked enum), so no
        // range check is needed.
        let handler = entry.tag.handler;
        let op = unlink_filetag(handler, entry.tag)?;
        if op.result < 0 {
            // Race: DROP DATABASE may have deleted the file first, yielding
            // ENOENT (rmtree() ignores ENOENT too). Warn on anything else.
            if op.errno != ENOENT {
                ereport(WARNING)
                    .with_saved_errno(op.errno)
                    .errcode_for_file_access()
                    .errmsg(format!("could not remove file \"{}\": %m", op.path))
                    .finish(sync_location("SyncPostCheckpoint"))?;
            }
        }

        // Mark the list entry as canceled, just in case.
        state.pending_unlinks[i].canceled = true;

        // As in ProcessSyncRequests, don't stop absorbing fsync requests for a
        // long time when there are many deletions to do.
        absorb_counter -= 1;
        if absorb_counter <= 0 {
            backend_postmaster_checkpointer_seams::absorb_sync_requests::call()?;
            absorb_counter = UNLINKS_PER_ABSORB;
        }

        i += 1;
    }

    // If we reached the end of the list, drop the whole list; otherwise keep the
    // entries at or after the stop cell (C frees the first `ntodelete` and does
    // list_delete_first_n).
    if reached_end {
        // list_free_deep(pendingUnlinks); pendingUnlinks = NIL;
        state.pending_unlinks.clear();
    } else {
        // ntodelete == stop_index: drain the leading processed entries.
        state.pending_unlinks.drain(0..stop_index);
    }
    Ok(())
}

/// `ProcessSyncRequests(void)` (`sync.c:285-475`) — process queued fsync
/// requests. Called during checkpoints (only in a process that created
/// `pendingOps`). The `enableFsync` / `log_checkpoints` GUC values are passed
/// in (read off the caller's GUC state, not an ambient getter).
fn process_sync_requests(
    state: &mut SyncState,
    enable_fsync: bool,
    log_checkpoints: bool,
) -> PgResult<()> {
    // Statistics on sync times.
    let mut processed: i32 = 0;
    let mut longest: u64 = 0;
    let mut total_elapsed: u64 = 0;

    // This is only called during checkpoints, which only occur in processes that
    // created a pendingOps.
    if state.pending_ops.is_none() {
        return ereport(ERROR)
            .errmsg_internal("cannot sync without a pendingOps table")
            .finish(sync_location("ProcessSyncRequests"));
    }

    // The checkpointer must absorb all fsync requests queued by backends up to
    // this point (the BufferSync() race in the C comment).
    backend_postmaster_checkpointer_seams::absorb_sync_requests::call()?;

    // To avoid excess fsync'ing, ignore fsync requests entered after this point;
    // they'll be processed next time. If the previous run failed, forcibly
    // refresh stale cycle_ctr values to forestall wraparound aliasing.
    //
    // Think not to merge this loop with the main loop, as the problem is exactly
    // that the main loop may fail before having visited all the entries.
    if state.sync_in_progress {
        let ctr = state.sync_cycle_ctr;
        let ops = state
            .pending_ops
            .as_mut()
            .ok_or_else(|| PgError::error("ProcessSyncRequests: pendingOps is NULL"))?;
        for entry in ops.values_mut() {
            entry.cycle_ctr = ctr;
        }
    }

    // Advance the counter so new hashtable entries are distinguishable, and set
    // the flag to detect failure if we don't reach the end of the loop.
    state.sync_cycle_ctr = state.sync_cycle_ctr.wrapping_add(1);
    state.sync_in_progress = true;
    let sync_cycle_ctr = state.sync_cycle_ctr;

    // Now scan the hashtable for fsync requests to process.
    //
    // C uses hash_seq_search, where it is "unspecified whether newly-added
    // entries will be visited" — and it doesn't care, since new entries are
    // skipped by the cycle_ctr test. We snapshot the set of keys present now;
    // entries added by an interleaved AbsorbSyncRequests are not visited this
    // pass, which is within the C contract. Each key is re-fetched live before
    // use (a concurrent cancel via Absorb sets `canceled`).
    let mut absorb_counter = FSYNCS_PER_ABSORB;
    let keys: Vec<FileTag> = {
        let ops = state
            .pending_ops
            .as_ref()
            .ok_or_else(|| PgError::error("ProcessSyncRequests: pendingOps is NULL"))?;
        let mut keys = Vec::new();
        keys.try_reserve(ops.len()).map_err(|_| oom())?;
        keys.extend(ops.keys().copied());
        keys
    };

    for tag in keys {
        // Re-fetch the current entry; an intervening absorb may have
        // removed/canceled it. ("continue" matches dynahash handing back nothing
        // for a vanished key.)
        let entry = {
            let ops = state
                .pending_ops
                .as_ref()
                .ok_or_else(|| PgError::error("ProcessSyncRequests: pendingOps is NULL"))?;
            match ops.get(&tag).copied() {
                Some(e) => e,
                None => continue,
            }
        };

        // If the entry is new then don't process it this time. ("continue"
        // bypasses the hash-remove at the bottom of the loop.)
        if entry.cycle_ctr == sync_cycle_ctr {
            continue;
        }

        // Else assert we haven't missed it.
        debug_assert_eq!(entry.cycle_ctr.wrapping_add(1), sync_cycle_ctr);

        // If fsync is off we needn't open the file at all (checked here so
        // changing fsync on the fly behaves sensibly).
        if enable_fsync {
            // In the checkpointer, absorb pending requests periodically to
            // prevent fsync-queue overflow.
            absorb_counter -= 1;
            if absorb_counter <= 0 {
                backend_postmaster_checkpointer_seams::absorb_sync_requests::call()?;
                absorb_counter = FSYNCS_PER_ABSORB;
            }

            // `FileTag.handler` is a typed `SyncRequestHandler` (the checked
            // `syncsw[]` index), so it needs no range check.
            let handler = tag.handler;

            // The fsync table may name segments since deleted/unlinked. On
            // error, absorb pending requests and retry: mdunlink() queues a
            // "cancel" before unlinking, so the request is guaranteed marked
            // canceled after the absorb if it was this case. DROP DATABASE
            // likewise tells us to forget fsync requests before deleting.
            let mut failures = 0;
            // for (failures = 0; !entry->canceled; failures++)
            loop {
                // Re-fetch the live canceled flag (an interleaved Absorb may
                // have set it).
                let canceled = {
                    let ops = state
                        .pending_ops
                        .as_ref()
                        .ok_or_else(|| PgError::error("ProcessSyncRequests: pendingOps is NULL"))?;
                    match ops.get(&tag) {
                        Some(e) => e.canceled,
                        // The entry vanished; treat as canceled (loop ends).
                        None => true,
                    }
                };
                if canceled {
                    break;
                }

                let sync_start = instr_time_set_current();
                let op = sync_filetag(handler, tag)?;
                if op.result == 0 {
                    // Success; update statistics about sync timing.
                    let elapsed = elapsed_microsec(sync_start);
                    if elapsed > longest {
                        longest = elapsed;
                    }
                    total_elapsed += elapsed;
                    processed += 1;

                    if log_checkpoints {
                        ereport(DEBUG1)
                            .errmsg_internal(format!(
                                "checkpoint sync: number={} file={} time={:.3} ms",
                                processed,
                                op.path,
                                elapsed as f64 / 1000.0
                            ))
                            .finish(sync_location("ProcessSyncRequests"))?;
                    }

                    break; // out of retry loop
                }

                // The relation may have been dropped or truncated since the
                // request was entered. Allow ENOENT, but only if we didn't
                // already fail on this file.
                if !file_possibly_deleted(op.errno) || failures > 0 {
                    let elevel = backend_storage_file_seams::data_sync_elevel::call(ERROR);
                    return ereport(elevel)
                        .with_saved_errno(op.errno)
                        .errcode_for_file_access()
                        .errmsg(format!("could not fsync file \"{}\": %m", op.path))
                        .finish(sync_location("ProcessSyncRequests"));
                }
                // else: a tolerated first-attempt ENOENT — log and retry after
                // an absorb.
                ereport(DEBUG1)
                    .with_saved_errno(op.errno)
                    .errcode_for_file_access()
                    .errmsg_internal(format!(
                        "could not fsync file \"{}\" but retrying: %m",
                        op.path
                    ))
                    .finish(sync_location("ProcessSyncRequests"))?;

                // Absorb incoming requests and check whether a cancel arrived
                // for this relation fork.
                backend_postmaster_checkpointer_seams::absorb_sync_requests::call()?;
                absorb_counter = FSYNCS_PER_ABSORB; // might as well...

                failures += 1;
            } // end retry loop
        }

        // We are done with this entry, remove it.
        let removed = state
            .pending_ops
            .as_mut()
            .ok_or_else(|| PgError::error("ProcessSyncRequests: pendingOps is NULL"))?
            .remove(&tag);
        if removed.is_none() {
            return ereport(ERROR)
                .errmsg_internal("pendingOps corrupted")
                .finish(sync_location("ProcessSyncRequests"));
        }
    } // end loop over hashtable entries

    // Return sync performance metrics for the checkpoint-end report.
    backend_postmaster_checkpointer_seams::checkpoint_stats_set::call(
        processed,
        longest,
        total_elapsed,
    );

    // Flag successful completion.
    state.sync_in_progress = false;
    Ok(())
}

/// `RememberSyncRequest(const FileTag *ftag, SyncRequestType type)`
/// (`sync.c:486-571`) — checkpointer-side callback that stuffs an fsync request
/// into the local hash table (or, for UNLINK, the list) for execution at the
/// next checkpoint, and processes forget/filter cancellations.
fn remember_sync_request(
    state: &mut SyncState,
    ftag: &FileTag,
    request_type: SyncRequestType,
) -> PgResult<()> {
    debug_assert!(state.pending_ops.is_some());

    match request_type {
        SyncRequestType::SYNC_FORGET_REQUEST => {
            // Cancel a previously entered request.
            if let Some(entry) = state
                .pending_ops
                .as_mut()
                .and_then(|ops| ops.get_mut(ftag))
            {
                entry.canceled = true;
            }
        }
        SyncRequestType::SYNC_FILTER_REQUEST => {
            // `FileTag.handler` is a typed `SyncRequestHandler`; no range check.
            let handler = ftag.handler;

            // Cancel matching fsync requests. The filetag-match callback lives
            // behind a seam, so collect the same-handler candidate keys first,
            // then test each.
            let candidates: Vec<FileTag> = {
                let ops = state
                    .pending_ops
                    .as_ref()
                    .ok_or_else(|| PgError::error("RememberSyncRequest: pendingOps is NULL"))?;
                let mut v = Vec::new();
                for &k in ops.keys() {
                    if k.handler == ftag.handler {
                        v.try_reserve(1).map_err(|_| oom())?;
                        v.push(k);
                    }
                }
                v
            };
            for candidate in candidates {
                if filetag_matches(handler, *ftag, candidate)? {
                    if let Some(e) = state
                        .pending_ops
                        .as_mut()
                        .and_then(|ops| ops.get_mut(&candidate))
                    {
                        e.canceled = true;
                    }
                }
            }

            // Cancel matching unlink requests. Snapshot the same-handler indices
            // so we can test+mutate by index without aliasing the list.
            let unlink_idx: Vec<usize> = {
                let mut v = Vec::new();
                for (i, pue) in state.pending_unlinks.iter().enumerate() {
                    if pue.tag.handler == ftag.handler {
                        v.try_reserve(1).map_err(|_| oom())?;
                        v.push(i);
                    }
                }
                v
            };
            for i in unlink_idx {
                let candidate = state.pending_unlinks[i].tag;
                if filetag_matches(handler, *ftag, candidate)? {
                    state.pending_unlinks[i].canceled = true;
                }
            }
        }
        SyncRequestType::SYNC_UNLINK_REQUEST => {
            // Unlink request: put it on the list (charged to pendingOpsCxt in C
            // via MemoryContextSwitchTo). Reserve fallibly on growth.
            let entry = PendingUnlinkEntry {
                tag: *ftag,
                cycle_ctr: state.checkpoint_cycle_ctr,
                canceled: false,
            };
            state.pending_unlinks.try_reserve(1).map_err(|_| oom())?;
            state.pending_unlinks.push(entry);
        }
        SyncRequestType::SYNC_REQUEST => {
            // Normal case: enter a request to fsync this segment.
            let cycle_ctr = state.sync_cycle_ctr;
            // hash_search(pendingOps, ftag, HASH_ENTER, &found): inspect the
            // existing entry first, then act.
            let existing = state
                .pending_ops
                .as_ref()
                .ok_or_else(|| PgError::error("RememberSyncRequest: pendingOps is NULL"))?
                .get(ftag)
                .copied();
            match existing {
                // Existing, non-canceled entry: intentionally leave cycle_ctr
                // unchanged — it must represent the OLDEST fsync request that
                // could be in the entry.
                Some(entry) if !entry.canceled => {}
                // Existing but previously canceled: re-initialize it.
                Some(_) => {
                    let e = state
                        .pending_ops
                        .as_mut()
                        .ok_or_else(|| PgError::error("RememberSyncRequest: pendingOps is NULL"))?
                        .get_mut(ftag)
                        .ok_or_else(|| {
                            PgError::error("RememberSyncRequest: pendingOps entry vanished")
                        })?;
                    e.cycle_ctr = cycle_ctr;
                    e.canceled = false;
                }
                // New entry: reserve fallibly before inserting (data-derived
                // growth), then do the infallible insert.
                None => {
                    let ops = state
                        .pending_ops
                        .as_mut()
                        .ok_or_else(|| PgError::error("RememberSyncRequest: pendingOps is NULL"))?;
                    ops.try_reserve(1).map_err(|_| oom())?;
                    ops.insert(
                        *ftag,
                        PendingFsyncEntry {
                            cycle_ctr,
                            canceled: false,
                        },
                    );
                }
            }
        }
    }
    Ok(())
}

/// `RegisterSyncRequest(const FileTag *ftag, SyncRequestType type, bool
/// retryOnError)` (`sync.c:579-619`) — register the sync request locally
/// (standalone / startup process: fsync state is local), or forward it to the
/// checkpointer. Returns `true` on success, `false` if there wasn't space.
fn register_sync_request(
    state: &mut SyncState,
    ftag: &FileTag,
    request_type: SyncRequestType,
    retry_on_error: bool,
) -> PgResult<bool> {
    if state.pending_ops.is_some() {
        // Standalone backend or startup process: fsync state is local.
        remember_sync_request(state, ftag, request_type)?;
        return Ok(true);
    }

    loop {
        // Notify the checkpointer. If we fail to queue in retryOnError mode,
        // sleep and try again. (ForwardSyncRequest lives in checkpointer.c and
        // is the genuine shared-memory request-queue path.)
        let ret = backend_postmaster_checkpointer_seams::forward_sync_request::call(
            *ftag,
            request_type,
        )?;

        // Break if we queued it, or failed but were told not to retry.
        if ret || !retry_on_error {
            return Ok(ret);
        }

        backend_storage_ipc_latch_seams::wait_latch_register_sync_request::call()?;
    }
}

// ===========================================================================
// Seam installers: thin marshal + delegate over the thread_local state.
// ===========================================================================

fn init_sync_seam(create_pending_ops: bool) {
    with_state(|s| init_sync(s, create_pending_ops));
}

fn sync_pre_checkpoint_seam() -> PgResult<()> {
    with_state(sync_pre_checkpoint)
}

fn sync_post_checkpoint_seam() -> PgResult<()> {
    with_state(sync_post_checkpoint)
}

fn process_sync_requests_seam(enable_fsync: bool, log_checkpoints: bool) -> PgResult<()> {
    with_state(|s| process_sync_requests(s, enable_fsync, log_checkpoints))
}

fn register_sync_request_seam(
    ftag: FileTag,
    request_type: SyncRequestType,
    retry_on_error: bool,
) -> PgResult<bool> {
    with_state(|s| register_sync_request(s, &ftag, request_type, retry_on_error))
}

/// Install every seam in `backend-storage-sync-seams`.
pub fn init_seams() {
    backend_storage_sync_seams::init_sync::set(init_sync_seam);
    backend_storage_sync_seams::sync_pre_checkpoint::set(sync_pre_checkpoint_seam);
    backend_storage_sync_seams::sync_post_checkpoint::set(sync_post_checkpoint_seam);
    backend_storage_sync_seams::process_sync_requests::set(process_sync_requests_seam);
    backend_storage_sync_seams::register_sync_request::set(register_sync_request_seam);
}

#[cfg(test)]
mod tests;
