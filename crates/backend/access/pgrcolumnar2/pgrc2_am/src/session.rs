//! Session-envelope state + transaction hooks: the pgrcolumnar2 mirror of
//! the `catalog_storage` pendingDeletes discipline for O-7 table
//! DIRECTORIES, and the placement of the M3-D `WriterRegistry`
//! (`pgrc2_write::writer` doc: "M3-H owns placement + the xact-callback
//! registration").
//!
//! ## The pendingDirDeletes law (smgrDoPendingDeletes, transcribed)
//!
//! - Entries carry `(path, at_commit, nest_level)`.
//! - Lazily-created directories register `at_commit = false` (unlink on
//!   abort of the creating (sub)transaction).
//! - DROP TABLE / relfilelocator swap register `at_commit = true` (unlink
//!   only once the dropping transaction commits).
//! - Top-level COMMIT/ABORT: entries whose `at_commit == is_commit` are
//!   deleted; the rest are dropped. Deletion failures WARN and continue
//!   (the smgrdounlinkall posture — a missed unlink is dead-band garbage,
//!   never an aborted commit).
//! - Subcommit: entries at `nest_level >= cur` reparent to `cur - 1`.
//! - Subabort: entries at `nest_level >= cur` are processed with
//!   `is_commit = false` (creation rolled back ⇒ unlink now; DROP rolled
//!   back ⇒ forget).
//! - PREPARE: pgrcolumnar2 DDL/ingest does not support 2PC at M3 — a
//!   PREPARE with pending directory ops or buffered writers refuses typed
//!   (the pre-commit callback fires before the prepare record is built).
//!
//! ## Writer registry placement
//!
//! One [`WriterRegistry`] per backend thread, purged UNCONDITIONALLY at
//! top-level commit AND abort (the M3-D law: the successful statement
//! already took its writer at publish; anything left is abandoned).
//!
//! ## TLS census
//!
//! Exactly ONE `thread_local!` block (all cells co-located) — the
//! session-surface census counts declaration blocks; its ledger entry names
//! this module.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

use pgrc2_write::writer::WriterRegistry;
use pgrc2_write::wvfs::RealVfs;
use types_core::xact::{SubXactEvent, XactEvent};
use types_core::SubTransactionId;
use types_error::PgResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingDirDelete {
    pub path: String,
    pub at_commit: bool,
    pub nest_level: i32,
}

thread_local! {
    // ONE block (TLS census +1): writer registry, pending directory ops,
    // the once-per-backend hook-registration latch, the relcache
    // invalidation-callback latch (inval.rs's), and the per-statement
    // election-candidate cache (ingest.rs's — CodecCandidates is a pure
    // function of the relation schema; rebuilt per transaction, purged with
    // the writers). Co-located so the crate stays at exactly one census row.
    static WRITERS: RefCell<WriterRegistry> = RefCell::new(WriterRegistry::new());
    static PENDING_DIRS: RefCell<Vec<PendingDirDelete>> = const { RefCell::new(Vec::new()) };
    static HOOKS_REGISTERED: Cell<bool> = const { Cell::new(false) };
    static INVAL_REGISTERED: Cell<bool> = const { Cell::new(false) };
    static CANDIDATES: RefCell<BTreeMap<u64, pgrc2_write::elect::CodecCandidates>> =
        const { RefCell::new(BTreeMap::new()) };
    // relfilenumber → folded footer facts (footer.rs — plan/ANALYZE-time
    // rows/bytes/NDV, keyed by manifest generation inside the entry; the
    // resolve that precedes every read makes a stale serve structurally
    // impossible, so entries live for the session like the old AM's part
    // cache).
    static FOOTER_FACTS: RefCell<BTreeMap<u64, crate::footer::FooterFacts>> =
        const { RefCell::new(BTreeMap::new()) };
}

/// Per-relfilenumber footer-fact cache accessor (footer.rs owns the fold
/// and the generation-keyed staleness discipline).
pub fn with_footer_facts<R>(
    f: impl FnOnce(&mut BTreeMap<u64, crate::footer::FooterFacts>) -> R,
) -> R {
    FOOTER_FACTS.with(|c| f(&mut c.borrow_mut()))
}

/// Per-relfilenumber election-candidate cache accessor (ingest.rs builds
/// entries lazily from the relation schema; cleared with the writer purge
/// so schema lifetime is bounded by the transaction).
pub fn with_candidates<R>(
    f: impl FnOnce(&mut BTreeMap<u64, pgrc2_write::elect::CodecCandidates>) -> R,
) -> R {
    CANDIDATES.with(|c| f(&mut c.borrow_mut()))
}

/// The inval-callback latch accessor (the cell lives in this crate's single
/// census-pinned block; the logic lives in [`crate::inval`]).
pub fn with_inval_latch<R>(f: impl FnOnce(&Cell<bool>) -> R) -> R {
    INVAL_REGISTERED.with(f)
}

/// Run `f` over this backend's writer registry.
pub fn with_writers<R>(f: impl FnOnce(&mut WriterRegistry) -> R) -> R {
    WRITERS.with(|w| f(&mut w.borrow_mut()))
}

/// Register the xact + subxact callbacks once per backend thread (the
/// tableam `CB_EOXACT_REGISTERED` precedent). MUST be called before any
/// state lands in this module (every schedule/ingest entry point does).
pub fn ensure_session_hooks() {
    HOOKS_REGISTERED.with(|c| {
        if !c.get() {
            xact::RegisterXactCallback(pgrc2_xact_callback, datum::Datum::null());
            xact::RegisterSubXactCallback(pgrc2_subxact_callback, datum::Datum::null());
            c.set(true);
        }
    });
}

fn cur_nest_level() -> i32 {
    xact_seams::get_current_transaction_nest_level::call()
}

/// Schedule the table directory for deletion when the current transaction
/// COMMITS (DROP TABLE / relfilelocator swap).
pub fn schedule_dir_delete_at_commit(path: String) {
    ensure_session_hooks();
    let nest_level = cur_nest_level();
    PENDING_DIRS.with(|p| {
        p.borrow_mut().push(PendingDirDelete {
            path,
            at_commit: true,
            nest_level,
        })
    });
}

/// Schedule the (just-created) table directory for deletion if the current
/// (sub)transaction ABORTS.
pub fn schedule_dir_delete_at_abort(path: String) {
    ensure_session_hooks();
    let nest_level = cur_nest_level();
    PENDING_DIRS.with(|p| {
        p.borrow_mut().push(PendingDirDelete {
            path,
            at_commit: false,
            nest_level,
        })
    });
}

/// The pure pending-op partition (unit-tested): split `pending` into
/// (delete-now paths, keep entries) for an end event at `nest_level` with
/// commit/abort disposition `is_commit`. Top-level events pass
/// `nest_level = 1` and keep nothing.
pub fn partition_pending(
    pending: Vec<PendingDirDelete>,
    nest_level: i32,
    is_commit: bool,
) -> (Vec<String>, Vec<PendingDirDelete>) {
    let mut delete = Vec::new();
    let mut keep = Vec::new();
    for e in pending {
        if e.nest_level >= nest_level {
            if e.at_commit == is_commit {
                delete.push(e.path);
            }
            // at_commit != is_commit ⇒ the op is cancelled (a DROP whose
            // transaction aborted / a creation whose transaction committed).
        } else {
            keep.push(e);
        }
    }
    (delete, keep)
}

/// The pure subcommit reparent (unit-tested): mirror `AtSubCommit_smgr`.
pub fn reparent_pending(pending: &mut [PendingDirDelete], nest_level: i32) {
    for e in pending.iter_mut() {
        if e.nest_level >= nest_level {
            e.nest_level = nest_level - 1;
        }
    }
}

fn do_pending_dir_deletes(nest_level: i32, is_commit: bool) {
    let pending = PENDING_DIRS.with(|p| std::mem::take(&mut *p.borrow_mut()));
    let (delete, keep) = partition_pending(pending, nest_level, is_commit);
    PENDING_DIRS.with(|p| *p.borrow_mut() = keep);
    for path in delete {
        if let Err(e) = crate::dirpath::remove_table_dir(&path) {
            // The smgrdounlinkall posture: warn and continue — the
            // transaction outcome is already decided; a missed unlink is
            // reclaimable garbage, never an error surfaced to the client.
            let _ = elog::elog(
                types_error::WARNING,
                format!("pgrcolumnar2: could not remove table directory {path}: {e}"),
            );
        }
    }
}

fn purge_writers() {
    CANDIDATES.with(|c| c.borrow_mut().clear());
    let mut vfs = RealVfs;
    WRITERS.with(|w| {
        if let Err(e) = w.borrow_mut().at_eoxact(&mut vfs) {
            let _ = elog::elog(
                types_error::WARNING,
                format!("pgrcolumnar2: writer purge at transaction end failed: {e}"),
            );
        }
    });
}

fn pgrc2_xact_callback(event: XactEvent, _arg: datum::Datum) -> PgResult<()> {
    use XactEvent::*;
    match event {
        XACT_EVENT_COMMIT | XACT_EVENT_PARALLEL_COMMIT => {
            do_pending_dir_deletes(1, true);
            purge_writers();
        }
        XACT_EVENT_ABORT | XACT_EVENT_PARALLEL_ABORT => {
            do_pending_dir_deletes(1, false);
            purge_writers();
        }
        XACT_EVENT_PRE_PREPARE => {
            // 2PC refusal fires while erroring is still allowed.
            let dirty = PENDING_DIRS.with(|p| !p.borrow().is_empty())
                || WRITERS.with(|w| !w.borrow().is_empty());
            if dirty {
                return Err(crate::unsupported(
                    "PREPARE TRANSACTION after pgrcolumnar2 DDL or ingest",
                ));
            }
        }
        XACT_EVENT_PREPARE => {
            purge_writers();
        }
        XACT_EVENT_PRE_COMMIT | XACT_EVENT_PARALLEL_PRE_COMMIT => {}
    }
    Ok(())
}

fn pgrc2_subxact_callback(
    event: SubXactEvent,
    _my_subid: SubTransactionId,
    _parent_subid: SubTransactionId,
    _arg: datum::Datum,
) -> PgResult<()> {
    use SubXactEvent::*;
    match event {
        SUBXACT_EVENT_COMMIT_SUB => {
            let nest_level = cur_nest_level();
            PENDING_DIRS.with(|p| reparent_pending(&mut p.borrow_mut(), nest_level));
        }
        SUBXACT_EVENT_ABORT_SUB => {
            do_pending_dir_deletes(cur_nest_level(), false);
        }
        SUBXACT_EVENT_START_SUB | SUBXACT_EVENT_PRE_COMMIT_SUB => {}
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn pending_snapshot() -> Vec<PendingDirDelete> {
    PENDING_DIRS.with(|p| p.borrow().clone())
}
