//! Transaction-fact capabilities: the reader's [`CommitCheck`] (manifest
//! clog fence + snapshot visibility) and the writer's [`TxnProbe`]
//! (publish/recovery verdicts). Both are PASSED capabilities (spec §13.1);
//! this module is the ONE place the pgrc2 crates meet the transaction
//! layer.
//!
//! ## The visibility law (old-AM `rg_visible`, carried verbatim at
//! manifest-generation grain)
//!
//! A generation is visible iff its publisher xid `is_current`, OR
//! (`!XidInMVCCSnapshot` AND `did_commit`). Own-transaction publishes are
//! visible to later statements of the same transaction (COPY then SELECT);
//! concurrent uncommitted or snapshot-future publishers are not. The
//! epoch-qualified fxid in the manifest exists so a recycled xid can never
//! resurrect an aborted publish (#254); at this layer the xid-domain probes
//! honor that qualification by mapping the recorded fxid through the
//! recent-past window ([`recent_past_xid`]) before consulting the 32-bit
//! clog/snapshot layer — a fxid from an older epoch (its clog slot recycled
//! across wraparound) never adopts the recycled transaction's status
//! (`resolve_effective` also refuses structurally-broken chains regardless).
//!
//! ## Error discipline
//!
//! `CommitCheck::committed` returns `bool` (a frozen M3-F face), but the
//! clog/snapshot probes can error. Errors are RECORDED in the check and
//! must be re-raised by the caller after resolution ([`take_error`]); the
//! answer under a recorded error is the CONSERVATIVE arm (not visible /
//! in-progress) so a swallowed error can hide rows but never resurrect
//! aborted ones — and it is never actually swallowed because every caller
//! re-raises.

use std::cell::RefCell;

use pgrc2_read::CommitCheck;
use pgrc2_write::publish::{TxnProbe, TxnVerdict};
use types_core::{FullTransactionId, TransactionId, TransactionIdIsNormal};
use types_error::PgError;
use types_snapshot::SnapshotData;

/// The pure visibility decision (unit-tested; the law above).
pub fn visible(is_current: bool, in_snapshot: bool, did_commit: bool) -> bool {
    is_current || (!in_snapshot && did_commit)
}

/// Map the recorded publisher [`FullTransactionId`] onto the bare 32-bit xid
/// the clog/snapshot layer probes — but ONLY when that xid still names *this*
/// transaction. A manifest carries an epoch-qualified 64-bit fxid precisely so
/// a recycled xid cannot resurrect an aborted publish (#254); the raw
/// `fxid as u32` truncation that this replaces threw the epoch away, so after
/// xid wraparound the 32-bit probes adjudicated whatever transaction now owns
/// the recycled clog slot.
///
/// C keeps clog authoritative only inside the recent-past window: a bare xid is
/// resolved to a full xid via `FullTransactionIdFromAllowableAt(nextFullXid,
/// xid)` (access/transam.h) — the unique full xid `<= nextFullXid` whose low 32
/// bits are `xid`, from the current epoch or the one before. If the recorded
/// fxid equals that reconstruction, the 32-bit clog/snapshot answer is about
/// the publisher and truncation is exact. If it does not, the recorded fxid
/// predates the window: its clog slot has been recycled to a newer transaction,
/// so the probe MUST NOT consult clog and must fall to the conservative arm.
///
/// (In a correctly-freezing cluster an effective generation's publisher is
/// always inside the window; a mismatch means aborted residue outlived its clog
/// — a corruption tripwire, surfaced as an error, never a recycled verdict.)
enum RecentPast {
    /// The recorded fxid is inside the recent-past window; probe this xid.
    Live(TransactionId),
    /// The recorded fxid predates the window (older epoch); its clog slot is
    /// recycled. Callers take the conservative arm and record this error.
    Aliased(Box<PgError>),
}

fn recent_past_xid(fxid: u64) -> RecentPast {
    let xid = fxid as TransactionId;
    // Special/bootstrap xids are epoch-independent (BootstrapXid, FrozenXid)
    // and are never recycled — probe them directly, matching
    // FullTransactionIdFromAllowableAt's non-normal fast path.
    if !TransactionIdIsNormal(xid) {
        return RecentPast::Live(xid);
    }
    let next_full = match varsup_seams::read_next_full_transaction_id::call() {
        Ok(v) => v,
        Err(e) => return RecentPast::Aliased(e),
    };
    if full_xid_from_allowable_at(next_full, xid).to_u64() == fxid {
        RecentPast::Live(xid)
    } else {
        RecentPast::Aliased(Box::new(
            PgError::error(format!(
                "pgrcolumnar2: publisher fxid {fxid} predates the clog-addressable \
                 recent-past window (next full xid {}); its clog slot has been \
                 recycled and cannot adjudicate this generation",
                next_full.to_u64()
            ))
            .with_sqlstate(types_error::ERRCODE_DATA_CORRUPTED),
        ))
    }
}

/// `FullTransactionIdFromAllowableAt` (access/transam.h): the unique full xid
/// `<= next_full` whose low 32 bits are `xid`, from `next_full`'s epoch or the
/// one before. Non-normal xids are epoch-0. Caller guarantees normality here.
fn full_xid_from_allowable_at(next_full: FullTransactionId, xid: TransactionId) -> FullTransactionId {
    if !TransactionIdIsNormal(xid) {
        return FullTransactionId::from_epoch_and_xid(0, xid);
    }
    let mut epoch = next_full.epoch();
    if xid > next_full.xid() {
        // xid can't be from next_full's (not-yet-issued) epoch — it's the prior.
        epoch = epoch.saturating_sub(1);
    }
    FullTransactionId::from_epoch_and_xid(epoch, xid)
}

/// Snapshot-scoped commit check for scans.
pub struct SnapshotCommitCheck<'a> {
    snapshot: Option<&'a SnapshotData<'a>>,
    error: RefCell<Option<Box<PgError>>>,
}

impl<'a> SnapshotCommitCheck<'a> {
    pub fn new(snapshot: Option<&'a SnapshotData<'a>>) -> SnapshotCommitCheck<'a> {
        SnapshotCommitCheck {
            snapshot,
            error: RefCell::new(None),
        }
    }

    /// Re-raise a probe error recorded during resolution (callers MUST call
    /// this after `resolve_effective`).
    pub fn take_error(&self) -> Result<(), Box<PgError>> {
        match self.error.borrow_mut().take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn record(&self, e: Box<PgError>) {
        let mut slot = self.error.borrow_mut();
        if slot.is_none() {
            *slot = Some(e);
        }
    }

    /// Inherent face of [`CommitCheck::committed`] for callers outside
    /// the pgrc2_read trait's dependency reach (the sqe engine-registry
    /// currency probe). Same visibility law, same recorded-error
    /// discipline — call [`Self::take_error`] after.
    pub fn committed_fxid(&self, fxid: u64) -> bool {
        CommitCheck::committed(self, fxid)
    }
}

impl CommitCheck for SnapshotCommitCheck<'_> {
    fn committed(&self, fxid: u64) -> bool {
        let xid = match recent_past_xid(fxid) {
            RecentPast::Live(xid) => xid,
            // Recycled clog slot: conservative arm is "not visible" (a
            // swallowed error can hide rows but never resurrect aborted ones).
            RecentPast::Aliased(e) => {
                self.record(e);
                return false;
            }
        };
        if xact_seams::transaction_id_is_current_transaction_id::call(xid) {
            return true;
        }
        if let Some(snap) = self.snapshot {
            match snapmgr::XidInMVCCSnapshot(xid, snap) {
                Ok(true) => return false,
                Ok(false) => {}
                Err(e) => {
                    self.record(e);
                    return false;
                }
            }
        }
        match transam_seams::transaction_id_did_commit::call(xid) {
            Ok(v) => v,
            Err(e) => {
                self.record(e);
                false
            }
        }
    }
}

/// Clog probe for publish + recovery (no snapshot: publish effectiveness
/// and crash reclaim are commit facts, not snapshot facts).
pub struct ClogTxnProbe {
    error: RefCell<Option<Box<PgError>>>,
}

impl ClogTxnProbe {
    pub fn new() -> ClogTxnProbe {
        ClogTxnProbe {
            error: RefCell::new(None),
        }
    }

    pub fn take_error(&self) -> Result<(), Box<PgError>> {
        match self.error.borrow_mut().take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn record(&self, e: Box<PgError>) {
        let mut slot = self.error.borrow_mut();
        if slot.is_none() {
            *slot = Some(e);
        }
    }
}

impl Default for ClogTxnProbe {
    fn default() -> Self {
        ClogTxnProbe::new()
    }
}

impl TxnProbe for ClogTxnProbe {
    fn verdict(&self, fxid: u64) -> TxnVerdict {
        let xid = match recent_past_xid(fxid) {
            RecentPast::Live(xid) => xid,
            // Recycled clog slot: conservative arm is InProgress (keeps temps
            // — dead-band, never data loss). Recording the error makes
            // recover_and_clean re-raise instead of either resurrecting the
            // aborted generation as Committed or reclaiming an old-committed one.
            RecentPast::Aliased(e) => {
                self.record(e);
                return TxnVerdict::InProgress;
            }
        };
        // Own transaction: in progress (recovery must never reclaim the
        // current transaction's temps; publish walks past its own gens via
        // the CommitCheck arm, not this one).
        if xact_seams::transaction_id_is_current_transaction_id::call(xid) {
            return TxnVerdict::InProgress;
        }
        match transam_seams::transaction_id_did_commit::call(xid) {
            Ok(true) => return TxnVerdict::Committed,
            Ok(false) => {}
            Err(e) => {
                self.record(e);
                return TxnVerdict::InProgress;
            }
        }
        match transam_seams::transaction_id_did_abort::call(xid) {
            Ok(true) => TxnVerdict::Aborted,
            // Crashed xids carry no clog record (did_commit and did_abort
            // both false). The C discipline: not committed AND not in
            // progress ⇒ dead. Consult procarray; on error stay
            // conservative (InProgress keeps temps — dead-band, never data
            // loss).
            Ok(false) => match procarray_seams::transaction_id_is_in_progress::call(xid) {
                Ok(true) => TxnVerdict::InProgress,
                Ok(false) => TxnVerdict::Aborted,
                Err(e) => {
                    self.record(e);
                    TxnVerdict::InProgress
                }
            },
            Err(e) => {
                self.record(e);
                TxnVerdict::InProgress
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, Once};

    // Seams are process-global install-once fn pointers, so the transaction
    // layer is scripted once and each test drives it through these statics.
    // NEXT_FULL is the cluster's next full xid; the recycled slot COMMITTED_XID
    // reads committed. TEST_LOCK serialises the shared-state tests.
    static NEXT_FULL: AtomicU64 = AtomicU64::new(0);
    static COMMITTED_XID: AtomicU64 = AtomicU64::new(0);
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn install_seams() {
        static SETUP: Once = Once::new();
        SETUP.call_once(|| {
            varsup_seams::read_next_full_transaction_id::set(|| {
                Ok(FullTransactionId::from_u64(NEXT_FULL.load(Ordering::Relaxed)))
            });
            xact_seams::transaction_id_is_current_transaction_id::set(|_| false);
            transam_seams::transaction_id_did_commit::set(|xid| {
                Ok(xid as u64 == COMMITTED_XID.load(Ordering::Relaxed))
            });
            transam_seams::transaction_id_did_abort::set(|_| Ok(false));
            procarray_seams::transaction_id_is_in_progress::set(|_| Ok(false));
        });
    }

    // full_xid_from_allowable_at mirrors access/transam.h exactly (pure; no
    // seams, so it needs no lock).
    #[test]
    fn allowable_at_current_and_prior_epoch() {
        let next = FullTransactionId::from_epoch_and_xid(3, 100);
        // xid <= next.xid() ⇒ current epoch.
        assert_eq!(full_xid_from_allowable_at(next, 50).to_u64(), (3u64 << 32) | 50);
        // xid > next.xid() ⇒ it must be from the prior epoch.
        assert_eq!(full_xid_from_allowable_at(next, 200).to_u64(), (2u64 << 32) | 200);
        // Non-normal xids are epoch-0.
        assert_eq!(full_xid_from_allowable_at(next, 2).to_u64(), 2);
    }

    // The security property: an aborted publisher from an OLDER epoch whose
    // 32-bit xid has been recycled to a now-COMMITTED transaction must not be
    // adjudicated Committed. The old `fxid as u32` truncation returned
    // Committed here (resurrection); the recent-past guard returns InProgress
    // and records a corruption error instead.
    #[test]
    fn verdict_does_not_resurrect_recycled_aborted_publisher() {
        let _g = TEST_LOCK.lock().unwrap();
        install_seams();
        NEXT_FULL.store((2u64 << 32) | 100, Ordering::Relaxed);
        COMMITTED_XID.store(50, Ordering::Relaxed); // recycled slot commits

        let probe = ClogTxnProbe::new();
        // Publisher recorded in epoch 1 (aborted), xid 50 — now recycled.
        let aborted_old = (1u64 << 32) | 50;
        assert_eq!(probe.verdict(aborted_old), TxnVerdict::InProgress);
        assert!(probe.take_error().is_err(), "aliased fxid must surface an error");
    }

    // A recent-epoch publisher is probed exactly as before (no regression):
    // its fxid maps back to itself through the recent-past window.
    #[test]
    fn verdict_probes_current_epoch_publisher_normally() {
        let _g = TEST_LOCK.lock().unwrap();
        install_seams();
        NEXT_FULL.store((2u64 << 32) | 100, Ordering::Relaxed);
        COMMITTED_XID.store(50, Ordering::Relaxed);

        let probe = ClogTxnProbe::new();
        let committed_recent = (2u64 << 32) | 50;
        assert_eq!(probe.verdict(committed_recent), TxnVerdict::Committed);
        assert!(probe.take_error().is_ok());
    }

    // CommitCheck (reader path): a recycled old-epoch aborted publisher must
    // not be reported visible, and the error is recorded for re-raise.
    #[test]
    fn committed_does_not_resurrect_recycled_aborted_publisher() {
        let _g = TEST_LOCK.lock().unwrap();
        install_seams();
        NEXT_FULL.store((2u64 << 32) | 100, Ordering::Relaxed);
        COMMITTED_XID.store(50, Ordering::Relaxed);

        let check = SnapshotCommitCheck::new(None);
        let aborted_old = (1u64 << 32) | 50;
        assert!(!check.committed(aborted_old));
        assert!(check.take_error().is_err());
    }
}
