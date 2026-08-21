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
//! mirror the old AM exactly (single xid epoch per running cluster;
//! `resolve_effective` refuses structurally-broken chains regardless).
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
use types_error::PgError;
use types_snapshot::SnapshotData;

/// The pure visibility decision (unit-tested; the law above).
pub fn visible(is_current: bool, in_snapshot: bool, did_commit: bool) -> bool {
    is_current || (!in_snapshot && did_commit)
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
        let xid = fxid as u32;
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
        let xid = fxid as u32;
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
