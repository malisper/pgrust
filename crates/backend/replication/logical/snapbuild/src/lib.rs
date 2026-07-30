#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

pub mod ondisk;
#[cfg(test)]
mod tests;

use std::cell::Cell;

use elog::{elog, ereport, errno};
use mcx::{Mcx, MemoryContext, PgVec};
use reorderbuffer::ReorderBuffer;
use snapmgr::Snapshot;
use types_core::{
    CommandId, FirstNormalTransactionId, InvalidCommandId, InvalidTransactionId, InvalidXLogRecPtr,
    TransactionId, TransactionIdFollows, TransactionIdFollowsOrEquals, TransactionIdIsNormal,
    TransactionIdIsValid, TransactionIdPrecedes, TransactionIdPrecedesOrEquals, XLogRecPtr,
};
use xact::XACT_XINFO_HAS_INVALS;
use types_error::{ErrorLocation, PgResult, DEBUG1, DEBUG2, DEBUG3, ERROR, LOG};
use types_snapshot::{SnapshotData, SNAPSHOT_HISTORIC_MVCC};

pub use ondisk::{
    SnapBuildOnDisk, PG_LOGICAL_SNAPSHOTS_DIR, SNAPBUILD_MAGIC, SNAPBUILD_VERSION,
};

#[cold]
#[inline(never)]
pub(crate) fn unported(what: &str) -> ! {
    panic!("unported callee reached from snapbuild.c: {what}")
}

#[track_caller]
pub(crate) fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

thread_local! {
    static SB_CTX: &'static MemoryContext =
        ::mcx::session_root("snapshot builder context");
    static EXPORT_IN_PROGRESS: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn sb_mcx() -> Mcx<'static> {
    SB_CTX.with(|c| c.mcx())
}

// LogicalIncreaseXminForSlot / LogicalIncreaseRestartDecodingForSlot delegate
// slots; the declarations belong in logical_seams once logical.c lands (that
// crate is owned by a parallel lane), installed via that crate's init_seams.
pub mod logical_hooks {
    use types_core::{TransactionId, XLogRecPtr};
    use types_error::PgResult;

    seam_core::seam!(
        pub fn logical_increase_xmin_for_slot(
            current_lsn: XLogRecPtr,
            xmin: TransactionId,
        ) -> PgResult<()>
    );

    seam_core::seam!(
        pub fn logical_increase_restart_decoding_for_slot(
            current_lsn: XLogRecPtr,
            restart_lsn: XLogRecPtr,
        ) -> PgResult<()>
    );
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i32)]
pub enum SnapBuildState {
    Start = -1,
    Building = 0,
    FullSnapshot = 1,
    Consistent = 2,
}

pub use SnapBuildState::*;

impl SnapBuildState {
    pub fn from_i32(v: i32) -> Option<SnapBuildState> {
        match v {
            -1 => Some(Start),
            0 => Some(Building),
            1 => Some(FullSnapshot),
            2 => Some(Consistent),
            _ => None,
        }
    }
}

// xl_running_xacts (standbydefs.h); xids carries xcnt running xids followed by
// subxcnt subxids, but only the xcnt prefix participates in waits, as in C.
pub struct XlRunningXacts<'a> {
    pub xcnt: u32,
    pub subxcnt: u32,
    pub subxid_overflow: bool,
    pub next_xid: TransactionId,
    pub oldest_running_xid: TransactionId,
    pub latest_completed_xid: TransactionId,
    pub xids: &'a [TransactionId],
}

pub struct SnapBuild {
    pub(crate) state: SnapBuildState,
    pub(crate) xmin: TransactionId,
    pub(crate) xmax: TransactionId,
    pub(crate) start_decoding_at: XLogRecPtr,
    pub(crate) two_phase_at: XLogRecPtr,
    pub(crate) initial_xmin_horizon: TransactionId,
    pub(crate) building_full_snapshot: bool,
    pub(crate) in_slot_creation: bool,
    pub(crate) snapshot: Option<Snapshot>,
    pub(crate) last_serialized_snapshot: XLogRecPtr,
    pub(crate) next_phase_at: TransactionId,
    pub(crate) committed_xcnt_space: usize,
    pub(crate) committed_includes_all_transactions: bool,
    pub(crate) committed_xip: PgVec<'static, TransactionId>,
    pub(crate) catchange_xip: PgVec<'static, TransactionId>,
}

#[inline]
fn NormalTransactionIdFollows(id1: TransactionId, id2: TransactionId) -> bool {
    debug_assert!(TransactionIdIsNormal(id1) && TransactionIdIsNormal(id2));
    (id1.wrapping_sub(id2) as i32) > 0
}

#[inline]
fn NormalTransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    debug_assert!(TransactionIdIsNormal(id1) && TransactionIdIsNormal(id2));
    (id1.wrapping_sub(id2) as i32) < 0
}

#[inline]
fn TransactionIdAdvance(xid: &mut TransactionId) {
    *xid = xid.wrapping_add(1);
    if *xid < FirstNormalTransactionId {
        *xid = FirstNormalTransactionId;
    }
}

fn lsn_hi(lsn: XLogRecPtr) -> u32 {
    (lsn >> 32) as u32
}

fn lsn_lo(lsn: XLogRecPtr) -> u32 {
    lsn as u32
}

pub fn allocate_snapshot_builder(
    xmin_horizon: TransactionId,
    start_lsn: XLogRecPtr,
    need_full_snapshot: bool,
    in_slot_creation: bool,
    two_phase_at: XLogRecPtr,
) -> Box<SnapBuild> {
    let mcx = sb_mcx();
    let mut committed_xip = PgVec::new_in(mcx);
    committed_xip.reserve(128);
    Box::new(SnapBuild {
        state: Start,
        xmin: InvalidTransactionId,
        xmax: InvalidTransactionId,
        start_decoding_at: start_lsn,
        two_phase_at,
        initial_xmin_horizon: xmin_horizon,
        building_full_snapshot: need_full_snapshot,
        in_slot_creation,
        snapshot: None,
        last_serialized_snapshot: InvalidXLogRecPtr,
        next_phase_at: InvalidTransactionId,
        committed_xcnt_space: 128,
        committed_includes_all_transactions: true,
        committed_xip,
        catchange_xip: PgVec::new_in(mcx),
    })
}

pub fn free_snapshot_builder(builder: Box<SnapBuild>) {
    drop(builder);
}

pub fn snap_build_snap_dec_refcount(snap: Snapshot) {
    debug_assert_eq!(snap.snapshot_type, SNAPSHOT_HISTORIC_MVCC);
    debug_assert_eq!(snap.curcid.get(), types_core::FirstCommandId);
    debug_assert!(!snap.suboverflowed);
    debug_assert!(!snap.takenDuringRecovery);
    drop(snap);
}

impl SnapBuild {
    pub fn current_state(&self) -> SnapBuildState {
        self.state
    }

    pub fn get_two_phase_at(&self) -> XLogRecPtr {
        self.two_phase_at
    }

    pub fn set_two_phase_at(&mut self, ptr: XLogRecPtr) {
        self.two_phase_at = ptr;
    }

    pub fn xact_needs_skip(&self, ptr: XLogRecPtr) -> bool {
        ptr < self.start_decoding_at
    }

    pub fn start_decoding_at(&self) -> XLogRecPtr {
        self.start_decoding_at
    }

    fn build_snapshot(&self) -> Snapshot {
        debug_assert!(self.state >= FullSnapshot);
        debug_assert!(TransactionIdIsNormal(self.xmin));
        debug_assert!(TransactionIdIsNormal(self.xmax));

        let mcx = sb_mcx();
        let mut snapshot = SnapshotData::sentinel(mcx, SNAPSHOT_HISTORIC_MVCC);
        snapshot.xmin = self.xmin;
        snapshot.xmax = self.xmax;
        let mut xip = PgVec::new_in(mcx);
        xip.extend_from_slice(&self.committed_xip);
        // xidComparator order so visibility can bsearch.
        xip.sort_unstable();
        snapshot.xcnt = xip.len() as u32;
        snapshot.xip = xip;
        std::rc::Rc::new(snapshot)
    }

    // SnapBuildInitialSnapshot (snapbuild.c): convert the inverted historic
    // snapshot (xip = committed) into a classical MVCC snapshot (xip = in
    // progress) usable by the surrounding REPEATABLE READ transaction. The
    // command-level validations (txn block, isolation, read-only, first
    // snapshot) happen in the walsender before this is reached.
    pub fn initial_snapshot(&mut self) -> PgResult<snapmgr::SerializedSnapshot> {
        debug_assert!(xact::IsolationUsesXactSnapshot());
        debug_assert!(self.building_full_snapshot);

        // Don't allow older snapshots: about to overwrite MyProc->xmin.
        snapmgr::InvalidateCatalogSnapshot();
        if snapmgr::HaveRegisteredOrActiveSnapshot() {
            elog(ERROR, "cannot build an initial slot snapshot when snapshots exist")?;
            unreachable!();
        }

        if self.state != Consistent {
            elog(
                ERROR,
                "cannot build an initial slot snapshot before reaching a consistent state",
            )?;
            unreachable!();
        }
        if !self.committed_includes_all_transactions {
            elog(
                ERROR,
                "cannot build an initial slot snapshot, not all transactions are monitored anymore",
            )?;
            unreachable!();
        }
        if TransactionIdIsValid(procarray::ProcArrayOwnXmin()) {
            elog(
                ERROR,
                "cannot build an initial slot snapshot when MyProc->xmin already is valid",
            )?;
            unreachable!();
        }

        let snap = self.build_snapshot();

        // snap->xmin is alive (logical xmin mechanism), but always double-check
        // that the horizon is enforced before adopting it.
        let safe_xid = procarray::GetOldestSafeDecodingTransactionId(false)?;
        if TransactionIdFollows(safe_xid, snap.xmin) {
            elog(
                ERROR,
                format!(
                    "cannot build an initial slot snapshot as oldest safe xid {safe_xid} follows snapshot's xmin {}",
                    snap.xmin
                ),
            )?;
            unreachable!();
        }
        procarray::ProcArraySetOwnXmin(snap.xmin);

        // Invert: mark every non-committed xid in [xmin, xmax) as in-progress.
        let max_xids = procarray::GetMaxSnapshotXidCount();
        let committed = &snap.xip[..snap.xcnt as usize];
        let mut newxip: Vec<TransactionId> = Vec::new();
        let mut xid = snap.xmin;
        while NormalTransactionIdPrecedes(xid, snap.xmax) {
            if committed.binary_search(&xid).is_err() {
                if newxip.len() >= max_xids {
                    return Err(ereport(ERROR)
                        .errcode(types_error::ERRCODE_T_R_SERIALIZATION_FAILURE)
                        .errmsg("initial slot snapshot too large")
                        .into_error()
                        .with_error_location(loc("SnapBuildInitialSnapshot"))
                        .into());
                }
                newxip.push(xid);
            }
            TransactionIdAdvance(&mut xid);
        }

        Ok(snapmgr::SerializedSnapshot {
            xmin: snap.xmin,
            xmax: snap.xmax,
            xip: newxip,
            subxip: Vec::new(),
            suboverflowed: false,
            takenDuringRecovery: false,
            curcid: types_core::FirstCommandId,
            vistest: types_core::GlobalVisStateHandle::new(0),
        })
    }

    // SnapBuildExportSnapshot (snapbuild.c:539): export the initial slot
    // snapshot through the regular SET TRANSACTION SNAPSHOT machinery. The
    // transaction started here stays open until the walsender's next
    // replication command (snap_build_clear_exported_snapshot) so the
    // importing side sees the source transaction still running and the xmin
    // horizon held.
    pub fn export_snapshot(&mut self) -> PgResult<String> {
        if xact::IsTransactionOrTransactionBlock() {
            elog(ERROR, "cannot export a snapshot from within a transaction")?;
        }
        if EXPORT_IN_PROGRESS.get() {
            elog(ERROR, "can only export one snapshot at a time")?;
        }
        // C additionally saves CurrentResourceOwner across the export
        // transaction; the port's transaction machinery owns that save and
        // restore internally.
        EXPORT_IN_PROGRESS.set(true);

        xact::StartTransactionCommand()?;
        // There doesn't seem to be a nicer API to set these (snapbuild.c:555).
        xact::SetXactIsoLevel(types_core::xact::XACT_REPEATABLE_READ);
        xact::SetXactReadOnly(true);

        let serialized = self.initial_snapshot()?;
        let xcnt = serialized.xip.len();
        let snap = snapmgr::RestoreSnapshot(&serialized);

        // Now that we've built a plain snapshot, export it the normal way.
        let snapname = snapmgr::ExportSnapshot(&snap)?;

        ereport(types_error::LOG)
            .errmsg(format!(
                "exported logical decoding snapshot: \"{snapname}\" with {xcnt} transaction ID{}",
                if xcnt == 1 { "" } else { "s" }
            ))
            .finish(loc("SnapBuildExportSnapshot"))?;
        Ok(snapname)
    }

    pub fn get_or_build_snapshot(&mut self) -> Snapshot {
        debug_assert_eq!(self.state, Consistent);
        if self.snapshot.is_none() {
            self.snapshot = Some(self.build_snapshot());
        }
        self.snapshot.clone().expect("snapshot present")
    }

    pub fn process_change(
        &mut self,
        rb: &mut ReorderBuffer,
        xid: TransactionId,
        lsn: XLogRecPtr,
    ) -> bool {
        if self.state < FullSnapshot {
            return false;
        }

        if self.state < Consistent && TransactionIdPrecedes(xid, self.next_phase_at) {
            return false;
        }

        if !rb.xid_has_base_snapshot(xid) {
            if self.snapshot.is_none() {
                self.snapshot = Some(self.build_snapshot());
            }
            let snap = self.snapshot.clone().expect("snapshot present");
            rb.set_base_snapshot(xid, lsn, snap);
        }

        true
    }

    pub fn process_new_cid(
        &mut self,
        rb: &mut ReorderBuffer,
        xid: TransactionId,
        lsn: XLogRecPtr,
        xlrec: &heapam_xlog::XlHeapNewCid,
    ) -> PgResult<()> {
        rb.xid_set_catalog_changes(xid, lsn);

        rb.add_new_tuple_cids(
            xlrec.top_xid,
            lsn,
            xlrec.target_locator,
            xlrec.target_tid,
            xlrec.cmin,
            xlrec.cmax,
            xlrec.combocid,
        );

        let cid: CommandId;
        if xlrec.cmin != InvalidCommandId && xlrec.cmax != InvalidCommandId {
            cid = xlrec.cmin.max(xlrec.cmax);
        } else if xlrec.cmax != InvalidCommandId {
            cid = xlrec.cmax;
        } else if xlrec.cmin != InvalidCommandId {
            cid = xlrec.cmin;
        } else {
            elog(
                ERROR,
                "xl_heap_new_cid record without a valid CommandId".to_string(),
            )?;
            unreachable!();
        }

        rb.add_new_command_id(xid, lsn, cid + 1)
    }

    fn distribute_snapshot_and_inval(
        &mut self,
        rb: &mut ReorderBuffer,
        lsn: XLogRecPtr,
        xid: TransactionId,
    ) -> PgResult<()> {
        // C fetches the committing xid's invalidations per iteration; the list
        // is loop-invariant, so it is hoisted (and copied out of rb's borrow).
        let msgs = rb.get_invalidations(xid).to_vec();
        let txns: Vec<reorderbuffer::TxnId> = rb.toplevel_txns().collect();

        for id in txns {
            let txn_xid = rb.txn(id).xid;
            debug_assert!(TransactionIdIsValid(txn_xid));

            if !rb.xid_has_base_snapshot(txn_xid) {
                continue;
            }

            if rb.txn(id).is_prepared() {
                continue;
            }

            elog(
                DEBUG2,
                format!(
                    "adding a new snapshot and invalidations to {} at {:X}/{:X}",
                    txn_xid,
                    lsn_hi(lsn),
                    lsn_lo(lsn)
                ),
            )?;

            let snap = self.snapshot.clone().expect("distributing a built snapshot");
            rb.add_snapshot(txn_xid, lsn, snap)?;

            if txn_xid != xid && !msgs.is_empty() {
                rb.add_distributed_invalidations(txn_xid, lsn, &msgs)?;
            }
        }
        Ok(())
    }

    fn add_committed_txn(&mut self, xid: TransactionId) -> PgResult<()> {
        debug_assert!(TransactionIdIsValid(xid));

        if self.committed_xip.len() == self.committed_xcnt_space {
            self.committed_xcnt_space = self.committed_xcnt_space * 2 + 1;
            elog(
                DEBUG1,
                format!(
                    "increasing space for committed transactions to {}",
                    self.committed_xcnt_space
                ),
            )?;
            let additional = self.committed_xcnt_space - self.committed_xip.len();
            self.committed_xip.reserve(additional);
        }

        self.committed_xip.push(xid);
        Ok(())
    }

    fn purge_older_txn(&mut self) -> PgResult<()> {
        if !TransactionIdIsNormal(self.xmin) {
            return Ok(());
        }

        let xmin = self.xmin;
        let before = self.committed_xip.len();
        self.committed_xip
            .retain(|&x| !NormalTransactionIdPrecedes(x, xmin));

        elog(
            DEBUG3,
            format!(
                "purged committed transactions from {} to {}, xmin: {}, xmax: {}",
                before,
                self.committed_xip.len(),
                self.xmin,
                self.xmax
            ),
        )?;

        if !self.catchange_xip.is_empty() {
            let before = self.catchange_xip.len();
            let off = self
                .catchange_xip
                .iter()
                .position(|&x| TransactionIdFollowsOrEquals(x, xmin))
                .unwrap_or(before);
            if off > 0 {
                self.catchange_xip.drain(..off);
            }
            elog(
                DEBUG3,
                format!(
                    "purged catalog modifying transactions from {} to {}, xmin: {}, xmax: {}",
                    before,
                    self.catchange_xip.len(),
                    self.xmin,
                    self.xmax
                ),
            )?;
        }
        Ok(())
    }

    fn xid_has_catalog_changes(
        &self,
        rb: &mut ReorderBuffer,
        xid: TransactionId,
        xinfo: u32,
    ) -> bool {
        if rb.xid_has_catalog_changes(xid) {
            return true;
        }

        if (xinfo & XACT_XINFO_HAS_INVALS) == 0 {
            return false;
        }

        !self.catchange_xip.is_empty() && self.catchange_xip.binary_search(&xid).is_ok()
    }

    pub fn commit_txn(
        &mut self,
        rb: &mut ReorderBuffer,
        lsn: XLogRecPtr,
        xid: TransactionId,
        subxacts: &[TransactionId],
        xinfo: u32,
    ) -> PgResult<()> {
        let mut needs_snapshot = false;
        let mut needs_timetravel = false;
        let mut sub_needs_timetravel = false;

        let mut xmax = xid;

        if self.state == Start
            || (self.state == Building && TransactionIdPrecedes(xid, self.next_phase_at))
        {
            if self.start_decoding_at <= lsn {
                self.start_decoding_at = lsn + 1;
            }
            return Ok(());
        }

        if self.state < Consistent {
            if self.start_decoding_at <= lsn {
                self.start_decoding_at = lsn + 1;
            }

            if self.building_full_snapshot {
                needs_timetravel = true;
            }
        }

        for &subxid in subxacts {
            if self.xid_has_catalog_changes(rb, subxid, xinfo) {
                sub_needs_timetravel = true;
                needs_snapshot = true;

                elog(
                    DEBUG1,
                    format!("found subtransaction {xid}:{subxid} with catalog changes"),
                )?;

                self.add_committed_txn(subxid)?;

                if NormalTransactionIdFollows(subxid, xmax) {
                    xmax = subxid;
                }
            } else if needs_timetravel {
                self.add_committed_txn(subxid)?;
                if NormalTransactionIdFollows(subxid, xmax) {
                    xmax = subxid;
                }
            }
        }

        if self.xid_has_catalog_changes(rb, xid, xinfo) {
            elog(
                DEBUG2,
                format!("found top level transaction {xid}, with catalog changes"),
            )?;
            needs_snapshot = true;
            needs_timetravel = true;
            self.add_committed_txn(xid)?;
        } else if sub_needs_timetravel {
            elog(
                DEBUG2,
                format!(
                    "forced transaction {xid} to do timetravel due to one of its subtransactions"
                ),
            )?;
            needs_timetravel = true;
            self.add_committed_txn(xid)?;
        } else if needs_timetravel {
            elog(DEBUG2, format!("forced transaction {xid} to do timetravel"))?;
            self.add_committed_txn(xid)?;
        }

        if !needs_timetravel {
            self.committed_includes_all_transactions = false;
        }

        debug_assert!(!needs_snapshot || needs_timetravel);

        if needs_timetravel
            && (!TransactionIdIsValid(self.xmax) || TransactionIdFollowsOrEquals(xmax, self.xmax))
        {
            self.xmax = xmax;
            TransactionIdAdvance(&mut self.xmax);
        }

        if needs_snapshot {
            if self.state < FullSnapshot {
                return Ok(());
            }

            self.snapshot = Some(self.build_snapshot());

            if !rb.xid_has_base_snapshot(xid) {
                let snap = self.snapshot.clone().expect("snapshot present");
                rb.set_base_snapshot(xid, lsn, snap);
            }

            self.distribute_snapshot_and_inval(rb, lsn, xid)?;
        }
        Ok(())
    }

    pub fn process_running_xacts(
        &mut self,
        rb: &mut ReorderBuffer,
        lsn: XLogRecPtr,
        running: &XlRunningXacts<'_>,
    ) -> PgResult<()> {
        if self.state < Consistent {
            if !self.find_snapshot(rb, lsn, running)? {
                return Ok(());
            }
        } else {
            self.serialize(rb, lsn)?;
        }

        self.xmin = running.oldest_running_xid;

        self.purge_older_txn()?;

        let mut xmin = rb.get_oldest_xmin();
        if xmin == InvalidTransactionId {
            xmin = running.oldest_running_xid;
        }
        elog(
            DEBUG3,
            format!(
                "xmin: {}, xmax: {}, oldest running: {}, oldest xmin: {}",
                self.xmin, self.xmax, running.oldest_running_xid, xmin
            ),
        )?;
        logical_hooks::logical_increase_xmin_for_slot::call(lsn, xmin)?;

        if self.state < Consistent {
            return Ok(());
        }

        let txn = rb.get_oldest_txn();

        match txn {
            Some(id) if rb.txn(id).restart_decoding_lsn != InvalidXLogRecPtr => {
                logical_hooks::logical_increase_restart_decoding_for_slot::call(
                    lsn,
                    rb.txn(id).restart_decoding_lsn,
                )?;
            }
            None if rb.current_restart_decoding_lsn() != InvalidXLogRecPtr
                && self.last_serialized_snapshot != InvalidXLogRecPtr =>
            {
                logical_hooks::logical_increase_restart_decoding_for_slot::call(
                    lsn,
                    self.last_serialized_snapshot,
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    fn find_snapshot(
        &mut self,
        rb: &mut ReorderBuffer,
        lsn: XLogRecPtr,
        running: &XlRunningXacts<'_>,
    ) -> PgResult<bool> {
        if TransactionIdIsNormal(self.initial_xmin_horizon)
            && NormalTransactionIdPrecedes(running.oldest_running_xid, self.initial_xmin_horizon)
        {
            ereport(DEBUG1)
                .errmsg_internal(format!(
                    "skipping snapshot at {:X}/{:X} while building logical decoding snapshot, xmin horizon too low",
                    lsn_hi(lsn),
                    lsn_lo(lsn)
                ))
                .errdetail_internal(format!(
                    "initial xmin horizon of {} vs the snapshot's {}",
                    self.initial_xmin_horizon, running.oldest_running_xid
                ))
                .finish(loc("SnapBuildFindSnapshot"))?;

            Self::wait_snapshot(running, self.initial_xmin_horizon)?;

            return Ok(true);
        }

        if running.oldest_running_xid == running.next_xid {
            if self.start_decoding_at == InvalidXLogRecPtr || self.start_decoding_at <= lsn {
                self.start_decoding_at = lsn + 1;
            }

            self.xmin = running.next_xid;
            self.xmax = running.next_xid;

            debug_assert!(TransactionIdIsNormal(self.xmin));
            debug_assert!(TransactionIdIsNormal(self.xmax));

            self.state = Consistent;
            self.next_phase_at = InvalidTransactionId;

            ereport(LOG)
                .errmsg(format!(
                    "logical decoding found consistent point at {:X}/{:X}",
                    lsn_hi(lsn),
                    lsn_lo(lsn)
                ))
                .errdetail("There are no running transactions.")
                .finish(loc("SnapBuildFindSnapshot"))?;

            return Ok(false);
        } else if !self.building_full_snapshot
            && !self.in_slot_creation
            && self.restore(rb, lsn)?
        {
            return Ok(false);
        } else if self.state == Start {
            self.state = Building;
            self.next_phase_at = running.next_xid;

            self.xmin = running.next_xid;
            self.xmax = running.next_xid;

            debug_assert!(TransactionIdIsNormal(self.xmin));
            debug_assert!(TransactionIdIsNormal(self.xmax));

            ereport(LOG)
                .errmsg(format!(
                    "logical decoding found initial starting point at {:X}/{:X}",
                    lsn_hi(lsn),
                    lsn_lo(lsn)
                ))
                .errdetail(format!(
                    "Waiting for transactions (approximately {}) older than {} to end.",
                    running.xcnt, running.next_xid
                ))
                .finish(loc("SnapBuildFindSnapshot"))?;

            Self::wait_snapshot(running, running.next_xid)?;
        } else if self.state == Building
            && TransactionIdPrecedesOrEquals(self.next_phase_at, running.oldest_running_xid)
        {
            self.state = FullSnapshot;
            self.next_phase_at = running.next_xid;

            ereport(LOG)
                .errmsg(format!(
                    "logical decoding found initial consistent point at {:X}/{:X}",
                    lsn_hi(lsn),
                    lsn_lo(lsn)
                ))
                .errdetail(format!(
                    "Waiting for transactions (approximately {}) older than {} to end.",
                    running.xcnt, running.next_xid
                ))
                .finish(loc("SnapBuildFindSnapshot"))?;

            Self::wait_snapshot(running, running.next_xid)?;
        } else if self.state == FullSnapshot
            && TransactionIdPrecedesOrEquals(self.next_phase_at, running.oldest_running_xid)
        {
            self.state = Consistent;
            self.next_phase_at = InvalidTransactionId;

            ereport(LOG)
                .errmsg(format!(
                    "logical decoding found consistent point at {:X}/{:X}",
                    lsn_hi(lsn),
                    lsn_lo(lsn)
                ))
                .errdetail("There are no old transactions anymore.")
                .finish(loc("SnapBuildFindSnapshot"))?;
        }

        Ok(true)
    }

    fn wait_snapshot(running: &XlRunningXacts<'_>, cutoff: TransactionId) -> PgResult<()> {
        for &xid in &running.xids[..running.xcnt as usize] {
            if xact::TransactionIdIsCurrentTransactionId(xid) {
                elog(ERROR, "waiting for ourselves".to_string())?;
            }

            if TransactionIdFollows(xid, cutoff) {
                continue;
            }

            lmgr::XactLockTableWait(xid, None, None, types_storage::lock::XLTW_Oper::None)?;
        }

        // Unit tests have no WAL substrate to log a standby snapshot into.
        #[cfg(not(test))]
        if !transam_xlog::RecoveryInProgress() {
            standby::LogStandbySnapshot()?;
        }
        Ok(())
    }

    pub fn serialization_point(&mut self, rb: &mut ReorderBuffer, lsn: XLogRecPtr) -> PgResult<()> {
        if self.state < Consistent {
            self.restore(rb, lsn)?;
            Ok(())
        } else {
            self.serialize(rb, lsn)
        }
    }

    fn serialize(&mut self, rb: &mut ReorderBuffer, lsn: XLogRecPtr) -> PgResult<()> {
        debug_assert!(lsn != InvalidXLogRecPtr);
        debug_assert!(
            self.last_serialized_snapshot == InvalidXLogRecPtr
                || self.last_serialized_snapshot <= lsn
        );

        if self.state < Consistent {
            return Ok(());
        }

        debug_assert!(self.next_phase_at == InvalidTransactionId);

        let path = ondisk::snapshot_path(lsn);

        match std::fs::metadata(&path) {
            Ok(_) => {
                fd::fsync_fname(&path, false)?;
                fd::fsync_fname(PG_LOGICAL_SNAPSHOTS_DIR, true)?;

                self.last_serialized_snapshot = lsn;
                rb.set_restart_point(self.last_serialized_snapshot);
                return Ok(());
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return ereport(ERROR)
                    .with_saved_errno(e.raw_os_error().unwrap_or(0))
                    .errcode_for_file_access()
                    .errmsg(format!("could not stat file \"{path}\": %m"))
                    .finish(loc("SnapBuildSerialize"));
            }
        }

        elog(DEBUG1, format!("serializing snapshot to {path}"))?;

        let tmppath = format!(
            "{}/{:X}-{:X}.snap.{}.tmp",
            PG_LOGICAL_SNAPSHOTS_DIR,
            lsn_hi(lsn),
            lsn_lo(lsn),
            init_small::globals::MyProcPid()
        );

        if ondisk::c_unlink(&tmppath) != 0 && errno::current_errno() != libc::ENOENT {
            return ereport(ERROR)
                .with_saved_errno(errno::current_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not remove file \"{tmppath}\": %m"))
                .finish(loc("SnapBuildSerialize"));
        }

        let catchange_xip = rb.get_catalog_changes_xacts();
        let image = ondisk::build_image(self, &catchange_xip);

        let fd_ = fd::OpenTransientFile(&tmppath, libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY)?;
        if fd_ < 0 {
            return ereport(ERROR)
                .with_saved_errno(errno::current_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not open file \"{tmppath}\": %m"))
                .finish(loc("SnapBuildSerialize"));
        }

        // SAFETY: image is a live readable buffer of image.len() bytes.
        if unsafe { libc::write(fd_, image.as_ptr().cast(), image.len()) } != image.len() as isize {
            let save_errno = errno::current_errno();
            fd::CloseTransientFile(fd_);
            let save_errno = if save_errno != 0 { save_errno } else { libc::ENOSPC };
            return ereport(ERROR)
                .with_saved_errno(save_errno)
                .errcode_for_file_access()
                .errmsg(format!("could not write to file \"{tmppath}\": %m"))
                .finish(loc("SnapBuildSerialize"));
        }

        if fd::pg_fsync(fd_) != 0 {
            let save_errno = errno::current_errno();
            fd::CloseTransientFile(fd_);
            return ereport(ERROR)
                .with_saved_errno(save_errno)
                .errcode_for_file_access()
                .errmsg(format!("could not fsync file \"{tmppath}\": %m"))
                .finish(loc("SnapBuildSerialize"));
        }

        if fd::CloseTransientFile(fd_) != 0 {
            return ereport(ERROR)
                .with_saved_errno(errno::current_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not close file \"{tmppath}\": %m"))
                .finish(loc("SnapBuildSerialize"));
        }

        fd::fsync_fname(PG_LOGICAL_SNAPSHOTS_DIR, true)?;

        if ondisk::c_rename(&tmppath, &path) != 0 {
            return ereport(ERROR)
                .with_saved_errno(errno::current_errno())
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not rename file \"{tmppath}\" to \"{path}\": %m"
                ))
                .finish(loc("SnapBuildSerialize"));
        }

        fd::fsync_fname(&path, false)?;
        fd::fsync_fname(PG_LOGICAL_SNAPSHOTS_DIR, true)?;

        self.last_serialized_snapshot = lsn;

        rb.set_restart_point(self.last_serialized_snapshot);
        Ok(())
    }

    fn restore(&mut self, rb: &mut ReorderBuffer, lsn: XLogRecPtr) -> PgResult<bool> {
        if self.state == Consistent {
            return Ok(false);
        }

        let Some(ondisk) = ondisk::restore_snapshot(lsn, true)? else {
            return Ok(false);
        };

        if ondisk.state < Consistent as i32 {
            return Ok(false);
        }

        if TransactionIdPrecedes(ondisk.xmin, self.initial_xmin_horizon) {
            return Ok(false);
        }

        debug_assert!(ondisk.next_phase_at == InvalidTransactionId);
        self.next_phase_at = InvalidTransactionId;

        self.xmin = ondisk.xmin;
        self.xmax = ondisk.xmax;
        self.state = SnapBuildState::from_i32(ondisk.state).expect("state validated above");

        if !ondisk.committed.is_empty() {
            self.committed_xcnt_space = ondisk.committed.len();
            self.committed_xip.clear();
            self.committed_xip.extend_from_slice(&ondisk.committed);
        } else {
            self.committed_xip.clear();
        }

        self.catchange_xip.clear();
        self.catchange_xip.extend_from_slice(&ondisk.catchange);

        self.snapshot = Some(self.build_snapshot());

        rb.set_restart_point(lsn);

        debug_assert_eq!(self.state, Consistent);

        ereport(LOG)
            .errmsg(format!(
                "logical decoding found consistent point at {:X}/{:X}",
                lsn_hi(lsn),
                lsn_lo(lsn)
            ))
            .errdetail("Logical decoding will begin using saved snapshot.")
            .finish(loc("SnapBuildRestore"))?;
        Ok(true)
    }
}

// SnapBuildClearExportedSnapshot (snapbuild.c:600): abort the transaction
// that kept the exported snapshot's xmin pinned; runs at the start of every
// replication command.
pub fn snap_build_clear_exported_snapshot() -> PgResult<()> {
    if !EXPORT_IN_PROGRESS.get() {
        return Ok(());
    }
    if !xact::IsTransactionState() {
        elog(ERROR, "clearing exported snapshot in wrong transaction state")?;
    }
    // AbortCurrentTransaction takes care of resetting the snapshot state
    // (and, in this port, the resource owner C restores by hand).
    xact::AbortCurrentTransaction()?;
    EXPORT_IN_PROGRESS.set(false);
    Ok(())
}

pub fn snap_build_reset_exported_snapshot_state() {
    EXPORT_IN_PROGRESS.set(false);
}

pub fn check_point_snap_build() -> PgResult<()> {
    let redo = transam_xlog::GetRedoRecPtr();

    let mut cutoff = slot::ReplicationSlotsComputeLogicalRestartLSN()?;

    if redo < cutoff {
        cutoff = redo;
    }

    let entries = match std::fs::read_dir(PG_LOGICAL_SNAPSHOTS_DIR) {
        Ok(entries) => entries,
        Err(e) => {
            return ereport(ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not read directory \"{PG_LOGICAL_SNAPSHOTS_DIR}\": %m"
                ))
                .finish(loc("CheckPointSnapBuild"));
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => {
                return ereport(ERROR)
                    .with_saved_errno(e.raw_os_error().unwrap_or(0))
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not read directory \"{PG_LOGICAL_SNAPSHOTS_DIR}\": %m"
                    ))
                    .finish(loc("CheckPointSnapBuild"));
            }
        };
        let name = entry.file_name();
        let name = name.to_string_lossy().into_owned();
        let path = format!("{PG_LOGICAL_SNAPSHOTS_DIR}/{name}");

        if let Ok(de_type) = entry.file_type() {
            if !de_type.is_file() {
                elog(DEBUG1, format!("only regular files expected: {path}"))?;
                continue;
            }
        }

        let Some((hi, lo)) = ondisk::parse_snap_name(&name) else {
            ereport(LOG)
                .errmsg(format!("could not parse file name \"{path}\""))
                .finish(loc("CheckPointSnapBuild"))?;
            continue;
        };

        let lsn: XLogRecPtr = ((hi as u64) << 32) | lo as u64;

        if lsn < cutoff || cutoff == InvalidXLogRecPtr {
            elog(DEBUG1, format!("removing snapbuild snapshot {path}"))?;

            if ondisk::c_unlink(&path) < 0 {
                ereport(LOG)
                    .with_saved_errno(errno::current_errno())
                    .errcode_for_file_access()
                    .errmsg(format!("could not remove file \"{path}\": %m"))
                    .finish(loc("CheckPointSnapBuild"))?;
                continue;
            }
        }
    }
    Ok(())
}

pub fn snap_build_snapshot_exists(lsn: XLogRecPtr) -> PgResult<bool> {
    let path = ondisk::snapshot_path(lsn);
    match std::fs::metadata(&path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => ereport(ERROR)
            .with_saved_errno(e.raw_os_error().unwrap_or(0))
            .errcode_for_file_access()
            .errmsg(format!("could not stat file \"{path}\": %m"))
            .finish(loc("SnapBuildSnapshotExists"))
            .map(|_| false),
    }
}

pub fn init_seams() {
    snapbuild_seams::snap_build_reset_exported_snapshot_state::set(
        snap_build_reset_exported_snapshot_state,
    );
    snapbuild_seams::check_point_snap_build::set(check_point_snap_build);
}
