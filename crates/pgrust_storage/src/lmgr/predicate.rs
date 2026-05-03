use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};
use serde::{Deserialize, Serialize};

use crate::ClientId;
use pgrust_core::{INVALID_TRANSACTION_ID, Snapshot, TransactionId};
use pgrust_core::{InterruptReason, InterruptState, check_for_interrupts};

const SAFE_SNAPSHOT_WAIT_INTERVAL: Duration = Duration::from_millis(50);
const OLD_COMMITTED_CLIENT_ID: ClientId = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SerializableXactId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PredicateLockTarget {
    pub db_oid: u32,
    pub relation_oid: u32,
    pub block_number: Option<u32>,
    pub offset_number: Option<u16>,
}

impl PredicateLockTarget {
    pub fn relation(db_oid: u32, relation_oid: u32) -> Self {
        Self {
            db_oid,
            relation_oid,
            block_number: None,
            offset_number: None,
        }
    }

    pub fn page(db_oid: u32, relation_oid: u32, block_number: u32) -> Self {
        Self {
            db_oid,
            relation_oid,
            block_number: Some(block_number),
            offset_number: None,
        }
    }

    pub fn tuple(db_oid: u32, relation_oid: u32, block_number: u32, offset_number: u16) -> Self {
        Self {
            db_oid,
            relation_oid,
            block_number: Some(block_number),
            offset_number: Some(offset_number),
        }
    }

    fn self_and_parents(self) -> [Self; 3] {
        [
            self,
            Self {
                offset_number: None,
                ..self
            },
            Self {
                block_number: None,
                offset_number: None,
                ..self
            },
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateFailureReason {
    PivotDuringWrite,
    PivotDuringCommit,
    PivotDuringRead,
}

impl PredicateFailureReason {
    pub fn detail(self) -> &'static str {
        match self {
            Self::PivotDuringWrite => {
                "Reason code: Canceled on identification as a pivot, during write."
            }
            Self::PivotDuringCommit => {
                "Reason code: Canceled on identification as a pivot, during commit attempt."
            }
            Self::PivotDuringRead => {
                "Reason code: Canceled on identification as a pivot, during read."
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateLockError {
    SerializationFailure(PredicateFailureReason),
    UnknownSerializableTransaction(SerializableXactId),
    Interrupted(InterruptReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializableRegistration {
    Tracked(SerializableXactId),
    SafeReadOnly,
    WaitForSafeSnapshot(SerializableXactId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SafeSnapshotWaitResult {
    Safe,
    Retry,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedPredicateState {
    pub read_only: bool,
    pub deferrable: bool,
    pub xmin: TransactionId,
    pub snapshot_xmax: TransactionId,
    pub last_commit_before_snapshot: u64,
    pub did_write: bool,
    pub has_conflict_in: bool,
    pub has_conflict_out: bool,
    #[serde(default)]
    pub earliest_out_conflict_commit: Option<u64>,
    pub locks: Vec<PredicateLockTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredicateLockSnapshotRow {
    pub target: PredicateLockTarget,
    pub client_id: ClientId,
    pub granted: bool,
}

#[derive(Debug)]
pub struct PredicateLockManager {
    next_id: AtomicU64,
    state: Mutex<PredicateState>,
    safe_snapshot_cv: Condvar,
}

impl PredicateLockManager {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            state: Mutex::new(PredicateState::default()),
            safe_snapshot_cv: Condvar::new(),
        }
    }

    pub fn begin_serializable_xact(
        &self,
        client_id: ClientId,
        snapshot: &Snapshot,
        read_only: bool,
        deferrable: bool,
    ) -> SerializableRegistration {
        let mut state = self.state.lock();
        state.cleanup_summaries();
        let possible_unsafe_conflicts = if read_only {
            state.active_writer_ids()
        } else {
            BTreeSet::new()
        };
        if read_only && possible_unsafe_conflicts.is_empty() {
            return SerializableRegistration::SafeReadOnly;
        }

        let id = SerializableXactId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let waiting_for_safe_snapshot = read_only && deferrable;
        let xact = SerializableXact {
            id,
            client_id,
            top_xid: None,
            subxids: BTreeSet::new(),
            xmin: snapshot.xmin,
            snapshot_xmax: snapshot.xmax,
            last_commit_before_snapshot: state.last_commit_seq,
            read_only,
            deferrable,
            safe_read_only: false,
            unsafe_read_only: false,
            waiting_for_safe_snapshot,
            did_write: false,
            prepared: false,
            committed: false,
            commit_seq: None,
            doomed: false,
            conflict_in: BTreeSet::new(),
            conflict_out: BTreeSet::new(),
            summary_conflict_in: false,
            summary_conflict_out: false,
            earliest_out_conflict_commit: None,
            possible_unsafe_conflicts,
            locks: BTreeSet::new(),
        };
        state.client_to_xact.insert(client_id, id);
        state.xacts.insert(id, xact);
        if waiting_for_safe_snapshot {
            SerializableRegistration::WaitForSafeSnapshot(id)
        } else {
            SerializableRegistration::Tracked(id)
        }
    }

    pub fn wait_for_safe_snapshot(
        &self,
        id: SerializableXactId,
        interrupts: &InterruptState,
    ) -> Result<SafeSnapshotWaitResult, PredicateLockError> {
        let mut state = self.state.lock();
        loop {
            let Some(xact) = state.xacts.get(&id) else {
                return Ok(SafeSnapshotWaitResult::Safe);
            };
            if xact.unsafe_read_only {
                state.release_xact(id);
                self.safe_snapshot_cv.notify_all();
                return Ok(SafeSnapshotWaitResult::Retry);
            }
            if xact.safe_read_only {
                state.release_xact(id);
                state.cleanup_summaries();
                self.safe_snapshot_cv.notify_all();
                return Ok(SafeSnapshotWaitResult::Safe);
            }

            if let Err(reason) = check_for_interrupts(interrupts) {
                state.release_xact(id);
                state.cleanup_summaries();
                self.safe_snapshot_cv.notify_all();
                return Err(PredicateLockError::Interrupted(reason));
            }
            self.safe_snapshot_cv
                .wait_for(&mut state, SAFE_SNAPSHOT_WAIT_INTERVAL);
        }
    }

    pub fn safe_snapshot_blocking_pids(&self, blocked_pid: ClientId) -> Vec<ClientId> {
        let state = self.state.lock();
        let Some(id) = state.client_to_xact.get(&blocked_pid).copied() else {
            return Vec::new();
        };
        let Some(xact) = state.xacts.get(&id) else {
            return Vec::new();
        };
        if !xact.waiting_for_safe_snapshot || xact.safe_read_only || xact.unsafe_read_only {
            return Vec::new();
        }
        xact.possible_unsafe_conflicts
            .iter()
            .filter_map(|writer_id| state.xacts.get(writer_id).map(|writer| writer.client_id))
            .collect()
    }

    pub fn register_xid(
        &self,
        id: SerializableXactId,
        xid: TransactionId,
    ) -> Result<(), PredicateLockError> {
        if xid == INVALID_TRANSACTION_ID {
            return Ok(());
        }
        let mut state = self.state.lock();
        let xact = state
            .xacts
            .get_mut(&id)
            .ok_or(PredicateLockError::UnknownSerializableTransaction(id))?;
        if xact.top_xid.is_none() {
            xact.top_xid = Some(xid);
        } else if xact.top_xid != Some(xid) {
            xact.subxids.insert(xid);
        }
        state.xid_to_xact.insert(xid, id);
        Ok(())
    }

    pub fn register_subxid(
        &self,
        id: SerializableXactId,
        xid: TransactionId,
    ) -> Result<(), PredicateLockError> {
        if xid == INVALID_TRANSACTION_ID {
            return Ok(());
        }
        let mut state = self.state.lock();
        let xact = state
            .xacts
            .get_mut(&id)
            .ok_or(PredicateLockError::UnknownSerializableTransaction(id))?;
        xact.subxids.insert(xid);
        state.xid_to_xact.insert(xid, id);
        Ok(())
    }

    pub fn predicate_lock(
        &self,
        id: SerializableXactId,
        target: PredicateLockTarget,
    ) -> Result<(), PredicateLockError> {
        let mut state = self.state.lock();
        let xact = state
            .xacts
            .get_mut(&id)
            .ok_or(PredicateLockError::UnknownSerializableTransaction(id))?;
        if xact.safe_read_only {
            return Ok(());
        }
        if xact.locks.contains(&target) {
            return Ok(());
        }
        xact.locks.insert(target);
        state.target_locks.entry(target).or_default().insert(id);
        Ok(())
    }

    pub fn check_conflict_out(
        &self,
        reader_id: SerializableXactId,
        writer_xid: TransactionId,
    ) -> Result<(), PredicateLockError> {
        if writer_xid == INVALID_TRANSACTION_ID {
            return Ok(());
        }
        let mut state = self.state.lock();
        {
            let reader = state.xacts.get(&reader_id).ok_or(
                PredicateLockError::UnknownSerializableTransaction(reader_id),
            )?;
            if reader.safe_read_only {
                return Ok(());
            }
        }
        let Some(&writer_id) = state.xid_to_xact.get(&writer_xid) else {
            state.check_conflict_out_to_summary(reader_id, writer_xid)?;
            self.safe_snapshot_cv.notify_all();
            return Ok(());
        };
        if reader_id == writer_id {
            return Ok(());
        }
        if state.read_only_conflict_out_is_safe(reader_id, writer_id) {
            return Ok(());
        }
        if !state.conflict_may_overlap(reader_id, writer_id) {
            return Ok(());
        }
        state.add_conflict(reader_id, writer_id)?;
        if state.is_pivot(reader_id) {
            state.mark_doomed(reader_id);
            return Err(PredicateLockError::SerializationFailure(
                PredicateFailureReason::PivotDuringRead,
            ));
        }
        self.safe_snapshot_cv.notify_all();
        Ok(())
    }

    pub fn check_conflict_in(
        &self,
        writer_id: SerializableXactId,
        target: PredicateLockTarget,
    ) -> Result<(), PredicateLockError> {
        let mut state = self.state.lock();
        {
            let writer = state.xacts.get_mut(&writer_id).ok_or(
                PredicateLockError::UnknownSerializableTransaction(writer_id),
            )?;
            writer.did_write = true;
            if writer.doomed {
                return Err(PredicateLockError::SerializationFailure(
                    PredicateFailureReason::PivotDuringWrite,
                ));
            }
        }

        let mut readers = BTreeSet::new();
        for parent in target.self_and_parents() {
            if let Some(ids) = state.target_locks.get(&parent) {
                readers.extend(ids.iter().copied());
            }
        }

        for reader_id in readers {
            if reader_id == writer_id || !state.conflict_may_overlap(reader_id, writer_id) {
                continue;
            }
            state.add_conflict(reader_id, writer_id)?;
            if state.write_conflict_makes_writer_dangerous(writer_id) {
                state.mark_doomed(writer_id);
                return Err(PredicateLockError::SerializationFailure(
                    PredicateFailureReason::PivotDuringWrite,
                ));
            }
        }
        if state.add_old_committed_conflict_in(writer_id, target)?
            && state.write_conflict_makes_writer_dangerous(writer_id)
        {
            state.mark_doomed(writer_id);
            return Err(PredicateLockError::SerializationFailure(
                PredicateFailureReason::PivotDuringWrite,
            ));
        }
        self.safe_snapshot_cv.notify_all();
        Ok(())
    }

    pub fn pre_commit(&self, id: SerializableXactId) -> Result<(), PredicateLockError> {
        let mut state = self.state.lock();
        let xact = state
            .xacts
            .get(&id)
            .ok_or(PredicateLockError::UnknownSerializableTransaction(id))?;
        if xact.doomed || state.is_pivot(id) {
            state.mark_doomed(id);
            return Err(PredicateLockError::SerializationFailure(
                PredicateFailureReason::PivotDuringCommit,
            ));
        }
        Ok(())
    }

    pub fn prepare(
        &self,
        id: SerializableXactId,
        prepared_client_id: ClientId,
    ) -> Result<PreparedPredicateState, PredicateLockError> {
        self.pre_commit(id)?;
        let mut state = self.state.lock();
        let old_client_id = state
            .xacts
            .get(&id)
            .ok_or(PredicateLockError::UnknownSerializableTransaction(id))?
            .client_id;
        let xact = state
            .xacts
            .get_mut(&id)
            .ok_or(PredicateLockError::UnknownSerializableTransaction(id))?;
        xact.prepared = true;
        xact.client_id = prepared_client_id;
        let prepared_state = xact.prepared_state();
        state.client_to_xact.remove(&old_client_id);
        state.client_to_xact.insert(prepared_client_id, id);
        Ok(prepared_state)
    }

    pub fn commit(&self, id: SerializableXactId) -> Result<(), PredicateLockError> {
        let mut state = self.state.lock();
        let (client_id, safe_read_only) = {
            let xact = state
                .xacts
                .get(&id)
                .ok_or(PredicateLockError::UnknownSerializableTransaction(id))?;
            (xact.client_id, xact.read_only && xact.safe_read_only)
        };
        if safe_read_only {
            state.release_xact(id);
            state.cleanup_summaries();
            self.safe_snapshot_cv.notify_all();
            return Ok(());
        }

        state.last_commit_seq = state.last_commit_seq.saturating_add(1);
        let commit_seq = state.last_commit_seq;
        let xact = state
            .xacts
            .get_mut(&id)
            .ok_or(PredicateLockError::UnknownSerializableTransaction(id))?;
        xact.prepared = false;
        xact.committed = true;
        xact.commit_seq = Some(commit_seq);
        state.client_to_xact.remove(&client_id);
        state.note_committed_conflict_target(id, commit_seq);
        state.finish_possible_unsafe_writer(id);
        state.summarize_committed_xact(id);
        state.cleanup_summaries();
        self.safe_snapshot_cv.notify_all();
        Ok(())
    }

    pub fn rollback(&self, id: SerializableXactId) {
        let mut state = self.state.lock();
        if state
            .xacts
            .get(&id)
            .is_some_and(|xact| !xact.read_only && !xact.committed)
        {
            state.finish_possible_unsafe_writer(id);
        }
        state.release_xact(id);
        state.cleanup_summaries();
        self.safe_snapshot_cv.notify_all();
    }

    pub fn commit_prepared_xid(&self, xid: TransactionId) -> Result<(), PredicateLockError> {
        let Some(id) = self.state.lock().xid_to_xact.get(&xid).copied() else {
            return Ok(());
        };
        self.commit(id)
    }

    pub fn rollback_prepared_xid(&self, xid: TransactionId) {
        let Some(id) = self.state.lock().xid_to_xact.get(&xid).copied() else {
            return;
        };
        self.rollback(id);
    }

    pub fn restore_prepared(
        &self,
        prepared_client_id: ClientId,
        xid: TransactionId,
        subxids: &[TransactionId],
        prepared: PreparedPredicateState,
    ) {
        let id = SerializableXactId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let locks: BTreeSet<_> = prepared.locks.iter().copied().collect();
        let xact = SerializableXact {
            id,
            client_id: prepared_client_id,
            top_xid: Some(xid),
            subxids: subxids.iter().copied().collect(),
            xmin: prepared.xmin,
            snapshot_xmax: prepared.snapshot_xmax,
            last_commit_before_snapshot: prepared.last_commit_before_snapshot,
            read_only: prepared.read_only,
            deferrable: prepared.deferrable,
            safe_read_only: false,
            unsafe_read_only: false,
            waiting_for_safe_snapshot: false,
            did_write: prepared.did_write,
            prepared: true,
            committed: false,
            commit_seq: None,
            doomed: false,
            conflict_in: BTreeSet::new(),
            conflict_out: BTreeSet::new(),
            summary_conflict_in: prepared.has_conflict_in,
            summary_conflict_out: prepared.has_conflict_out,
            earliest_out_conflict_commit: prepared.earliest_out_conflict_commit,
            possible_unsafe_conflicts: BTreeSet::new(),
            locks,
        };
        let mut state = self.state.lock();
        state.client_to_xact.insert(prepared_client_id, id);
        state.xid_to_xact.insert(xid, id);
        for subxid in subxids {
            state.xid_to_xact.insert(*subxid, id);
        }
        for target in &prepared.locks {
            state.target_locks.entry(*target).or_default().insert(id);
        }
        state.xacts.insert(id, xact);
    }

    pub fn snapshot(&self) -> Vec<PredicateLockSnapshotRow> {
        let state = self.state.lock();
        let mut rows = Vec::new();
        for (target, holders) in &state.target_locks {
            for holder in holders {
                let Some(xact) = state.xacts.get(holder) else {
                    continue;
                };
                rows.push(PredicateLockSnapshotRow {
                    target: *target,
                    client_id: xact.client_id,
                    granted: true,
                });
            }
        }
        for target in state.old_committed_locks.keys() {
            rows.push(PredicateLockSnapshotRow {
                target: *target,
                client_id: OLD_COMMITTED_CLIENT_ID,
                granted: true,
            });
        }
        rows
    }
}

impl Default for PredicateLockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Default)]
struct PredicateState {
    xacts: BTreeMap<SerializableXactId, SerializableXact>,
    xid_to_xact: BTreeMap<TransactionId, SerializableXactId>,
    client_to_xact: BTreeMap<ClientId, SerializableXactId>,
    target_locks: BTreeMap<PredicateLockTarget, BTreeSet<SerializableXactId>>,
    old_committed_locks: BTreeMap<PredicateLockTarget, OldCommittedPredicateLock>,
    serial_summaries: BTreeMap<TransactionId, SerialSummary>,
    last_commit_seq: u64,
}

impl PredicateState {
    fn has_active_writers(&self) -> bool {
        self.xacts
            .values()
            .any(|xact| !xact.read_only && !xact.committed)
    }

    fn active_writer_ids(&self) -> BTreeSet<SerializableXactId> {
        self.xacts
            .iter()
            .filter_map(|(id, xact)| (!xact.read_only && !xact.committed).then_some(*id))
            .collect()
    }

    fn conflict_may_overlap(
        &self,
        reader_id: SerializableXactId,
        writer_id: SerializableXactId,
    ) -> bool {
        let Some(reader) = self.xacts.get(&reader_id) else {
            return false;
        };
        let Some(writer) = self.xacts.get(&writer_id) else {
            return false;
        };
        if reader.committed
            && reader
                .commit_seq
                .is_some_and(|seq| seq <= writer.last_commit_before_snapshot)
        {
            return false;
        }
        if writer.committed
            && writer
                .commit_seq
                .is_some_and(|seq| seq <= reader.last_commit_before_snapshot)
        {
            return false;
        }
        true
    }

    fn read_only_conflict_out_is_safe(
        &self,
        reader_id: SerializableXactId,
        writer_id: SerializableXactId,
    ) -> bool {
        let Some(reader) = self.xacts.get(&reader_id) else {
            return false;
        };
        let Some(writer) = self.xacts.get(&writer_id) else {
            return false;
        };
        reader.read_only
            && writer.committed
            && !writer.summary_conflict_out
            && writer
                .earliest_out_conflict_commit
                .is_none_or(|earliest| reader.last_commit_before_snapshot < earliest)
    }

    fn add_conflict(
        &mut self,
        reader_id: SerializableXactId,
        writer_id: SerializableXactId,
    ) -> Result<(), PredicateLockError> {
        if !self.xacts.contains_key(&reader_id) {
            return Err(PredicateLockError::UnknownSerializableTransaction(
                reader_id,
            ));
        }
        if !self.xacts.contains_key(&writer_id) {
            return Err(PredicateLockError::UnknownSerializableTransaction(
                writer_id,
            ));
        }
        let writer_commit_seq = self
            .xacts
            .get(&writer_id)
            .and_then(|writer| writer.commit_seq);
        if let Some(reader) = self.xacts.get_mut(&reader_id) {
            reader.conflict_out.insert(writer_id);
            if let Some(commit_seq) = writer_commit_seq {
                reader.note_out_conflict_commit(commit_seq);
            }
        }
        if let Some(writer) = self.xacts.get_mut(&writer_id) {
            writer.conflict_in.insert(reader_id);
        }
        self.flag_read_only_unsafe_for_writer(reader_id);
        Ok(())
    }

    fn check_conflict_out_to_summary(
        &mut self,
        reader_id: SerializableXactId,
        writer_xid: TransactionId,
    ) -> Result<(), PredicateLockError> {
        let Some(summary) = self.serial_summaries.get(&writer_xid).copied() else {
            return Ok(());
        };
        let reader = self.xacts.get_mut(&reader_id).ok_or(
            PredicateLockError::UnknownSerializableTransaction(reader_id),
        )?;
        if reader.safe_read_only || summary.commit_seq <= reader.last_commit_before_snapshot {
            return Ok(());
        }
        if let Some(min_conflict_out_commit) = summary.min_conflict_out_commit
            && (!reader.read_only || min_conflict_out_commit <= reader.last_commit_before_snapshot)
        {
            return Err(PredicateLockError::SerializationFailure(
                PredicateFailureReason::PivotDuringRead,
            ));
        }
        if reader.summary_conflict_in || !reader.conflict_in.is_empty() {
            return Err(PredicateLockError::SerializationFailure(
                PredicateFailureReason::PivotDuringRead,
            ));
        }
        reader.summary_conflict_out = true;
        reader.note_out_conflict_commit(summary.commit_seq);
        self.flag_read_only_unsafe_for_writer(reader_id);
        Ok(())
    }

    fn is_pivot(&self, id: SerializableXactId) -> bool {
        self.xacts.get(&id).is_some_and(|xact| {
            (xact.summary_conflict_in || !xact.conflict_in.is_empty())
                && (xact.summary_conflict_out || !xact.conflict_out.is_empty())
        })
    }

    fn write_conflict_makes_writer_dangerous(&self, writer_id: SerializableXactId) -> bool {
        let Some(writer) = self.xacts.get(&writer_id) else {
            return false;
        };
        writer.summary_conflict_out || !writer.conflict_out.is_empty()
    }

    fn mark_doomed(&mut self, id: SerializableXactId) {
        if let Some(xact) = self.xacts.get_mut(&id) {
            xact.doomed = true;
        }
    }

    fn release_xact(&mut self, id: SerializableXactId) {
        let Some(xact) = self.xacts.remove(&id) else {
            return;
        };
        self.release_xact_mappings_and_locks(id, &xact);
        for other in self.xacts.values_mut() {
            other.conflict_in.remove(&id);
            other.conflict_out.remove(&id);
            other.possible_unsafe_conflicts.remove(&id);
        }
    }

    fn release_xact_mappings_and_locks(&mut self, id: SerializableXactId, xact: &SerializableXact) {
        self.client_to_xact.remove(&xact.client_id);
        if let Some(xid) = xact.top_xid {
            self.xid_to_xact.remove(&xid);
        }
        for xid in &xact.subxids {
            self.xid_to_xact.remove(xid);
        }
        self.release_locks_for_xact(id, &xact.locks);
    }

    fn release_locks_for_xact(
        &mut self,
        id: SerializableXactId,
        locks: &BTreeSet<PredicateLockTarget>,
    ) {
        for target in locks {
            let remove_target = if let Some(holders) = self.target_locks.get_mut(target) {
                holders.remove(&id);
                holders.is_empty()
            } else {
                false
            };
            if remove_target {
                self.target_locks.remove(target);
            }
        }
    }

    fn add_old_committed_conflict_in(
        &mut self,
        writer_id: SerializableXactId,
        target: PredicateLockTarget,
    ) -> Result<bool, PredicateLockError> {
        let last_commit_before_snapshot = self
            .xacts
            .get(&writer_id)
            .ok_or(PredicateLockError::UnknownSerializableTransaction(
                writer_id,
            ))?
            .last_commit_before_snapshot;
        let overlaps_old_committed = target.self_and_parents().into_iter().any(|parent| {
            self.old_committed_locks
                .get(&parent)
                .is_some_and(|lock| lock.commit_seq > last_commit_before_snapshot)
        });
        if overlaps_old_committed {
            let writer = self.xacts.get_mut(&writer_id).ok_or(
                PredicateLockError::UnknownSerializableTransaction(writer_id),
            )?;
            writer.summary_conflict_in = true;
        }
        Ok(overlaps_old_committed)
    }

    fn note_committed_conflict_target(
        &mut self,
        committed_id: SerializableXactId,
        commit_seq: u64,
    ) {
        let Some(readers) = self
            .xacts
            .get(&committed_id)
            .map(|xact| xact.conflict_in.iter().copied().collect::<Vec<_>>())
        else {
            return;
        };
        for reader_id in readers {
            if let Some(reader) = self.xacts.get_mut(&reader_id) {
                reader.note_out_conflict_commit(commit_seq);
            }
            self.flag_read_only_unsafe_for_writer(reader_id);
        }
    }

    fn flag_read_only_unsafe_for_writer(&mut self, writer_id: SerializableXactId) {
        let Some(earliest_out_conflict_commit) = self
            .xacts
            .get(&writer_id)
            .and_then(|writer| writer.earliest_out_conflict_commit)
        else {
            return;
        };
        let unsafe_readers = self
            .xacts
            .iter()
            .filter_map(|(id, xact)| {
                (xact.read_only
                    && !xact.safe_read_only
                    && !xact.unsafe_read_only
                    && xact.possible_unsafe_conflicts.contains(&writer_id)
                    && earliest_out_conflict_commit <= xact.last_commit_before_snapshot)
                    .then_some(*id)
            })
            .collect::<Vec<_>>();
        for reader_id in unsafe_readers {
            if let Some(reader) = self.xacts.get_mut(&reader_id) {
                reader.unsafe_read_only = true;
                reader.possible_unsafe_conflicts.clear();
            }
        }
    }

    fn finish_possible_unsafe_writer(&mut self, writer_id: SerializableXactId) {
        self.flag_read_only_unsafe_for_writer(writer_id);
        let readers = self
            .xacts
            .iter()
            .filter_map(|(id, xact)| {
                (xact.read_only
                    && !xact.safe_read_only
                    && !xact.unsafe_read_only
                    && xact.possible_unsafe_conflicts.contains(&writer_id))
                .then_some(*id)
            })
            .collect::<Vec<_>>();
        for reader_id in readers {
            let should_mark_safe = if let Some(reader) = self.xacts.get_mut(&reader_id) {
                reader.possible_unsafe_conflicts.remove(&writer_id);
                reader.possible_unsafe_conflicts.is_empty()
            } else {
                false
            };
            if should_mark_safe {
                self.mark_read_only_safe(reader_id);
            }
        }
    }

    fn mark_read_only_safe(&mut self, id: SerializableXactId) {
        let Some(locks) = self.xacts.get(&id).map(|xact| xact.locks.clone()) else {
            return;
        };
        self.release_locks_for_xact(id, &locks);
        if let Some(xact) = self.xacts.get_mut(&id) {
            xact.safe_read_only = true;
            xact.locks.clear();
            xact.conflict_in.clear();
            xact.conflict_out.clear();
            xact.summary_conflict_in = false;
            xact.summary_conflict_out = false;
            xact.possible_unsafe_conflicts.clear();
        }
        for other in self.xacts.values_mut() {
            other.conflict_in.remove(&id);
            other.conflict_out.remove(&id);
        }
    }

    fn summarize_committed_xact(&mut self, id: SerializableXactId) {
        let Some(xact) = self.xacts.get(&id).cloned() else {
            return;
        };
        if xact.prepared || !xact.committed {
            return;
        }
        let Some(commit_seq) = xact.commit_seq else {
            return;
        };

        let has_conflict_out = xact.summary_conflict_out || !xact.conflict_out.is_empty();
        let min_conflict_out_commit = if has_conflict_out {
            Some(xact.earliest_out_conflict_commit.unwrap_or(commit_seq))
        } else {
            None
        };
        if !xact.read_only {
            let summary = SerialSummary {
                commit_seq,
                min_conflict_out_commit,
            };
            if let Some(xid) = xact.top_xid {
                self.serial_summaries.insert(xid, summary);
            }
            for xid in &xact.subxids {
                self.serial_summaries.insert(*xid, summary);
            }
        }

        for target in &xact.locks {
            self.old_committed_locks
                .entry(*target)
                .and_modify(|lock| lock.commit_seq = lock.commit_seq.max(commit_seq))
                .or_insert(OldCommittedPredicateLock { commit_seq });
        }

        for other in self.xacts.values_mut() {
            if other.conflict_out.remove(&id) {
                other.summary_conflict_out = true;
                other.note_out_conflict_commit(commit_seq);
            }
            if other.conflict_in.remove(&id) {
                other.summary_conflict_in = true;
            }
            other.possible_unsafe_conflicts.remove(&id);
        }

        self.xacts.remove(&id);
        self.release_xact_mappings_and_locks(id, &xact);
    }

    fn cleanup_summaries(&mut self) {
        let oldest_active_snapshot = self
            .xacts
            .values()
            .filter(|xact| !xact.committed)
            .map(|xact| xact.last_commit_before_snapshot)
            .min();
        let Some(oldest_active_snapshot) = oldest_active_snapshot else {
            self.serial_summaries.clear();
            self.old_committed_locks.clear();
            return;
        };
        self.serial_summaries
            .retain(|_, summary| summary.commit_seq > oldest_active_snapshot);
        self.old_committed_locks
            .retain(|_, lock| lock.commit_seq > oldest_active_snapshot);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SerialSummary {
    commit_seq: u64,
    min_conflict_out_commit: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OldCommittedPredicateLock {
    commit_seq: u64,
}

#[derive(Debug, Clone)]
struct SerializableXact {
    id: SerializableXactId,
    client_id: ClientId,
    top_xid: Option<TransactionId>,
    subxids: BTreeSet<TransactionId>,
    xmin: TransactionId,
    snapshot_xmax: TransactionId,
    last_commit_before_snapshot: u64,
    read_only: bool,
    deferrable: bool,
    safe_read_only: bool,
    unsafe_read_only: bool,
    waiting_for_safe_snapshot: bool,
    did_write: bool,
    prepared: bool,
    committed: bool,
    commit_seq: Option<u64>,
    doomed: bool,
    conflict_in: BTreeSet<SerializableXactId>,
    conflict_out: BTreeSet<SerializableXactId>,
    summary_conflict_in: bool,
    summary_conflict_out: bool,
    earliest_out_conflict_commit: Option<u64>,
    possible_unsafe_conflicts: BTreeSet<SerializableXactId>,
    locks: BTreeSet<PredicateLockTarget>,
}

impl SerializableXact {
    fn note_out_conflict_commit(&mut self, commit_seq: u64) {
        self.earliest_out_conflict_commit = Some(
            self.earliest_out_conflict_commit
                .map_or(commit_seq, |earliest| earliest.min(commit_seq)),
        );
    }

    fn prepared_state(&self) -> PreparedPredicateState {
        PreparedPredicateState {
            read_only: self.read_only,
            deferrable: self.deferrable,
            xmin: self.xmin,
            snapshot_xmax: self.snapshot_xmax,
            last_commit_before_snapshot: self.last_commit_before_snapshot,
            did_write: self.did_write,
            has_conflict_in: self.summary_conflict_in || !self.conflict_in.is_empty(),
            has_conflict_out: self.summary_conflict_out || !self.conflict_out.is_empty(),
            earliest_out_conflict_commit: self.earliest_out_conflict_commit,
            locks: self.locks.iter().copied().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn test_snapshot() -> Snapshot {
        Snapshot {
            current_xid: INVALID_TRANSACTION_ID,
            current_cid: 0,
            heap_current_cid: None,
            xmin: 3,
            xmax: 100,
            in_progress: BTreeSet::new(),
            own_xids: BTreeSet::new(),
        }
    }

    fn tracked(registration: SerializableRegistration) -> SerializableXactId {
        match registration {
            SerializableRegistration::Tracked(id)
            | SerializableRegistration::WaitForSafeSnapshot(id) => id,
            SerializableRegistration::SafeReadOnly => panic!("expected tracked SSI state"),
        }
    }

    #[test]
    fn read_only_without_active_writer_uses_safe_snapshot_immediately() {
        let manager = PredicateLockManager::new();

        let registration = manager.begin_serializable_xact(1, &test_snapshot(), true, true);

        assert_eq!(registration, SerializableRegistration::SafeReadOnly);
        assert!(manager.snapshot().is_empty());
    }

    #[test]
    fn deferrable_read_only_waits_for_active_writer_to_finish() {
        let manager = Arc::new(PredicateLockManager::new());
        let writer = tracked(manager.begin_serializable_xact(1, &test_snapshot(), false, false));
        let reader = match manager.begin_serializable_xact(2, &test_snapshot(), true, true) {
            SerializableRegistration::WaitForSafeSnapshot(id) => id,
            other => panic!("expected safe snapshot wait, got {other:?}"),
        };
        assert_eq!(manager.safe_snapshot_blocking_pids(2), vec![1]);

        let waiter_manager = Arc::clone(&manager);
        let waiter = thread::spawn(move || {
            let interrupts = InterruptState::new();
            waiter_manager.wait_for_safe_snapshot(reader, &interrupts)
        });

        manager.commit(writer).unwrap();

        assert_eq!(
            waiter.join().unwrap().unwrap(),
            SafeSnapshotWaitResult::Safe
        );
        assert!(manager.safe_snapshot_blocking_pids(2).is_empty());
    }

    #[test]
    fn deferrable_read_only_retries_when_snapshot_becomes_unsafe() {
        let manager = PredicateLockManager::new();
        let writer = tracked(manager.begin_serializable_xact(1, &test_snapshot(), false, false));
        let old_writer =
            tracked(manager.begin_serializable_xact(3, &test_snapshot(), false, false));
        manager.register_xid(old_writer, 30).unwrap();
        manager.commit(old_writer).unwrap();

        let reader = match manager.begin_serializable_xact(2, &test_snapshot(), true, true) {
            SerializableRegistration::WaitForSafeSnapshot(id) => id,
            other => panic!("expected safe snapshot wait, got {other:?}"),
        };

        manager.check_conflict_out(writer, 30).unwrap();

        let interrupts = InterruptState::new();
        assert_eq!(
            manager.wait_for_safe_snapshot(reader, &interrupts).unwrap(),
            SafeSnapshotWaitResult::Retry
        );
    }

    #[test]
    fn summarized_committed_xact_keeps_serial_summary_and_old_lock_until_overlap_ends() {
        let manager = PredicateLockManager::new();
        let target = PredicateLockTarget::relation(1, 1259);
        let active = tracked(manager.begin_serializable_xact(1, &test_snapshot(), false, false));
        let old_reader =
            tracked(manager.begin_serializable_xact(2, &test_snapshot(), false, false));
        manager.register_xid(old_reader, 40).unwrap();
        manager.predicate_lock(old_reader, target).unwrap();

        manager.commit(old_reader).unwrap();

        {
            let state = manager.state.lock();
            assert!(state.serial_summaries.contains_key(&40));
            assert!(state.old_committed_locks.contains_key(&target));
            assert!(!state.xacts.contains_key(&old_reader));
        }
        assert!(
            manager
                .snapshot()
                .iter()
                .any(|row| { row.target == target && row.client_id == OLD_COMMITTED_CLIENT_ID })
        );

        manager.check_conflict_out(active, 40).unwrap();
        assert!(matches!(
            manager.check_conflict_in(active, target),
            Err(PredicateLockError::SerializationFailure(
                PredicateFailureReason::PivotDuringWrite
            ))
        ));

        manager.rollback(active);
        let state = manager.state.lock();
        assert!(state.serial_summaries.is_empty());
        assert!(state.old_committed_locks.is_empty());
    }
}
