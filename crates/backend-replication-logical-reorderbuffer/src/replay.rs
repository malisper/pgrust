//! Change-replay family of `replication/logical/reorderbuffer.c`.
//!
//! This is the spine that drives a decoded transaction out to the output
//! plugin: the parent/child association and snapshot transfer
//! (`ReorderBufferAssignChild` / `ReorderBufferTransferSnapToParent` /
//! `ReorderBufferCommitChild`), the k-way LSN merge over the top transaction's
//! and its subtransactions' in-memory change queues
//! (`ReorderBufferIterTXNInit` / `Next` / `Finish`), the per-change action
//! dispatch (`ReorderBufferProcessTXN`) including the now-landed historic
//! snapshot setup (`SetupHistoricSnapshot`, snapmgr-owned), the apply callbacks
//! (`ReorderBufferApplyChange` / `ApplyTruncate` / `ApplyMessage`), the local
//! execution of accumulated invalidations
//! (`ReorderBufferExecuteInvalidations`), the commit-time cleanup
//! (`ReorderBufferCleanupTXN` / `ReorderBufferTruncateTXN` /
//! `ReorderBufferResetTXN`), the per-record frees (`ReorderBufferFreeChange` /
//! `ReorderBufferFreeTXN`), and the public commit entry
//! (`ReorderBufferReplay` / `ReorderBufferCommit`).
//!
//! Several interior steps of `ReorderBufferProcessTXN` reach subsystems whose
//! model this repo has not built yet; each is a loud mirror-PG-and-panic, never
//! a silent stub:
//!
//! * the decoded-tuple INSERT/UPDATE/DELETE/SPEC apply path needs the decoded
//!   `HeapTuple` carrier on `ReorderBufferChangeData::Tp` (not yet modeled), the
//!   relfilenumber→oid map (`RelidByRelfilenumber`), `RelationIdGetRelation`,
//!   `IsToastRelation` / `RelationIsLogicallyLogged`, and TOAST reassembly;
//! * the output-plugin callbacks (`rb->begin` / `apply_change` / `commit` / …)
//!   are owned by `logical.c` and reached through its
//!   `dispatch_reorderbuffer_callback` inward seam, which itself needs the
//!   reorderbuffer owner to hold the live `LogicalDecodingContext`
//!   (`rb->private_data`) and to produce the `RelationHandle` / `ChangeHandle`
//!   the wrappers read — neither modeled yet;
//! * the historic-snapshot setup/teardown (`SetupHistoricSnapshot` /
//!   `TeardownHistoricSnapshot`) signature in `snapmgr` is still the C
//!   `Snapshot` / `*mut HTAB` ABI, not this crate's owned value model;
//! * the surrounding transaction machinery
//!   (`BeginInternalSubTransaction` / `StartTransactionCommand` /
//!   `AbortCurrentTransaction` / `RollbackAndReleaseCurrentSubTransaction`) is
//!   unported.
//!
//! Everything that does not cross one of those boundaries is ported faithfully.

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::{TimestampTz, TransactionId, XLogRecPtr};
use types_core::xact::{CommandId, FirstCommandId, InvalidTransactionId, InvalidXLogRecPtr};
use types_core::primitive::RepOriginId;

use crate::{
    ReorderBuffer, ReorderBufferChange, ReorderBufferChangeData, ReorderBufferChangeType,
    RBTXN_IS_SERIALIZED, RBTXN_IS_SERIALIZED_CLEAR, RBTXN_IS_STREAMED, RBTXN_IS_SUBXACT,
};

/// `CHANGES_THRESHOLD` (reorderbuffer.c) — emit a keepalive
/// (`update_progress_txn`) after this many changes.
const CHANGES_THRESHOLD: i32 = 100;

// ---------------------------------------------------------------------------
// Iterator state (file-local C structs ReorderBufferIterTXNState / *Entry)
// ---------------------------------------------------------------------------

/// One entry of the k-way merge: the next pending change of one (sub)txn, keyed
/// for the binary heap by its `lsn`. Mirrors `ReorderBufferIterTXNEntry`; the
/// on-disk `file`/`segno` fields belong to the spill family (serialized restore
/// is reached through [`ReorderBuffer::restore_changes`]).
struct IterEntry {
    /// `XLogRecPtr lsn` — LSN of `change` (the current change of this txn).
    lsn: XLogRecPtr,
    /// The owning (sub)transaction's xid.
    txn_xid: TransactionId,
    /// Index of the next change to yield from that txn's `changes` Vec.
    next_idx: usize,
    /// `TXNEntryFile file` — the open spill segment + read cursor for a
    /// serialized txn (C `entries[off].file`). Default (no file open) until the
    /// txn is serialized and its changes are paged back through
    /// `ReorderBufferRestoreChanges`.
    file: crate::spill::TxnEntryFile,
    /// `XLogSegNo segno` — the segment the next restore batch reads from
    /// (C `entries[off].segno`). 0 == not yet started.
    segno: u64,
}

/// The binary-heap node: `(lsn, entry-index)`. The C heap stores the int32
/// entry index and reads `state->entries[off].lsn` in the comparator via the
/// heap's `arg`; carrying the LSN in the node itself is the faithful equivalent
/// with no shared state, and keeps the `entry-index` as the secondary tag so
/// `replace_first`/`remove_first` address the right entry.
type IterNode = (XLogRecPtr, i32);

/// `ReorderBufferIterTXNState` — the binary heap plus the owned entry array. The
/// C `old_change` deferred-free slot is unnecessary: our changes are owned in
/// the txn `changes` Vec and freed when the txn is cleaned up, not reused across
/// `Next` calls.
pub(crate) struct IterTxnState {
    /// `entries[]` — one per (sub)txn that contains changes.
    entries: Vec<IterEntry>,
    /// `binaryheap *heap` of `(lsn, off)`, min-ordered on `lsn`.
    heap: backend_lib_binaryheap::BinaryHeap<IterNode, IterCompare>,
}

/// `ReorderBufferIterCompare` — order the heap so the smallest `lsn` comes
/// first. The C returns the inverted sign (a max-heap API used as a min-heap);
/// our [`backend_lib_binaryheap`] is a faithful port of the same API, so we
/// mirror the sign exactly.
type IterCompare = fn(&IterNode, &IterNode) -> i32;

impl ReorderBuffer {
    // -----------------------------------------------------------------------
    // ReorderBufferAssignChild / TransferSnapToParent / CommitChild
    // -----------------------------------------------------------------------

    /// `ReorderBufferAssignChild(rb, xid, subxid, lsn)` — record that `subxid`
    /// is a subtransaction of `xid`, as of `lsn`.
    pub fn assign_child(&mut self, xid: TransactionId, subxid: TransactionId, lsn: XLogRecPtr) {
        let mut new_top = None;
        let txn_xid = self
            .txn_by_xid_pub(xid, true, &mut new_top, lsn, true)
            .expect("create == true yields a txn");
        let mut new_sub = None;
        let subtxn_xid = self
            .txn_by_xid_pub(subxid, true, &mut new_sub, lsn, false)
            .expect("create == true yields a txn");

        if new_sub != Some(true) {
            if self.with_txn_pub(subtxn_xid, |t| t.is_known_subxact()) {
                // already associated, nothing to do.
                return;
            }
            // Seen before but as a top-level txn; now we know it isn't.
            self.toplevel_by_lsn_remove(subtxn_xid);
        }

        self.with_txn_pub(subtxn_xid, |t| {
            t.txn_flags |= RBTXN_IS_SUBXACT;
            t.toplevel_xid = xid;
            debug_assert!(t.nsubtxns == 0);
        });

        // add to subtransaction list
        self.with_txn_pub(txn_xid, |t| {
            t.subtxns.push(subtxn_xid);
            t.nsubtxns += 1;
        });

        // Possibly transfer the subtxn's snapshot to its top-level txn.
        self.transfer_snap_to_parent(txn_xid, subtxn_xid);

        self.assert_txn_lsn_order_pub();
    }

    /// `ReorderBufferTransferSnapToParent(txn, subtxn)` — move the subxact's
    /// base snapshot up to its top-level txn when the top has none or a later
    /// one. The subtransaction's snapshot is cleared either way.
    fn transfer_snap_to_parent(&mut self, txn_xid: TransactionId, subtxn_xid: TransactionId) {
        debug_assert!(self.with_txn_pub(subtxn_xid, |t| t.toplevel_xid) == txn_xid);

        let (sub_has_snap, sub_lsn) =
            self.with_txn_pub(subtxn_xid, |t| (t.base_snapshot.is_some(), t.base_snapshot_lsn));
        if !sub_has_snap {
            return;
        }

        let (top_has_snap, top_lsn) =
            self.with_txn_pub(txn_xid, |t| (t.base_snapshot.is_some(), t.base_snapshot_lsn));

        if !top_has_snap || sub_lsn < top_lsn {
            // If the toplevel already has a base snapshot but it's newer than
            // the subxact's, purge it.
            if top_has_snap {
                self.base_snapshot_dec_refcount(txn_xid);
                self.txns_by_base_snapshot_lsn_remove(txn_xid);
            }

            // The snapshot is now the top transaction's; transfer it, and move
            // the top txn into the subxact's position in the LSN-ordered list.
            let (snap, snap_lsn) =
                self.with_txn_pub(subtxn_xid, |t| (t.base_snapshot.take(), t.base_snapshot_lsn));
            self.with_txn_pub(txn_xid, |t| {
                t.base_snapshot = snap;
                t.base_snapshot_lsn = snap_lsn;
            });
            self.txns_by_base_snapshot_lsn_insert_before(subtxn_xid, txn_xid);

            // The subtransaction doesn't have a snapshot anymore.
            self.with_txn_pub(subtxn_xid, |t| {
                t.base_snapshot = None;
                t.base_snapshot_lsn = InvalidXLogRecPtr;
            });
            self.txns_by_base_snapshot_lsn_remove(subtxn_xid);
        } else {
            // Base snap of toplevel is fine, so the subxact's is not needed.
            self.base_snapshot_dec_refcount(subtxn_xid);
            self.txns_by_base_snapshot_lsn_remove(subtxn_xid);
            self.with_txn_pub(subtxn_xid, |t| {
                t.base_snapshot = None;
                t.base_snapshot_lsn = InvalidXLogRecPtr;
            });
        }
    }

    /// `ReorderBufferCommitChild(rb, xid, subxid, commit_lsn, end_lsn)`.
    pub fn commit_child(
        &mut self,
        xid: TransactionId,
        subxid: TransactionId,
        commit_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
    ) {
        let subtxn_xid =
            match self.txn_by_xid_pub(subxid, false, &mut None, InvalidXLogRecPtr, false) {
                None => return, // no changes in subxact -> nothing to do
                Some(x) => x,
            };

        self.with_txn_pub(subtxn_xid, |t| {
            t.final_lsn = commit_lsn;
            t.end_lsn = end_lsn;
        });

        self.assign_child(xid, subxid, InvalidXLogRecPtr);
    }

    // -----------------------------------------------------------------------
    // ReorderBufferIterTXNInit / Next / Finish (k-way merge)
    // -----------------------------------------------------------------------

    /// `ReorderBufferIterTXNInit(rb, txn, &iterstate)` — assemble the binary
    /// heap with one entry per (sub)txn that has in-memory changes.
    ///
    /// Serialized (spilled) txns require `ReorderBufferSerializeTXN` +
    /// `ReorderBufferRestoreChanges` to page their changes back in; that is the
    /// spill family and is reached through [`ReorderBuffer::restore_changes`].
    fn iter_txn_init(&mut self, txn_xid: TransactionId) -> IterTxnState {
        self.assert_change_lsn_order(txn_xid);

        // Collect the (sub)txns that contain changes: the toplevel txn plus its
        // subtxns (one heap element each).
        let mut member_xids: Vec<TransactionId> = Vec::new();
        if self.with_txn_pub(txn_xid, |t| t.nentries) > 0 {
            member_xids.push(txn_xid);
        }
        let subtxns = self.with_txn_pub(txn_xid, |t| t.subtxns.clone());
        for sub_xid in subtxns {
            self.assert_change_lsn_order(sub_xid);
            if self.with_txn_pub(sub_xid, |t| t.nentries) > 0 {
                member_xids.push(sub_xid);
            }
        }

        let nr_txns = member_xids.len();
        let mut entries: Vec<IterEntry> = Vec::with_capacity(nr_txns);

        for &m_xid in &member_xids {
            let mut file = crate::spill::TxnEntryFile::default();
            let mut segno: u64 = 0;
            if self.with_txn_pub(m_xid, |t| t.is_serialized()) {
                // serialize any remaining in-memory changes, then page the first
                // batch back from disk (C ReorderBufferIterTXNInit).
                self.serialize_txn(m_xid);
                self.restore_changes(m_xid, &mut file, &mut segno);
            }
            let lsn = self.with_txn_pub(m_xid, |t| t.changes[0].lsn);
            entries.push(IterEntry {
                lsn,
                txn_xid: m_xid,
                next_idx: 1,
                file,
                segno,
            });
        }

        let cmp: IterCompare = iter_compare;
        let mut heap = backend_lib_binaryheap::BinaryHeap::allocate(nr_txns as i32, cmp)
            .expect("binaryheap_allocate");
        // Insert items unordered, then a single build step (more efficient).
        for off in 0..nr_txns {
            heap.add_unordered((entries[off].lsn, off as i32))
                .expect("binaryheap_add_unordered");
        }
        heap.build();

        IterTxnState { entries, heap }
    }

    /// `ReorderBufferIterTXNNext(rb, state)` — yield the next change in LSN
    /// order across all (sub)txns, or `None` when exhausted.
    fn iter_txn_next(
        &mut self,
        state: &mut IterTxnState,
    ) -> Option<(ReorderBufferChange, TransactionId)> {
        if state.heap.is_empty() {
            return None;
        }

        let (_, off) = *state.heap.first();
        let entry = &state.entries[off as usize];
        let entry_xid = entry.txn_xid;
        let idx = entry.next_idx - 1;

        // The C yields a borrowed change and advances; our changes are owned in
        // the txn Vec. We clone the change to hand out (the apply callbacks read
        // it; the original is freed when the txn is cleaned/truncated). Cloning
        // preserves the exact yielded value without disturbing list ownership.
        let change = self.with_txn_pub(entry_xid, |t| t.changes[idx].shallow_clone());

        let (nentries, nentries_mem) =
            self.with_txn_pub(entry_xid, |t| (t.nentries, t.nentries_mem));
        let has_next_mem = state.entries[off as usize].next_idx
            < self.with_txn_pub(entry_xid, |t| t.changes.len());

        if has_next_mem {
            // there are more in-memory changes for this txn.
            let next_idx = state.entries[off as usize].next_idx;
            let next_lsn = self.with_txn_pub(entry_xid, |t| t.changes[next_idx].lsn);
            state.entries[off as usize].lsn = next_lsn;
            state.entries[off as usize].next_idx = next_idx + 1;
            state.heap.replace_first((next_lsn, off));
            return Some((change, entry_xid));
        }

        // try to load changes from disk (spill family).
        if nentries != nentries_mem {
            let txn_size = self.with_txn_pub(entry_xid, |t| t.size) as i64;
            self.totalbytes_add(txn_size);
            // Page the next batch back in; restore_changes threads the entry's
            // persistent open segment + cursor.
            let restored = {
                let entry = &mut state.entries[off as usize];
                let mut file = core::mem::take(&mut entry.file);
                let mut segno = entry.segno;
                let n = self.restore_changes(entry_xid, &mut file, &mut segno);
                let entry = &mut state.entries[off as usize];
                entry.file = file;
                entry.segno = segno;
                n
            };
            if restored > 0 {
                let next_lsn = self.with_txn_pub(entry_xid, |t| t.changes[0].lsn);
                state.entries[off as usize].lsn = next_lsn;
                state.entries[off as usize].next_idx = 1;
                state.heap.replace_first((next_lsn, off));
                return Some((change, entry_xid));
            }
        }

        // no changes left for this txn; remove it from the heap.
        state.heap.remove_first();
        Some((change, entry_xid))
    }

    /// `ReorderBufferIterTXNFinish(rb, state)` — free the iterator: close any
    /// still-open spill segment vfds, then drop the heap.
    fn iter_txn_finish(&mut self, state: IterTxnState) {
        for entry in &state.entries {
            if let Some(vfd) = entry.file.vfd {
                backend_storage_file_fd_seams::file_close::call(vfd);
            }
        }
        state.heap.free();
    }

    /// Test-only: drive the full k-way merge and collect yielded LSNs in order.
    #[cfg(test)]
    pub(crate) fn iter_lsns_collect(&mut self, txn_xid: TransactionId) -> Vec<XLogRecPtr> {
        let mut state = self.iter_txn_init(txn_xid);
        let mut out = Vec::new();
        while let Some((change, _xid)) = self.iter_txn_next(&mut state) {
            out.push(change.lsn);
        }
        self.iter_txn_finish(state);
        out
    }

    // -----------------------------------------------------------------------
    // ReorderBufferProcessTXN — the per-change action dispatch
    // -----------------------------------------------------------------------

    /// `ReorderBufferProcessTXN(rb, txn, commit_lsn, snapshot_now, command_id,
    /// streaming)` — send a transaction's changes to the output plugin in LSN
    /// order.
    ///
    /// The historic tuplecid hash this builds is what activates the now-landed
    /// `ResolveCminCmaxDuringDecoding` path: `SetupHistoricSnapshot` (snapmgr,
    /// via [`ReorderBuffer::setup_historic_snapshot`]) installs both the
    /// snapshot and `txn->tuplecid_hash` as the active `(relfilelocator, ctid)
    /// -> (cmin, cmax)` lookup, exactly as the C
    /// `SetupHistoricSnapshot(snapshot_now, txn->tuplecid_hash)` does.
    ///
    /// The output-plugin callbacks, the transaction machinery, and the decoded
    /// tuple/relcache path are unported; the body drives the portable spine and
    /// panics loudly at each of those boundaries (see the module docs).
    fn process_txn(
        &mut self,
        txn_xid: TransactionId,
        commit_lsn: XLogRecPtr,
        snapshot_now: crate::SnapshotData,
        command_id: CommandId,
        streaming: bool,
    ) {
        // build data to be able to lookup the CommandIds of catalog tuples.
        self.build_tuple_cid_hash(txn_xid);

        // setup the initial snapshot: hand the decoded snapshot and the built
        // tuplecid hash to snapmgr's SetupHistoricSnapshot so the historic-MVCC
        // resolver can see them (snapmgr owns the active `tuplecid_data`).
        self.setup_historic_snapshot(&snapshot_now, txn_xid);

        // Decoding needs access to syscaches et al., which use heavyweight locks;
        // run inside an internal (sub)transaction. When called via the SQL SRF a
        // transaction is already started, so use an explicit subtransaction.
        let using_subtxn =
            backend_access_transam_xact_seams::is_transaction_or_transaction_block::call();

        // C `volatile` locals shared between the PG_TRY body and the PG_CATCH
        // handler. We thread them by `&mut` into the body and read the mutated
        // values back in the handler.
        let mut snapshot_now = snapshot_now;
        let mut command_id = command_id;
        let mut prev_lsn = InvalidXLogRecPtr;
        let mut specinsert: Option<ReorderBufferChange> = None;
        let mut stream_started = false;
        let mut curtxn: Option<TransactionId> = None;

        let result = self.process_txn_body(
            txn_xid,
            commit_lsn,
            streaming,
            using_subtxn,
            &mut snapshot_now,
            &mut command_id,
            &mut prev_lsn,
            &mut specinsert,
            &mut stream_started,
            &mut curtxn,
        );

        match result {
            Ok(()) => {}
            Err(errdata) => {
                self.process_txn_catch(
                    txn_xid,
                    streaming,
                    using_subtxn,
                    snapshot_now,
                    command_id,
                    prev_lsn,
                    specinsert,
                    stream_started,
                    curtxn,
                    errdata,
                );
            }
        }
    }

    /// The PG_TRY body of `ReorderBufferProcessTXN` (reorderbuffer.c:2242-2722).
    /// Returns `Err` on the first `ereport` (the C `PG_CATCH` boundary); the
    /// `&mut` volatile locals carry the partial state the handler needs.
    ///
    /// `iterstate` is held locally — on the error path the C
    /// `ReorderBufferIterTXNFinish(iterstate)` runs in the handler; here the
    /// iterator owns open spill vfds, so we move it back through `self` is not
    /// possible (it borrows nothing of self). Instead we finish it on the error
    /// path inside this function before returning `Err` for the cases that own
    /// it, mirroring the C cleanup (`if (iterstate) ReorderBufferIterTXNFinish`).
    #[allow(clippy::too_many_arguments)]
    fn process_txn_body(
        &mut self,
        txn_xid: TransactionId,
        commit_lsn: XLogRecPtr,
        streaming: bool,
        using_subtxn: bool,
        snapshot_now: &mut crate::SnapshotData,
        command_id: &mut CommandId,
        prev_lsn: &mut XLogRecPtr,
        specinsert: &mut Option<ReorderBufferChange>,
        stream_started: &mut bool,
        curtxn: &mut Option<TransactionId>,
    ) -> Result<(), types_error::PgError> {
        let mut changes_count = 0;

        if using_subtxn {
            backend_access_transam_xact_seams::begin_internal_sub_transaction::call(Some(
                if streaming { "stream" } else { "replay" },
            ))?;
        } else {
            backend_access_transam_xact_seams::start_transaction_command::call()?;
        }

        // We only need to send begin/begin-prepare for non-streamed txns.
        if !streaming {
            self.begin_output(txn_xid, streaming);
        }

        let mut iterstate = self.iter_txn_init(txn_xid);

        let prepared = self.with_txn_pub(txn_xid, |t| t.is_prepared());

        // Run the per-change loop. On error we finish the iterator (the C
        // PG_CATCH cleanup) before propagating.
        let loop_result = self.process_txn_loop(
            txn_xid,
            streaming,
            &mut iterstate,
            snapshot_now,
            command_id,
            prev_lsn,
            specinsert,
            stream_started,
            curtxn,
            &mut changes_count,
        );

        if let Err(e) = loop_result {
            self.iter_txn_finish(iterstate);
            return Err(e);
        }

        // speculative insertion record must be freed by now.
        debug_assert!(specinsert.is_none());

        // clean up the iterator.
        self.iter_txn_finish(iterstate);

        // Update total txn count + bytes (after IterTXNFinish, which released the
        // serialized change already accounted in IterTXNNext).
        if !self.with_txn_pub(txn_xid, |t| t.is_streamed()) {
            self.total_txns_add(1);
        }
        let total_size = self.with_txn_pub(txn_xid, |t| t.total_size);
        self.totalbytes_add(total_size as i64);

        // Send the last message for this set of changes.
        if streaming {
            if *stream_started {
                self.stream_stop_output(txn_xid, *prev_lsn);
                *stream_started = false;
            }
        } else if prepared {
            debug_assert!(!self.with_txn_pub(txn_xid, |t| t.sent_prepare()));
            self.prepare_output(txn_xid, commit_lsn)?;
            self.with_txn_pub(txn_xid, |t| t.txn_flags |= crate::RBTXN_SENT_PREPARE);
        } else {
            self.commit_output(txn_xid, commit_lsn)?;
        }

        // sanity check against bad output plugin behaviour.
        if backend_access_transam_xact_seams::get_current_transaction_id_if_any::call()
            != InvalidTransactionId
        {
            return Err(types_error::PgError::error(
                "output plugin used XID during logical decoding",
            ));
        }

        // Remember command id + snapshot for the next set of changes (streaming),
        // else free the per-run copy.
        if streaming {
            self.save_txn_snapshot(txn_xid, snapshot_now.clone(), *command_id);
        } else if snapshot_now.copied {
            self.free_snap(snapshot_now.clone());
        }

        // cleanup.
        backend_utils_time_snapmgr_seams::teardown_historic_snapshot::call(false);

        // Abort the (sub)transaction as a whole: release locks, no persistent
        // effects.
        backend_access_transam_xact_seams::abort_current_transaction::call()?;

        // make sure there's no cache pollution.
        self.process_txn_apply_inval(txn_xid)?;

        if using_subtxn {
            backend_access_transam_xact_seams::rollback_and_release_current_sub_transaction::call()?;
        }

        // Truncate (streamed/prepared) or fully clean up (committed).
        if streaming || prepared {
            if streaming {
                self.maybe_mark_txn_streamed(txn_xid);
            }
            self.truncate_txn(txn_xid, prepared);
            // Reset CheckXidAlive.
            backend_access_transam_xact_seams::set_check_xid_alive::call(InvalidTransactionId);
        } else {
            self.cleanup_txn(txn_xid);
        }

        Ok(())
    }

    /// The per-change `while` loop body of `ReorderBufferProcessTXN`
    /// (reorderbuffer.c:2266-2605). Split out so the body can `?`-propagate the
    /// first `ereport` cleanly.
    #[allow(clippy::too_many_arguments)]
    fn process_txn_loop(
        &mut self,
        txn_xid: TransactionId,
        streaming: bool,
        iterstate: &mut IterTxnState,
        snapshot_now: &mut crate::SnapshotData,
        command_id: &mut CommandId,
        prev_lsn: &mut XLogRecPtr,
        specinsert: &mut Option<ReorderBufferChange>,
        stream_started: &mut bool,
        curtxn: &mut Option<TransactionId>,
        changes_count: &mut i32,
    ) -> Result<(), types_error::PgError> {
        while let Some((change, change_txn)) = self.iter_txn_next(iterstate) {
            // CHECK_FOR_INTERRUPTS();

            // We can't call start stream callback before the first change.
            if *prev_lsn == InvalidXLogRecPtr && streaming {
                self.with_txn_pub(txn_xid, |t| t.origin_id = change.origin_id);
                self.stream_start_output(txn_xid, change.lsn);
                *stream_started = true;
            }

            debug_assert!(*prev_lsn == InvalidXLogRecPtr || *prev_lsn <= change.lsn);
            *prev_lsn = change.lsn;

            // Set the current xid to detect concurrent aborts (we decode before
            // the COMMIT record is processed for streaming / prepared txns).
            // `change->txn` is the (sub)txn that owns the change — returned by the
            // iterator alongside the change.
            let change_prepared = self.with_txn_pub(change_txn, |t| t.is_prepared());
            if streaming || change_prepared {
                *curtxn = Some(change_txn);
                let cxid = self.with_txn_pub(change_txn, |t| t.xid);
                backend_access_transam_xact_seams::set_check_xid_alive::call(cxid);
            }

            self.process_txn_change(
                txn_xid,
                streaming,
                change,
                snapshot_now,
                command_id,
                specinsert,
            )?;

            *changes_count += 1;
            if *changes_count >= CHANGES_THRESHOLD {
                self.update_progress_txn(txn_xid, *prev_lsn);
                *changes_count = 0;
            }
        }
        Ok(())
    }

    /// The change-action `switch` of `ReorderBufferProcessTXN`
    /// (reorderbuffer.c:2307-2586) for one change.
    ///
    /// The decoded-tuple INSERT/UPDATE/DELETE/SPEC/TRUNCATE apply path and the
    /// MESSAGE callback bottom out on the output-plugin Relation/Change/Prefix/
    /// Message handle-producer facade (a registry the reorder buffer owner must
    /// publish so the output plugin can resolve the opaque handles in the
    /// `ApplyChange`/`ApplyTruncate`/`Message` callback variants). That facade is
    /// not modeled yet; those arms mirror-PG-and-panic in
    /// [`ReorderBuffer::apply_decoded_change`] / [`ReorderBuffer::apply_message`].
    #[allow(clippy::too_many_arguments)]
    fn process_txn_change(
        &mut self,
        txn_xid: TransactionId,
        streaming: bool,
        change: ReorderBufferChange,
        snapshot_now: &mut crate::SnapshotData,
        command_id: &mut CommandId,
        specinsert: &mut Option<ReorderBufferChange>,
    ) -> Result<(), types_error::PgError> {
        match change.action {
            ReorderBufferChangeType::InternalSpecConfirm
            | ReorderBufferChangeType::Insert
            | ReorderBufferChangeType::Update
            | ReorderBufferChangeType::Delete => {
                // SPEC_CONFIRM falls through to INSERT in C, reusing the stashed
                // specinsert; both reach the decoded-tuple apply path below.
                self.apply_decoded_change(txn_xid, &change, streaming);
                // (unreachable past the panic until the apply facade lands;
                // the specinsert free / RelationClose tail lives there too.)
                let _ = (specinsert, &change);
                Ok(())
            }
            ReorderBufferChangeType::InternalSpecInsert => {
                // Delay the insert until the confirm arrives: stash the change
                // (the C unlinks it from the chain so it isn't freed/reused).
                if let Some(prev) = specinsert.take() {
                    self.free_change(prev, true);
                }
                *specinsert = Some(change);
                Ok(())
            }
            ReorderBufferChangeType::InternalSpecAbort => {
                // Spec abort: clean the specinsert + toast hash (only the first
                // time, for the main table).
                if let Some(prev) = specinsert.take() {
                    self.toast_reset(txn_xid);
                    self.free_change(prev, true);
                }
                Ok(())
            }
            ReorderBufferChangeType::Truncate => {
                self.apply_decoded_change(txn_xid, &change, streaming);
                Ok(())
            }
            ReorderBufferChangeType::Message => {
                self.apply_message(txn_xid, &change, streaming);
                Ok(())
            }
            ReorderBufferChangeType::Invalidation => {
                if let ReorderBufferChangeData::Inval(msgs) = &change.data {
                    self.execute_invalidations(msgs);
                }
                Ok(())
            }
            ReorderBufferChangeType::InternalSnapshot => {
                let new_snap = match &change.data {
                    ReorderBufferChangeData::Snapshot(s) => s.clone(),
                    _ => unreachable!("INTERNAL_SNAPSHOT change carries a snapshot"),
                };
                backend_utils_time_snapmgr_seams::teardown_historic_snapshot::call(false);

                if snapshot_now.copied {
                    self.free_snap(snapshot_now.clone());
                    *snapshot_now = self.copy_snap(&new_snap, txn_xid, *command_id);
                } else if new_snap.copied {
                    *snapshot_now = self.copy_snap(&new_snap, txn_xid, *command_id);
                } else {
                    *snapshot_now = new_snap;
                }

                self.setup_historic_snapshot(snapshot_now, txn_xid);
                Ok(())
            }
            ReorderBufferChangeType::InternalCommandId => {
                let cid = match &change.data {
                    ReorderBufferChangeData::CommandId(c) => *c,
                    _ => unreachable!("INTERNAL_COMMAND_ID change carries a command id"),
                };
                debug_assert!(cid != types_core::xact::InvalidCommandId);

                if *command_id < cid {
                    *command_id = cid;

                    if !snapshot_now.copied {
                        *snapshot_now = self.copy_snap(snapshot_now, txn_xid, *command_id);
                    }
                    snapshot_now.curcid = *command_id;

                    backend_utils_time_snapmgr_seams::teardown_historic_snapshot::call(false);
                    self.setup_historic_snapshot(snapshot_now, txn_xid);
                }
                Ok(())
            }
            ReorderBufferChangeType::InternalTupleCid => {
                Err(types_error::PgError::error("tuplecid value in changequeue"))
            }
        }
    }

    /// The PG_CATCH handler of `ReorderBufferProcessTXN`
    /// (reorderbuffer.c:2723-2798). On the concurrent-abort error
    /// (`ERRCODE_TRANSACTION_ROLLBACK`) for a streamed/prepared txn the txn is
    /// reset so the remaining data can still stream; otherwise the error is
    /// re-thrown after cleanup (a panic here, since `process_txn` has no
    /// `PgResult` return — the C `PG_RE_THROW` unwinds the walsender).
    #[allow(clippy::too_many_arguments)]
    fn process_txn_catch(
        &mut self,
        txn_xid: TransactionId,
        _streaming: bool,
        using_subtxn: bool,
        snapshot_now: crate::SnapshotData,
        command_id: CommandId,
        prev_lsn: XLogRecPtr,
        specinsert: Option<ReorderBufferChange>,
        stream_started: bool,
        curtxn: Option<TransactionId>,
        errdata: types_error::PgError,
    ) {
        // iterstate already finished in the body's error path.

        backend_utils_time_snapmgr_seams::teardown_historic_snapshot::call(true);

        // Force cache invalidation outside a valid transaction.
        backend_access_transam_xact_seams::abort_current_transaction::call()
            .expect("AbortCurrentTransaction in PG_CATCH");

        self.process_txn_apply_inval(txn_xid)
            .expect("ReorderBufferExecuteInvalidations in PG_CATCH");

        if using_subtxn {
            backend_access_transam_xact_seams::rollback_and_release_current_sub_transaction::call()
                .expect("RollbackAndReleaseCurrentSubTransaction in PG_CATCH");
        }

        let prepared = self.with_txn_pub(txn_xid, |t| t.is_prepared());

        // Concurrent abort of the (sub)txn we are streaming/preparing: cleanup
        // and return gracefully.
        if errdata.sqlstate == types_error::error::ERRCODE_TRANSACTION_ROLLBACK
            && (stream_started || prepared)
        {
            let curtxn = curtxn.expect("curtxn set for streaming/prepared txns");
            debug_assert!(!self.with_txn_pub(curtxn, |t| t.is_committed()));
            self.with_txn_pub(curtxn, |t| t.txn_flags |= crate::RBTXN_IS_ABORTED);

            if stream_started {
                self.maybe_mark_txn_streamed(txn_xid);
            }

            self.reset_txn(txn_xid, snapshot_now, command_id, prev_lsn, specinsert);
        } else {
            self.cleanup_txn(txn_xid);
            // C PG_RE_THROW: the error propagates up the walsender. There is no
            // PgResult to carry it here, so we re-raise it as a panic (the
            // faithful "unwind" boundary for this value-typed entry point).
            panic!(
                "ReorderBufferProcessTXN: {} (SQLSTATE {:?})",
                errdata.message, errdata.sqlstate
            );
        }
    }

    /// `if (rbtxn_distr_inval_overflowed(txn)) InvalidateSystemCaches(); else {
    /// ReorderBufferExecuteInvalidations(txn->ninvalidations, ...);
    /// ReorderBufferExecuteInvalidations(txn->ninvalidations_distributed, ...); }`
    /// — the cache-pollution guard shared by the TRY tail and the CATCH handler.
    fn process_txn_apply_inval(&mut self, txn_xid: TransactionId) -> Result<(), types_error::PgError> {
        if self.with_txn_pub(txn_xid, |t| t.distr_inval_overflowed()) {
            debug_assert!(self.with_txn_pub(txn_xid, |t| t.invalidations_distributed.is_empty()));
            backend_utils_cache_inval_seams::invalidate_system_caches::call()?;
        } else {
            let invals = self.with_txn_pub(txn_xid, |t| t.invalidations.clone());
            self.execute_invalidations(&invals);
            let dist = self.with_txn_pub(txn_xid, |t| t.invalidations_distributed.clone());
            self.execute_invalidations(&dist);
        }
        Ok(())
    }

    /// `rb->commit(rb, txn, commit_lsn)`.
    fn commit_output(
        &mut self,
        xid: TransactionId,
        commit_lsn: XLogRecPtr,
    ) -> Result<(), types_error::PgError> {
        let (txn_final_lsn, txn_end_lsn) =
            self.with_txn_pub(xid, |t| (t.final_lsn, t.end_lsn));
        let cb = types_logical::ReorderBufferCallback::Commit {
            txn: crate::registry::txn_handle_for_xid(xid),
            txn_xid: xid,
            txn_final_lsn,
            txn_end_lsn,
            commit_lsn,
        };
        backend_replication_logical_logical_seams::dispatch_reorderbuffer_callback::call(cb)
    }

    /// `rb->prepare(rb, txn, commit_lsn)`.
    fn prepare_output(
        &mut self,
        xid: TransactionId,
        prepare_lsn: XLogRecPtr,
    ) -> Result<(), types_error::PgError> {
        let (txn_final_lsn, txn_end_lsn) =
            self.with_txn_pub(xid, |t| (t.final_lsn, t.end_lsn));
        let cb = types_logical::ReorderBufferCallback::Prepare {
            txn: crate::registry::txn_handle_for_xid(xid),
            txn_xid: xid,
            txn_final_lsn,
            txn_end_lsn,
            prepare_lsn,
        };
        backend_replication_logical_logical_seams::dispatch_reorderbuffer_callback::call(cb)
    }

    // -----------------------------------------------------------------------
    // ReorderBufferReplay / ReorderBufferCommit
    // -----------------------------------------------------------------------

    /// `ReorderBufferReplay(txn, rb, xid, commit_lsn, end_lsn, commit_time,
    /// origin_id, origin_lsn)` — replay a toplevel txn (and its subtxns).
    #[allow(clippy::too_many_arguments)]
    fn replay(
        &mut self,
        txn_xid: TransactionId,
        commit_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        commit_time: TimestampTz,
        origin_id: RepOriginId,
        origin_lsn: XLogRecPtr,
    ) {
        self.with_txn_pub(txn_xid, |t| {
            t.final_lsn = commit_lsn;
            t.end_lsn = end_lsn;
            t.xact_time = commit_time;
            t.origin_id = origin_id;
            t.origin_lsn = origin_lsn;
        });

        // A (partially) streamed txn commits the streamed way.
        if self.with_txn_pub(txn_xid, |t| t.is_streamed()) {
            self.stream_commit(txn_xid);
            return;
        }

        // No snapshot -> no changes to decode.
        if self.with_txn_pub(txn_xid, |t| t.base_snapshot.is_none()) {
            debug_assert!(self.with_txn_pub(txn_xid, |t| t.invalidations.is_empty()));
            if !self.with_txn_pub(txn_xid, |t| t.is_prepared()) {
                self.cleanup_txn(txn_xid);
            }
            return;
        }

        let snapshot_now = self
            .with_txn_pub(txn_xid, |t| t.base_snapshot.clone())
            .expect("base_snapshot present");

        self.process_txn(txn_xid, commit_lsn, snapshot_now, FirstCommandId, false);
    }

    /// `ReorderBufferCommit(rb, xid, commit_lsn, end_lsn, commit_time,
    /// origin_id, origin_lsn)`.
    #[allow(clippy::too_many_arguments)]
    pub fn commit(
        &mut self,
        xid: TransactionId,
        commit_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        commit_time: TimestampTz,
        origin_id: RepOriginId,
        origin_lsn: XLogRecPtr,
    ) {
        let txn_xid = match self.txn_by_xid_pub(xid, false, &mut None, InvalidXLogRecPtr, false) {
            None => return, // unknown transaction, nothing to replay
            Some(x) => x,
        };
        self.replay(txn_xid, commit_lsn, end_lsn, commit_time, origin_id, origin_lsn);
    }

    // -----------------------------------------------------------------------
    // ReorderBufferCleanupTXN / TruncateTXN / ResetTXN
    // -----------------------------------------------------------------------

    /// `ReorderBufferCleanupTXN(rb, txn)` — discard a transaction and all its
    /// resources after commit/abort.
    pub(crate) fn cleanup_txn(&mut self, txn_xid: TransactionId) {
        // cleanup subtransactions & their changes (never recurses >1 deep).
        let subtxns = self.with_txn_pub(txn_xid, |t| t.subtxns.clone());
        for sub_xid in subtxns {
            debug_assert!(self.with_txn_pub(sub_xid, |t| t.is_known_subxact()));
            debug_assert!(self.with_txn_pub(sub_xid, |t| t.nsubtxns == 0));
            self.cleanup_txn(sub_xid);
        }

        // cleanup changes in the txn (sum freed memory, update once below).
        let changes = self.with_txn_pub(txn_xid, |t| core::mem::take(&mut t.changes));
        let mut mem_freed = 0usize;
        for change in changes {
            mem_freed += crate::change_size(&change);
            self.free_change(change, false);
        }
        self.change_memory_update_sub_txn(txn_xid, mem_freed);

        // cleanup the tuplecids (catalog snapshot access).
        let tuplecids = self.with_txn_pub(txn_xid, |t| core::mem::take(&mut t.tuplecids));
        for change in tuplecids {
            debug_assert!(change.action == ReorderBufferChangeType::InternalTupleCid);
            self.free_change(change, true);
        }

        // cleanup the base snapshot, if set.
        if self.with_txn_pub(txn_xid, |t| t.base_snapshot.is_some()) {
            self.base_snapshot_dec_refcount(txn_xid);
            self.txns_by_base_snapshot_lsn_remove(txn_xid);
        }

        // cleanup the snapshot for the last streamed run.
        if self.with_txn_pub(txn_xid, |t| t.snapshot_now.is_some()) {
            debug_assert!(self.with_txn_pub(txn_xid, |t| t.is_streamed()));
            let snap = self.with_txn_pub(txn_xid, |t| t.snapshot_now.take()).unwrap();
            self.free_snap(snap);
        }

        // remove TXN from its containing lists.
        let (is_subxact, top_xid, has_cat) = self.with_txn_pub(txn_xid, |t| {
            (t.is_known_subxact(), t.toplevel_xid, t.has_catalog_changes())
        });
        if is_subxact {
            self.subtxns_remove(top_xid, txn_xid);
        } else {
            self.toplevel_by_lsn_remove(txn_xid);
        }
        if has_cat {
            self.catchange_remove(txn_xid);
        }

        // remove entries spilled to disk (spill family).
        if self.with_txn_pub(txn_xid, |t| t.is_serialized()) {
            self.restore_cleanup(txn_xid);
        }

        // now remove reference from buffer + deallocate.
        self.free_txn(txn_xid);
    }

    /// `ReorderBufferTruncateTXN(rb, txn, txn_prepared)` — discard a txn's
    /// changes (after streaming / PREPARE / abort) while keeping the txn shell,
    /// its invalidations, snapshot, and (unless prepared) its tuplecids.
    pub(crate) fn truncate_txn(&mut self, txn_xid: TransactionId, txn_prepared: bool) {
        // cleanup subtransactions & their changes.
        let subtxns = self.with_txn_pub(txn_xid, |t| t.subtxns.clone());
        for sub_xid in subtxns {
            debug_assert!(self.with_txn_pub(sub_xid, |t| t.is_known_subxact()));
            debug_assert!(self.with_txn_pub(sub_xid, |t| t.nsubtxns == 0));
            self.maybe_mark_txn_streamed(sub_xid);
            self.truncate_txn(sub_xid, txn_prepared);
        }

        // free changes, summing memory.
        let changes = self.with_txn_pub(txn_xid, |t| core::mem::take(&mut t.changes));
        let mut mem_freed = 0usize;
        for change in changes {
            mem_freed += crate::change_size(&change);
            self.free_change(change, false);
        }
        self.change_memory_update_sub_txn(txn_xid, mem_freed);

        if txn_prepared {
            let tuplecids = self.with_txn_pub(txn_xid, |t| core::mem::take(&mut t.tuplecids));
            for change in tuplecids {
                debug_assert!(change.action == ReorderBufferChangeType::InternalTupleCid);
                self.free_change(change, true);
            }
        }

        // destroy the tuplecid_hash if built.
        self.with_txn_pub(txn_xid, |t| t.tuplecid_hash = None);

        if self.with_txn_pub(txn_xid, |t| t.is_serialized()) {
            self.restore_cleanup(txn_xid);
            self.with_txn_pub(txn_xid, |t| {
                t.txn_flags &= !RBTXN_IS_SERIALIZED;
                t.txn_flags |= RBTXN_IS_SERIALIZED_CLEAR;
            });
        }
        self.with_txn_pub(txn_xid, |t| {
            t.nentries_mem = 0;
            t.nentries = 0;
        });
    }

    /// `ReorderBufferResetTXN(rb, txn, snapshot_now, command_id, last_lsn,
    /// specinsert)` — reset a streamed/prepared txn after a concurrent abort so
    /// the remaining data can still be streamed.
    ///
    /// Reached from `ReorderBufferProcessTXN`'s `PG_CATCH` concurrent-abort arm,
    /// which sits in the commit tail behind the still-unported output-plugin
    /// dispatch; the body itself is ported in full.
    #[allow(dead_code)]
    pub(crate) fn reset_txn(
        &mut self,
        txn_xid: TransactionId,
        snapshot_now: crate::SnapshotData,
        command_id: CommandId,
        last_lsn: XLogRecPtr,
        specinsert: Option<ReorderBufferChange>,
    ) {
        let prepared = self.with_txn_pub(txn_xid, |t| t.is_prepared());
        self.truncate_txn(txn_xid, prepared);
        self.toast_reset(txn_xid);

        if let Some(spec) = specinsert {
            self.free_change(spec, true);
        }

        if self.with_txn_pub(txn_xid, |t| t.is_streamed()) {
            self.stream_stop_output(txn_xid, last_lsn);
            self.save_txn_snapshot(txn_xid, snapshot_now, command_id);
        }
        debug_assert!(self.with_txn_pub(txn_xid, |t| t.size) == 0);
    }

    /// `ReorderBufferMaybeMarkTXNStreamed(rb, txn)`.
    pub(crate) fn maybe_mark_txn_streamed(&mut self, txn_xid: TransactionId) {
        let (is_top, nentries_mem) =
            self.with_txn_pub(txn_xid, |t| (!t.is_known_subxact(), t.nentries_mem));
        if is_top || nentries_mem != 0 {
            self.with_txn_pub(txn_xid, |t| t.txn_flags |= RBTXN_IS_STREAMED);
        }
    }

    /// `ReorderBufferSaveTXNSnapshot(rb, txn, snapshot_now, command_id)`.
    pub(crate) fn save_txn_snapshot(
        &mut self,
        txn_xid: TransactionId,
        snapshot_now: crate::SnapshotData,
        command_id: CommandId,
    ) {
        self.with_txn_pub(txn_xid, |t| t.command_id = command_id);
        if snapshot_now.copied {
            self.with_txn_pub(txn_xid, |t| t.snapshot_now = Some(snapshot_now));
        } else {
            let copied = self.copy_snap(&snapshot_now, txn_xid, command_id);
            self.with_txn_pub(txn_xid, |t| t.snapshot_now = Some(copied));
        }
    }

    // -----------------------------------------------------------------------
    // ReorderBufferFreeChange / FreeTXN
    // -----------------------------------------------------------------------

    /// `ReorderBufferFreeChange(rb, change, upd_mem)` — release one change's
    /// owned payload. The change is consumed by value (the C `pfree(change)`).
    pub(crate) fn free_change(&mut self, change: ReorderBufferChange, upd_mem: bool) {
        if upd_mem {
            let sz = crate::change_size(&change);
            self.change_memory_update_sub(sz);
        }
        // The owned payload (tuples, message bytes, invalidations, relids) drops
        // with `change`. An INTERNAL_SNAPSHOT change's snapshot is released via
        // free_snap to mirror ReorderBufferFreeSnap's refcount discipline; every
        // other payload simply drops with `change` here.
        if let ReorderBufferChangeData::Snapshot(snap) = change.data {
            self.free_snap(snap);
        }
    }

    /// `ReorderBufferFreeTXN(rb, txn)` — deallocate a txn shell after its
    /// resources are released, also clearing the one-entry lookup cache and
    /// removing it from `by_txn`.
    pub(crate) fn free_txn(&mut self, txn_xid: TransactionId) {
        self.invalidate_by_txn_cache(txn_xid);
        // gid / invalidations / invalidations_distributed / tuplecid_hash drop
        // with the removed txn; ReorderBufferToastReset frees any toast data.
        self.toast_reset(txn_xid);
        debug_assert!(self.by_txn_get(txn_xid).map(|t| t.size).unwrap_or(0) == 0);
        self.by_txn_remove(txn_xid);
    }

    // -----------------------------------------------------------------------
    // ReorderBufferExecuteInvalidations
    // -----------------------------------------------------------------------

    /// `ReorderBufferExecuteInvalidations(nmsgs, msgs)` — locally execute each
    /// accumulated shared-invalidation message.
    pub(crate) fn execute_invalidations(
        &self,
        msgs: &[types_storage::sinval::SharedInvalidationMessage],
    ) {
        for msg in msgs {
            backend_utils_cache_inval_seams::local_execute_invalidation_message::call(msg)
                .expect("LocalExecuteInvalidationMessage");
        }
    }
}

/// `ReorderBufferIterCompare(a, b, arg)` — heap comparator over entry LSNs.
/// `a`/`b` are `(lsn, off)` nodes; the smallest LSN must come out first, so we
/// invert the sign exactly as the C does (a max-heap API driven as a min-heap).
fn iter_compare(a: &IterNode, b: &IterNode) -> i32 {
    let (pos_a, _) = *a;
    let (pos_b, _) = *b;
    if pos_a < pos_b {
        1
    } else if pos_a == pos_b {
        0
    } else {
        -1
    }
}

#[allow(unused_imports)]
use InvalidTransactionId as _IgnoreInvalidTxn;
