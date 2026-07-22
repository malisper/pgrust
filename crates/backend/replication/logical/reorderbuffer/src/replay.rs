use std::rc::Rc;

use mcx::PgVec;
use snapmgr::Snapshot;
use types_core::{
    CommandId, FirstCommandId, InvalidCommandId, InvalidOid, InvalidTransactionId,
    InvalidXLogRecPtr, Oid, RepOriginId, TimestampTz, TransactionId, TransactionIdPrecedes,
    XLogRecPtr, RELPERSISTENCE_PERMANENT,
};
use types_error::PgResult;
use types_rel::{RelationData, RELKIND_SEQUENCE};
use types_snapshot::SnapshotData;
use types_storage::SharedInvalidationMessage;
use types_tuple::HeapTupleData;

use crate::iter::IterState;
use crate::visibility::{ReorderBufferTupleCidEnt, ReorderBufferTupleCidKey, TupleCidHash};
use crate::{
    dl_delete, dl_iter, rb_error, ChangeId, ListHead, ReorderBuffer,
    ReorderBufferChangeData, ReorderBufferChangeType::*, TxnId, RBTXN_IS_SERIALIZED,
    RBTXN_IS_SERIALIZED_CLEAR, RBTXN_IS_STREAMED, RBTXN_SENT_PREPARE,
};

pub(crate) fn relation_is_logically_logged(relation: &RelationData<'static>) -> bool {
    transam_xlog_seams::xlog_logical_info_active::call()
        && relation.rd_rel.relpersistence == RELPERSISTENCE_PERMANENT
        && !catalog::IsCatalogRelation(relation)
}

pub(crate) fn execute_invalidations(msgs: &[SharedInvalidationMessage]) -> PgResult<()> {
    for msg in msgs {
        inval::local::LocalExecuteInvalidationMessage(msg)?;
    }
    Ok(())
}

// SetupCheckXidLive (reorderbuffer.c:2048). While decoding a prepared (or,
// phase-2, streamed in-progress) transaction, its changes are replayed
// against the catalog using that transaction's own snapshot. If the
// transaction aborts concurrently (ROLLBACK PREPARED from another session),
// catalog rows it created can be vacuumed away mid-decode and the replay
// would read a wrong catalog version. Publishing the xid in CheckXidAlive
// makes every systable scan (genam handle_concurrent_abort) re-check that
// the xid is still alive and raise ERRCODE_TRANSACTION_ROLLBACK
// ("transaction aborted during system catalog scan") when it is not — the
// error process_txn's concurrent-abort arm converts into a clean stop.
pub(crate) fn setup_check_xid_live(xid: TransactionId) -> PgResult<()> {
    // Already set to this xid: nothing to do.
    if xact::CheckXidAlive() == xid {
        return Ok(());
    }

    // Set CheckXidAlive if the xid is not committed yet. We don't check
    // whether it aborted; that happens during catalog access.
    if !transam_seams::transaction_id_did_commit::call(xid)? {
        xact::SetCheckXidAlive(xid);
    } else {
        xact::SetCheckXidAlive(InvalidTransactionId);
    }
    Ok(())
}

// Unwind safety for the per-thread CheckXidAlive/bsysscan state. C needs no
// guard: its error unwind always funnels through AbortTransaction /
// AbortSubTransaction, which call ResetLogicalStreamingState (xact.c:2902,
// :5297). Our Err path takes the same route (process_txn's error arm calls
// xact::AbortCurrentTransaction), but a Rust panic unwinds past it — and in
// the threaded server the thread outlives a caught panic, so a leaked xid
// would poison every later catalog scan on that thread with spurious
// "transaction aborted during system catalog scan" errors. Dropping this
// guard re-runs the reset; it is idempotent when the state is already clean.
pub(crate) struct CheckXidLiveGuard;

impl Drop for CheckXidLiveGuard {
    fn drop(&mut self) {
        xact::SetCheckXidAlive(InvalidTransactionId);
        xact::SetBsysscan(false);
    }
}

impl ReorderBuffer {
    pub fn change_size(&self, id: ChangeId) -> usize {
        let change = self.change(id);
        // Sizes use this build's struct layouts where C uses its own sizeof.
        let mut sz = std::mem::size_of::<crate::ReorderBufferChange>();
        match (&change.action, &change.data) {
            (
                Insert | Update | Delete | InternalSpecInsert,
                ReorderBufferChangeData::Tp { oldtuple, newtuple, .. },
            ) => {
                if let Some(t) = oldtuple {
                    sz += std::mem::size_of::<HeapTupleData>() + t.t_len as usize;
                }
                if let Some(t) = newtuple {
                    sz += std::mem::size_of::<HeapTupleData>() + t.t_len as usize;
                }
            }
            (Message, ReorderBufferChangeData::Msg { prefix, message }) => {
                sz += prefix.len() + 1
                    + message.len()
                    + std::mem::size_of::<usize>()
                    + std::mem::size_of::<usize>();
            }
            (Invalidation, ReorderBufferChangeData::Inval { invalidations }) => {
                sz += std::mem::size_of::<SharedInvalidationMessage>() * invalidations.len();
            }
            (InternalSnapshot, ReorderBufferChangeData::Snapshot(snap)) => {
                sz += std::mem::size_of::<SnapshotData>()
                    + std::mem::size_of::<TransactionId>() * snap.xcnt as usize
                    + std::mem::size_of::<TransactionId>() * snap.subxcnt.max(0) as usize;
            }
            (Truncate, ReorderBufferChangeData::Truncate { relids, .. }) => {
                sz += std::mem::size_of::<Oid>() * relids.len();
            }
            _ => {}
        }
        sz
    }

    pub(crate) fn change_memory_update(
        &mut self,
        change: Option<ChangeId>,
        txn: Option<TxnId>,
        addition: bool,
        sz: usize,
    ) {
        debug_assert!(txn.is_some() || change.is_some());

        if let Some(cid) = change {
            if self.change(cid).action == InternalTupleCid {
                return;
            }
        }
        if sz == 0 {
            return;
        }

        let txn = txn.unwrap_or_else(|| self.change(change.expect("change set")).txn);
        let toptxn = self.toptxn_id(txn);

        // C additionally maintains rb->txn_heap here; the max-heap only feeds
        // eviction, which this port selects by scan at limit-check time
        // (spill.rs largest_txn).
        if addition {
            self.txn_mut(txn).size += sz;
            self.size += sz;
            // wrapping_add: total_size can sit wrapped-negative after the
            // subtraction arm below (see its comment); C's unsigned Size adds
            // straight through.
            let t = self.txn_mut(toptxn);
            t.total_size = t.total_size.wrapping_add(sz);
        } else {
            debug_assert!(self.size >= sz && self.txn(txn).size >= sz);
            self.txn_mut(txn).size -= sz;
            self.size -= sz;
            // C's unsigned Size wraps here (pre-assignment subtxn bytes were
            // counted on the old top); keep the same arithmetic.
            let t = self.txn_mut(toptxn);
            t.total_size = t.total_size.wrapping_sub(sz);
        }
        debug_assert!(self.txn(txn).size <= self.size);
    }

    pub(crate) fn copy_snap(&self, orig: &Snapshot, txn: TxnId, cid: CommandId) -> Snapshot {
        let mut snap = SnapshotData::sentinel(self.mcx, orig.snapshot_type);
        snap.xmin = orig.xmin;
        snap.xmax = orig.xmax;
        let mut xip = PgVec::new_in(self.mcx);
        xip.extend_from_slice(&orig.xip[..orig.xcnt as usize]);
        snap.xip = xip;
        snap.xcnt = orig.xcnt;
        snap.suboverflowed = orig.suboverflowed;
        snap.takenDuringRecovery = orig.takenDuringRecovery;
        snap.speculativeToken = orig.speculativeToken;
        snap.vistest = orig.vistest;
        snap.snapXactCompletionCount = orig.snapXactCompletionCount;
        snap.copied = true;
        snap.active_count.set(1);
        snap.regd_count.set(0);

        // subxip holds every xid of this transaction tree (cmin/cmax checks).
        let mut subxip = PgVec::new_in(self.mcx);
        subxip.push(self.txn(txn).xid);
        for sub in dl_iter(&self.txns, self.txn(txn).subtxns, |t| t.node) {
            subxip.push(self.txn(sub).xid);
        }
        subxip.sort_unstable();
        snap.subxcnt = subxip.len() as i32;
        snap.subxip = subxip;
        snap.curcid.set(cid);
        Rc::new(snap)
    }

    pub(crate) fn build_tuplecid_hash(&mut self, txn: TxnId) {
        if !self.txn(txn).has_catalog_changes() || self.txn(txn).tuplecids.is_empty() {
            return;
        }
        let mut hash: TupleCidHash =
            mcx::PgFxHashMap::with_hasher_in(Default::default(), self.mcx);
        for cid in dl_iter(&self.changes, self.txn(txn).tuplecids, |c| c.node) {
            let change = self.change(cid);
            debug_assert_eq!(change.action, InternalTupleCid);
            let ReorderBufferChangeData::TupleCid { locator, tid, cmin, cmax, combocid } =
                &change.data
            else {
                unreachable!("tuplecid change carries TupleCid data");
            };
            let key = ReorderBufferTupleCidKey { rlocator: *locator, tid: *tid };
            if let Some(ent) = hash.get_mut(&key) {
                debug_assert_eq!(ent.cmin, *cmin);
                debug_assert!(
                    ent.cmax == InvalidCommandId
                        || (*cmax != InvalidCommandId && *cmax > ent.cmax)
                );
                ent.cmax = *cmax;
            } else {
                hash.insert(
                    key,
                    ReorderBufferTupleCidEnt { cmin: *cmin, cmax: *cmax, combocid: *combocid },
                );
            }
        }
        self.txn_mut(txn).tuplecid_hash =
            Some(Rc::new(std::cell::RefCell::new(hash)));
    }

    pub(crate) fn cleanup_txn(&mut self, txn: TxnId) -> PgResult<()> {
        let subs: Vec<TxnId> = dl_iter(&self.txns, self.txn(txn).subtxns, |t| t.node).collect();
        for sub in subs {
            debug_assert!(self.txn(sub).is_known_subxact());
            debug_assert_eq!(self.txn(sub).nsubtxns, 0);
            self.cleanup_txn(sub)?;
        }

        let mut mem_freed = 0usize;
        let changes: Vec<ChangeId> =
            dl_iter(&self.changes, self.txn(txn).changes, |c| c.node).collect();
        self.txn_mut(txn).changes = ListHead::EMPTY;
        for cid in changes {
            debug_assert_eq!(self.change(cid).txn, txn);
            mem_freed += self.change_size(cid);
            self.free_change(cid, false);
        }
        self.change_memory_update(None, Some(txn), false, mem_freed);

        let tuplecids: Vec<ChangeId> =
            dl_iter(&self.changes, self.txn(txn).tuplecids, |c| c.node).collect();
        self.txn_mut(txn).tuplecids = ListHead::EMPTY;
        for cid in tuplecids {
            debug_assert_eq!(self.change(cid).txn, txn);
            debug_assert_eq!(self.change(cid).action, InternalTupleCid);
            self.free_change(cid, true);
        }

        if self.txn(txn).base_snapshot.is_some() {
            self.txn_mut(txn).base_snapshot = None;
            let mut list = self.txns_by_base_snapshot_lsn;
            dl_delete(&mut self.txns, &mut list, txn, |t| &mut t.base_snapshot_node);
            self.txns_by_base_snapshot_lsn = list;
        }

        if self.txn(txn).snapshot_now.is_some() {
            debug_assert!(self.txn(txn).is_streamed());
            self.txn_mut(txn).snapshot_now = None;
        }

        if self.txn(txn).is_known_subxact() {
            let parent = self.txn(txn).toptxn;
            let mut list = self.txn(parent).subtxns;
            dl_delete(&mut self.txns, &mut list, txn, |t| &mut t.node);
            self.txn_mut(parent).subtxns = list;
        } else {
            let mut list = self.toplevel_by_lsn;
            dl_delete(&mut self.txns, &mut list, txn, |t| &mut t.node);
            self.toplevel_by_lsn = list;
        }
        if self.txn(txn).has_catalog_changes() {
            let mut list = self.catchange_txns;
            dl_delete(&mut self.txns, &mut list, txn, |t| &mut t.catchange_node);
            self.catchange_txns = list;
            self.catchange_count -= 1;
        }

        let xid = self.txn(txn).xid;
        let removed = self.by_txn.remove(&xid);
        debug_assert!(removed.is_some());

        // Remove entries spilled to disk.
        if self.txn(txn).is_serialized() {
            self.restore_cleanup(txn)?;
        }
        self.free_txn(txn);
        Ok(())
    }

    pub(crate) fn truncate_txn(&mut self, txn: TxnId, txn_prepared: bool) -> PgResult<()> {
        let subs: Vec<TxnId> = dl_iter(&self.txns, self.txn(txn).subtxns, |t| t.node).collect();
        for sub in subs {
            debug_assert!(self.txn(sub).is_known_subxact());
            debug_assert_eq!(self.txn(sub).nsubtxns, 0);
            self.maybe_mark_txn_streamed(sub);
            self.truncate_txn(sub, txn_prepared)?;
        }

        let mut mem_freed = 0usize;
        let changes: Vec<ChangeId> =
            dl_iter(&self.changes, self.txn(txn).changes, |c| c.node).collect();
        self.txn_mut(txn).changes = ListHead::EMPTY;
        for cid in changes {
            debug_assert_eq!(self.change(cid).txn, txn);
            mem_freed += self.change_size(cid);
            self.free_change(cid, false);
        }
        self.change_memory_update(None, Some(txn), false, mem_freed);

        if txn_prepared {
            let tuplecids: Vec<ChangeId> =
                dl_iter(&self.changes, self.txn(txn).tuplecids, |c| c.node).collect();
            self.txn_mut(txn).tuplecids = ListHead::EMPTY;
            for cid in tuplecids {
                debug_assert_eq!(self.change(cid).txn, txn);
                debug_assert_eq!(self.change(cid).action, InternalTupleCid);
                self.free_change(cid, true);
            }
        }

        self.txn_mut(txn).tuplecid_hash = None;

        // If this txn is serialized then clean the disk space.
        if self.txn(txn).is_serialized() {
            self.restore_cleanup(txn)?;
            self.txn_mut(txn).txn_flags &= !RBTXN_IS_SERIALIZED;
            // Remember the transaction was ever serialized so the spill
            // statistics don't count it twice.
            self.txn_mut(txn).txn_flags |= RBTXN_IS_SERIALIZED_CLEAR;
        }

        self.txn_mut(txn).nentries_mem = 0;
        self.txn_mut(txn).nentries = 0;
        Ok(())
    }

    pub(crate) fn maybe_mark_txn_streamed(&mut self, txn: TxnId) {
        if self.txn(txn).is_toptxn() || self.txn(txn).nentries_mem != 0 {
            self.txn_mut(txn).txn_flags |= RBTXN_IS_STREAMED;
        }
    }

    pub fn commit(
        &mut self,
        xid: TransactionId,
        commit_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        commit_time: TimestampTz,
        origin_id: RepOriginId,
        origin_lsn: XLogRecPtr,
    ) -> PgResult<()> {
        let Some(txn) = self.txn_by_xid(xid, false, InvalidXLogRecPtr, false).0 else {
            return Ok(());
        };
        self.replay(txn, commit_lsn, end_lsn, commit_time, origin_id, origin_lsn)
    }

    pub(crate) fn replay(
        &mut self,
        txn: TxnId,
        commit_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        commit_time: TimestampTz,
        origin_id: RepOriginId,
        origin_lsn: XLogRecPtr,
    ) -> PgResult<()> {
        {
            let t = self.txn_mut(txn);
            t.final_lsn = commit_lsn;
            t.end_lsn = end_lsn;
            t.xact_time = commit_time;
            t.origin_id = origin_id;
            t.origin_lsn = origin_lsn;
        }

        // A (partially) streamed transaction commits in the streamed way:
        // stream the remaining part, then send stream_commit/stream_prepare
        // (reorderbuffer.c:2833).
        if self.txn(txn).is_streamed() {
            return self.stream_commit(txn);
        }

        if self.txn(txn).base_snapshot.is_none() {
            debug_assert!(self.txn(txn).invalidations.is_empty());
            if !self.txn(txn).is_prepared() {
                self.cleanup_txn(txn)?;
            }
            return Ok(());
        }

        let snapshot_now = self.txn(txn).base_snapshot.clone().expect("base snapshot");
        self.process_txn(txn, commit_lsn, snapshot_now, FirstCommandId, false)
    }

    pub(crate) fn process_txn(
        &mut self,
        txn: TxnId,
        commit_lsn: XLogRecPtr,
        snapshot_now: Snapshot,
        command_id: CommandId,
        streaming: bool,
    ) -> PgResult<()> {
        self.build_tuplecid_hash(txn);
        snapmgr::SetupHistoricSnapshot(snapshot_now.clone(), self.tuplecid_hash_any(txn));

        let using_subtxn = xact::IsTransactionOrTransactionBlock();

        let mut iterstate: Option<IterState> = None;
        let mut specinsert: Option<ChangeId> = None;
        let mut snapshot_now = snapshot_now;
        let mut command_id = command_id;
        let mut curtxn: Option<TxnId> = None;
        let mut stream_started = false;
        let mut prev_lsn = InvalidXLogRecPtr;

        // Reset CheckXidAlive/bsysscan even if a panic unwinds out of the
        // replay (see the guard's comment). Both normal exits already leave
        // the state clean: the success tail resets CheckXidAlive after
        // truncating a prepared txn (reorderbuffer.c:2718), and the Err arm's
        // AbortCurrentTransaction resets both via ResetLogicalStreamingState.
        let _check_xid_guard = CheckXidLiveGuard;

        let result = self.process_txn_guts(
            txn,
            commit_lsn,
            &mut snapshot_now,
            &mut command_id,
            using_subtxn,
            &mut iterstate,
            &mut specinsert,
            &mut curtxn,
            streaming,
            &mut stream_started,
            &mut prev_lsn,
        );

        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                if let Some(state) = iterstate.take() {
                    self.iter_txn_finish(state);
                }
                snapmgr::TeardownHistoricSnapshot(true);
                xact::AbortCurrentTransaction()?;

                if self.txn(txn).distr_inval_overflowed() {
                    debug_assert!(self.txn(txn).invalidations_distributed.is_empty());
                    inval::local::InvalidateSystemCaches()?;
                } else {
                    execute_invalidations(&self.txn(txn).invalidations)?;
                    execute_invalidations(&self.txn(txn).invalidations_distributed)?;
                }

                if using_subtxn {
                    xact::RollbackAndReleaseCurrentSubTransaction()?;
                }

                // ERRCODE_TRANSACTION_ROLLBACK signals a concurrent abort of
                // the (sub)transaction being streamed or prepared; clean up
                // and return gracefully so streaming can continue with the
                // remaining data / the caller (ReorderBufferPrepare) can
                // still send the prepare (reorderbuffer.c:2777). The error
                // reaches us either from a catalog scan that tripped on
                // CheckXidAlive (genam handle_concurrent_abort) or from the
                // plugin itself, exactly C's two sources. By this point
                // AbortCurrentTransaction has already reset CheckXidAlive
                // (ResetLogicalStreamingState), matching C's PG_CATCH
                // ordering.
                if e.sqlstate == types_error::ERRCODE_TRANSACTION_ROLLBACK
                    && (stream_started || self.txn(txn).is_prepared())
                {
                    // curtxn must be set for streamed or prepared replay.
                    let cur = curtxn.expect("current txn tracked for streamed/prepared replay");
                    debug_assert!(!self.txn(cur).is_committed());
                    self.txn_mut(cur).txn_flags |= crate::RBTXN_IS_ABORTED;

                    if stream_started {
                        self.maybe_mark_txn_streamed(txn);
                    }

                    // ReorderBufferResetTXN: discard the decoded changes so
                    // the txn can stream its remaining data / carry its
                    // prepared identity to the finish.
                    let prepared = self.txn(txn).is_prepared();
                    self.truncate_txn(txn, prepared)?;
                    self.toast_reset(txn);
                    if let Some(si) = specinsert.take() {
                        self.free_change(si, true);
                    }
                    // For the streaming case, stop the stream and remember
                    // the command ID and snapshot for the next run.
                    if self.txn(txn).is_streamed() {
                        let cb = self
                            .callbacks
                            .stream_stop
                            .expect("streamed replay requires the stream_stop callback");
                        cb(self, txn, prev_lsn)?;
                        self.save_txn_snapshot(txn, &snapshot_now, command_id);
                    }
                    debug_assert_eq!(self.txn(txn).size, 0);
                    return Ok(());
                }

                self.cleanup_txn(txn)?;
                Err(e)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn process_txn_guts(
        &mut self,
        txn: TxnId,
        commit_lsn: XLogRecPtr,
        snapshot_now: &mut Snapshot,
        command_id: &mut CommandId,
        using_subtxn: bool,
        iterstate: &mut Option<IterState>,
        specinsert: &mut Option<ChangeId>,
        curtxn: &mut Option<TxnId>,
        streaming: bool,
        stream_started: &mut bool,
        prev_lsn: &mut XLogRecPtr,
    ) -> PgResult<()> {
        let mut changes_count = 0u32;

        if using_subtxn {
            xact::BeginInternalSubTransaction(Some(if streaming { "stream" } else { "replay" }))?;
        } else {
            xact::StartTransactionCommand()?;
        }

        // Only send begin/begin-prepare for non-streamed transactions.
        if !streaming {
            if self.txn(txn).is_prepared() {
                { let cb = self.callbacks.begin_prepare; cb(self, txn)?; }
            } else {
                { let cb = self.callbacks.begin; cb(self, txn)?; }
            }
        }

        self.iter_txn_init(txn, iterstate)?;
        loop {
            let cur = {
                let state = iterstate.as_mut().expect("iterator initialized");
                self.iter_txn_next(state)?
            };
            let Some(cur) = cur else {
                break;
            };

            // The start-stream callback can only fire once the first change
            // is at hand (reorderbuffer.c:2273).
            if *prev_lsn == InvalidXLogRecPtr && streaming {
                let (origin_id, first_lsn) = {
                    let c = self.change(cur);
                    (c.origin_id, c.lsn)
                };
                self.txn_mut(txn).origin_id = origin_id;
                let cb = self
                    .callbacks
                    .stream_start
                    .expect("streamed replay requires the stream_start callback");
                cb(self, txn, first_lsn)?;
                *stream_started = true;
            }

            debug_assert!(*prev_lsn == InvalidXLogRecPtr || *prev_lsn <= self.change(cur).lsn);
            *prev_lsn = self.change(cur).lsn;

            // Set the current xid to detect concurrent aborts, required when
            // changes are decoded before the COMMIT record is processed
            // (reorderbuffer.c:2300).
            let change_txn = self.change(cur).txn;
            if streaming || self.txn(change_txn).is_prepared() {
                *curtxn = Some(change_txn);
                setup_check_xid_live(self.txn(change_txn).xid)?;
            }

            let action = self.change(cur).action;
            match action {
                InternalSpecConfirm | Insert | Update | Delete => {
                    let mut work = cur;
                    if action == InternalSpecConfirm {
                        let Some(si) = *specinsert else {
                            return Err(rb_error(
                                "invalid ordering of speculative insertion changes".into(),
                            ));
                        };
                        self.change_mut(si).action = Insert;
                        work = si;
                    }
                    self.apply_tuple_change(txn, work, specinsert, iterstate, streaming)?;
                }
                InternalSpecInsert => {
                    if let Some(prev) = specinsert.take() {
                        self.free_change(prev, true);
                    }
                    let state = iterstate.as_mut().expect("iterator initialized");
                    self.iter_extract_change(state, cur);
                    *specinsert = Some(cur);
                }
                InternalSpecAbort => {
                    if let Some(si) = specinsert.take() {
                        debug_assert!(matches!(
                            self.change(cur).data,
                            ReorderBufferChangeData::Tp { clear_toast_afterwards: true, .. }
                        ));
                        self.toast_reset(txn);
                        self.free_change(si, true);
                    }
                }
                Truncate => {
                    let relids: Vec<Oid> = match &self.change(cur).data {
                        ReorderBufferChangeData::Truncate { relids, .. } => {
                            relids.iter().copied().collect()
                        }
                        _ => unreachable!("truncate change carries Truncate data"),
                    };
                    let mut relations: Vec<Rc<RelationData<'static>>> = Vec::new();
                    for relid in relids {
                        let rel = relcache::RelationIdGetRelation(relid)?.ok_or_else(|| {
                            rb_error(format!("could not open relation with OID {relid}"))
                        })?;
                        if !relation_is_logically_logged(&rel) {
                            continue;
                        }
                        relations.push(rel);
                    }
                    let mut change = self.changes[cur as usize].take().expect("live change");
                    // ReorderBufferApplyTruncate (reorderbuffer.c:2085).
                    let cb = if streaming {
                        self.callbacks
                            .stream_truncate
                            .expect("streamed replay requires the stream_truncate callback")
                    } else {
                        self.callbacks.apply_truncate
                    };
                    let r = cb(self, txn, &relations, &mut change);
                    self.changes[cur as usize] = Some(change);
                    r?;
                }
                Message => {
                    self.apply_message(txn, cur, streaming)?;
                }
                Invalidation => {
                    let change = self.changes[cur as usize].take().expect("live change");
                    let r = match &change.data {
                        ReorderBufferChangeData::Inval { invalidations } => {
                            execute_invalidations(invalidations)
                        }
                        _ => unreachable!("invalidation change carries Inval data"),
                    };
                    self.changes[cur as usize] = Some(change);
                    r?;
                }
                InternalSnapshot => {
                    snapmgr::TeardownHistoricSnapshot(false);
                    let new_snap = match &self.change(cur).data {
                        ReorderBufferChangeData::Snapshot(s) => s.clone(),
                        _ => unreachable!("snapshot change carries Snapshot data"),
                    };
                    if snapshot_now.copied || new_snap.copied {
                        *snapshot_now = self.copy_snap(&new_snap, txn, *command_id);
                    } else {
                        *snapshot_now = new_snap;
                    }
                    snapmgr::SetupHistoricSnapshot(
                        snapshot_now.clone(),
                        self.tuplecid_hash_any(txn),
                    );
                }
                InternalCommandId => {
                    let new_cid = match self.change(cur).data {
                        ReorderBufferChangeData::CommandId(c) => c,
                        _ => unreachable!("command-id change carries CommandId data"),
                    };
                    debug_assert!(new_cid != InvalidCommandId);
                    if *command_id < new_cid {
                        *command_id = new_cid;
                        if !snapshot_now.copied {
                            *snapshot_now = self.copy_snap(snapshot_now, txn, *command_id);
                        }
                        snapshot_now.curcid.set(*command_id);
                        snapmgr::TeardownHistoricSnapshot(false);
                        snapmgr::SetupHistoricSnapshot(
                            snapshot_now.clone(),
                            self.tuplecid_hash_any(txn),
                        );
                    }
                }
                InternalTupleCid => {
                    return Err(rb_error("tuplecid value in changequeue".into()));
                }
            }

            changes_count += 1;
            if changes_count >= 100 {
                let cb = self.callbacks.update_progress_txn;
                cb(self, txn, *prev_lsn)?;
                changes_count = 0;
            }
        }

        debug_assert!(specinsert.is_none());

        let state = iterstate.take().expect("iterator initialized");
        self.iter_txn_finish(state);

        if !self.txn(txn).is_streamed() {
            self.totalTxns += 1;
        }
        self.totalBytes += self.txn(txn).total_size as i64;

        // Done with the current changes: send the closing message for this
        // set depending on the mode (stream_stop vs prepare/commit).
        if streaming {
            if *stream_started {
                let cb = self
                    .callbacks
                    .stream_stop
                    .expect("streamed replay requires the stream_stop callback");
                cb(self, txn, *prev_lsn)?;
                *stream_started = false;
            }
        } else if self.txn(txn).is_prepared() {
            debug_assert!(!self.txn(txn).sent_prepare());
            let cb = self.callbacks.prepare;
            cb(self, txn, commit_lsn)?;
            self.txn_mut(txn).txn_flags |= RBTXN_SENT_PREPARE;
        } else {
            let cb = self.callbacks.commit;
            cb(self, txn, commit_lsn)?;
        }

        if xact::GetCurrentTransactionIdIfAny() != InvalidTransactionId {
            return Err(rb_error(format!(
                "output plugin used XID {}",
                xact::GetCurrentTransactionIdIfAny()
            )));
        }

        // Remember the command ID and snapshot for the next set of changes
        // in streaming mode.
        if streaming {
            self.save_txn_snapshot(txn, snapshot_now, *command_id);
        }

        snapmgr::TeardownHistoricSnapshot(false);
        xact::AbortCurrentTransaction()?;

        if self.txn(txn).distr_inval_overflowed() {
            debug_assert!(self.txn(txn).invalidations_distributed.is_empty());
            inval::local::InvalidateSystemCaches()?;
        } else {
            execute_invalidations(&self.txn(txn).invalidations)?;
            execute_invalidations(&self.txn(txn).invalidations_distributed)?;
        }

        if using_subtxn {
            xact::RollbackAndReleaseCurrentSubTransaction()?;
        }

        // In-progress (streamed) and prepared transactions keep their
        // identity — truncate the decoded changes; a fully decoded committed
        // transaction cleans up entirely (reorderbuffer.c:2700).
        if streaming || self.txn(txn).is_prepared() {
            if streaming {
                self.maybe_mark_txn_streamed(txn);
            }
            let prepared = self.txn(txn).is_prepared();
            self.truncate_txn(txn, prepared)?;
            // Reset the CheckXidAlive (reorderbuffer.c:2718).
            xact::SetCheckXidAlive(InvalidTransactionId);
        } else {
            self.cleanup_txn(txn)?;
        }
        Ok(())
    }

    fn apply_tuple_change(
        &mut self,
        txn: TxnId,
        work: ChangeId,
        specinsert: &mut Option<ChangeId>,
        iterstate: &mut Option<IterState>,
        streaming: bool,
    ) -> PgResult<()> {
        let (rlocator, has_old, has_new, clear_toast) = match &self.change(work).data {
            ReorderBufferChangeData::Tp {
                rlocator,
                clear_toast_afterwards,
                oldtuple,
                newtuple,
            } => (
                *rlocator,
                oldtuple.is_some(),
                newtuple.is_some(),
                *clear_toast_afterwards,
            ),
            _ => unreachable!("tuple change carries Tp data"),
        };

        let reloid = relfilenumbermap_seams::relid_by_relfilenumber::call(
            rlocator.spcOid,
            rlocator.relNumber,
        )?;

        // Mapped catalog tuple without data, emitted mid-rewrite: skippable.
        let relation = if reloid == InvalidOid && !has_new && !has_old {
            None
        } else if reloid == InvalidOid {
            return Err(rb_error(format!(
                "could not map filenumber \"{}/{}/{}\" to relation OID",
                rlocator.spcOid, rlocator.dbOid, rlocator.relNumber
            )));
        } else {
            Some(relcache::RelationIdGetRelation(reloid)?.ok_or_else(|| {
                rb_error(format!("could not open relation with OID {reloid}"))
            })?)
        };

        if let Some(relation) = &relation {
            // rd_rel.relrewrite is not carried by this build's trimmed form;
            // transient rewrite heaps ride the logical-rewrite path (phase-2).
            if relation_is_logically_logged(relation)
                && relation.rd_rel.relkind != RELKIND_SEQUENCE
            {
                if !catalog::IsToastRelation(relation) {
                    self.toast_replace(txn, relation, work)?;
                    let mut change = self.changes[work as usize].take().expect("live change");
                    // ReorderBufferApplyChange (reorderbuffer.c:2072).
                    let cb = if streaming {
                        self.callbacks
                            .stream_change
                            .expect("streamed replay requires the stream_change callback")
                    } else {
                        self.callbacks.apply_change
                    };
                    let r = cb(self, txn, relation, &mut change);
                    self.changes[work as usize] = Some(change);
                    r?;
                    if clear_toast {
                        self.toast_reset(txn);
                    }
                } else if self.change(work).action == Insert {
                    debug_assert!(has_new);
                    debug_assert!(specinsert.is_none(), "spec-insert into a toast relation");
                    let state = iterstate.as_mut().expect("iterator initialized");
                    self.iter_extract_change(state, work);
                    self.toast_append_chunk(txn, relation, work)?;
                }
            }
        }

        if let Some(si) = specinsert.take() {
            self.free_change(si, true);
        }
        Ok(())
    }

    fn apply_message(&mut self, txn: TxnId, cur: ChangeId, streaming: bool) -> PgResult<()> {
        let change = self.changes[cur as usize].take().expect("live change");
        let lsn = change.lsn;
        // ReorderBufferApplyMessage (reorderbuffer.c:2098).
        let cb = if streaming {
            self.callbacks
                .stream_message
                .expect("streamed replay requires the stream_message callback")
        } else {
            self.callbacks.message
        };
        let r = match &change.data {
            ReorderBufferChangeData::Msg { prefix, message } => {
                cb(self, Some(txn), lsn, true, prefix.as_str(), message)
            }
            _ => unreachable!("message change carries Msg data"),
        };
        self.changes[cur as usize] = Some(change);
        r
    }

    pub fn abort(
        &mut self,
        xid: TransactionId,
        lsn: XLogRecPtr,
        abort_time: TimestampTz,
    ) -> PgResult<()> {
        let Some(txn) = self.txn_by_xid(xid, false, InvalidXLogRecPtr, false).0 else {
            return Ok(());
        };
        self.txn_mut(txn).xact_time = abort_time;

        // For streamed transactions notify the remote node about the abort,
        // and run this txn's own invalidations so future decoding doesn't
        // reuse cache entries loaded under its (DDL) view
        // (reorderbuffer.c:2874).
        if self.txn(txn).is_streamed() {
            let cb = self
                .callbacks
                .stream_abort
                .expect("streamed abort requires the stream_abort callback");
            cb(self, txn, lsn)?;

            if !self.txn(txn).invalidations.is_empty() {
                let invals = std::mem::take(&mut self.txn_mut(txn).invalidations);
                self.immediate_invalidation(&invals)?;
                self.txn_mut(txn).invalidations = invals;
            }
        }

        self.txn_mut(txn).final_lsn = lsn;
        self.cleanup_txn(txn)?;
        Ok(())
    }

    pub fn abort_old(&mut self, oldest_running_xid: TransactionId) -> PgResult<()> {
        loop {
            let head = self.toplevel_by_lsn.head;
            if head == crate::INVALID_ID {
                return Ok(());
            }
            let txn = head;
            if TransactionIdPrecedes(self.txn(txn).xid, oldest_running_xid) {
                // Notify the remote node about the crash/immediate restart.
                if self.txn(txn).is_streamed() {
                    let cb = self
                        .callbacks
                        .stream_abort
                        .expect("streamed abort requires the stream_abort callback");
                    cb(self, txn, InvalidXLogRecPtr)?;
                }
                self.cleanup_txn(txn)?;
            } else {
                return Ok(());
            }
        }
    }

    pub fn forget(&mut self, xid: TransactionId, lsn: XLogRecPtr) -> PgResult<()> {
        let Some(txn) = self.txn_by_xid(xid, false, InvalidXLogRecPtr, false).0 else {
            return Ok(());
        };
        debug_assert!(!self.txn(txn).is_streamed());
        self.txn_mut(txn).final_lsn = lsn;

        if self.txn(txn).base_snapshot.is_some() && !self.txn(txn).invalidations.is_empty() {
            let invals = std::mem::take(&mut self.txn_mut(txn).invalidations);
            self.immediate_invalidation(&invals)?;
            self.txn_mut(txn).invalidations = invals;
        } else {
            debug_assert!(self.txn(txn).invalidations.is_empty());
        }

        self.cleanup_txn(txn)?;
        Ok(())
    }

    pub fn invalidate(&mut self, xid: TransactionId, _lsn: XLogRecPtr) -> PgResult<()> {
        let Some(txn) = self.txn_by_xid(xid, false, InvalidXLogRecPtr, false).0 else {
            return Ok(());
        };
        if self.txn(txn).base_snapshot.is_some() && !self.txn(txn).invalidations.is_empty() {
            let invals = std::mem::take(&mut self.txn_mut(txn).invalidations);
            self.immediate_invalidation(&invals)?;
            self.txn_mut(txn).invalidations = invals;
        } else {
            debug_assert!(self.txn(txn).invalidations.is_empty());
        }
        Ok(())
    }

    pub fn immediate_invalidation(
        &mut self,
        invalidations: &[SharedInvalidationMessage],
    ) -> PgResult<()> {
        let use_subtxn = xact::IsTransactionOrTransactionBlock();

        if use_subtxn {
            xact::BeginInternalSubTransaction(Some("replay"))?;
            // Invalidations run outside a valid transaction so entries are
            // just marked invalid without catalog access.
            xact::AbortCurrentTransaction()?;
        }

        execute_invalidations(invalidations)?;

        if use_subtxn {
            xact::RollbackAndReleaseCurrentSubTransaction()?;
        }
        Ok(())
    }
}
