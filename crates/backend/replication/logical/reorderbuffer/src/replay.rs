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
    dl_delete, dl_iter, rb_error, unported, ChangeId, ListHead, ReorderBuffer,
    ReorderBufferChangeData, ReorderBufferChangeType::*, TxnId, RBTXN_IS_SERIALIZED,
    RBTXN_IS_SERIALIZED_CLEAR, RBTXN_IS_STREAMED, RBTXN_SENT_PREPARE,
};

fn relid_by_relfilenumber(_spc_oid: Oid, _rel_number: Oid) -> Oid {
    unported("RelidByRelfilenumber (utils/cache/relfilenumbermap.c)")
}

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
        // eviction (spill/stream), which is phase-2.
        if addition {
            self.txn_mut(txn).size += sz;
            self.size += sz;
            self.txn_mut(toptxn).total_size += sz;
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

    pub(crate) fn check_memory_limit(&self) {
        let limit = guc_tables::vars::logical_decoding_work_mem.read() as usize * 1024;
        if self.size < limit {
            return;
        }
        unported("ReorderBufferSerializeTXN (spill-to-disk): phase-2");
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

    pub(crate) fn cleanup_txn(&mut self, txn: TxnId) {
        let subs: Vec<TxnId> = dl_iter(&self.txns, self.txn(txn).subtxns, |t| t.node).collect();
        for sub in subs {
            debug_assert!(self.txn(sub).is_known_subxact());
            debug_assert_eq!(self.txn(sub).nsubtxns, 0);
            self.cleanup_txn(sub);
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

        debug_assert!(!self.txn(txn).is_serialized(), "spill files: phase-2");
        self.free_txn(txn);
    }

    pub(crate) fn truncate_txn(&mut self, txn: TxnId, txn_prepared: bool) {
        let subs: Vec<TxnId> = dl_iter(&self.txns, self.txn(txn).subtxns, |t| t.node).collect();
        for sub in subs {
            debug_assert!(self.txn(sub).is_known_subxact());
            debug_assert_eq!(self.txn(sub).nsubtxns, 0);
            self.maybe_mark_txn_streamed(sub);
            self.truncate_txn(sub, txn_prepared);
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

        if self.txn(txn).is_serialized() {
            self.txn_mut(txn).txn_flags &= !RBTXN_IS_SERIALIZED;
            self.txn_mut(txn).txn_flags |= RBTXN_IS_SERIALIZED_CLEAR;
        }

        self.txn_mut(txn).nentries_mem = 0;
        self.txn_mut(txn).nentries = 0;
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

    fn replay(
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

        if self.txn(txn).is_streamed() {
            unported("ReorderBufferStreamCommit (streaming): phase-2");
        }

        if self.txn(txn).base_snapshot.is_none() {
            debug_assert!(self.txn(txn).invalidations.is_empty());
            if !self.txn(txn).is_prepared() {
                self.cleanup_txn(txn);
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
        debug_assert!(!streaming, "ReorderBufferStreamTXN (streaming): phase-2");

        self.build_tuplecid_hash(txn);
        snapmgr::SetupHistoricSnapshot(snapshot_now.clone(), self.tuplecid_hash_any(txn));

        let using_subtxn = xact::IsTransactionOrTransactionBlock();

        let mut iterstate: Option<IterState> = None;
        let mut specinsert: Option<ChangeId> = None;
        let mut snapshot_now = snapshot_now;
        let mut command_id = command_id;

        let result = self.process_txn_guts(
            txn,
            commit_lsn,
            &mut snapshot_now,
            &mut command_id,
            using_subtxn,
            &mut iterstate,
            &mut specinsert,
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

                // C's ERRCODE_TRANSACTION_ROLLBACK graceful arm applies only
                // to streaming/prepared decoding: phase-2.
                self.cleanup_txn(txn);
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
    ) -> PgResult<()> {
        let mut prev_lsn = InvalidXLogRecPtr;
        let mut changes_count = 0u32;

        if using_subtxn {
            xact::BeginInternalSubTransaction(Some("replay"))?;
        } else {
            xact::StartTransactionCommand()?;
        }

        if self.txn(txn).is_prepared() {
            { let cb = self.callbacks.begin_prepare; cb(self, txn)?; }
        } else {
            { let cb = self.callbacks.begin; cb(self, txn)?; }
        }

        *iterstate = Some(self.iter_txn_init(txn));
        loop {
            let cur = {
                let state = iterstate.as_mut().expect("iterator initialized");
                self.iter_txn_next(state)
            };
            let Some(cur) = cur else {
                break;
            };

            debug_assert!(prev_lsn == InvalidXLogRecPtr || prev_lsn <= self.change(cur).lsn);
            prev_lsn = self.change(cur).lsn;

            // SetupCheckXidLive is streaming/prepared-only: phase-2.

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
                    self.apply_tuple_change(txn, work, specinsert)?;
                }
                InternalSpecInsert => {
                    if let Some(prev) = specinsert.take() {
                        self.free_change(prev, true);
                    }
                    let owner = self.change(cur).txn;
                    let mut list = self.txn(owner).changes;
                    dl_delete(&mut self.changes, &mut list, cur, |c| &mut c.node);
                    self.txn_mut(owner).changes = list;
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
                    let cb = self.callbacks.apply_truncate;
                    let r = cb(self, txn, &relations, &mut change);
                    self.changes[cur as usize] = Some(change);
                    r?;
                }
                Message => {
                    self.apply_message(txn, cur)?;
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
                cb(self, txn, prev_lsn)?;
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

        if self.txn(txn).is_prepared() {
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

        if self.txn(txn).is_prepared() {
            self.truncate_txn(txn, true);
        } else {
            self.cleanup_txn(txn);
        }
        Ok(())
    }

    fn apply_tuple_change(
        &mut self,
        txn: TxnId,
        work: ChangeId,
        specinsert: &mut Option<ChangeId>,
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

        let reloid = relid_by_relfilenumber(rlocator.spcOid, rlocator.relNumber);

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
                    let cb = self.callbacks.apply_change;
                    let r = cb(self, txn, relation, &mut change);
                    self.changes[work as usize] = Some(change);
                    r?;
                    if clear_toast {
                        self.toast_reset(txn);
                    }
                } else if self.change(work).action == Insert {
                    debug_assert!(has_new);
                    debug_assert!(specinsert.is_none(), "spec-insert into a toast relation");
                    let owner = self.change(work).txn;
                    let mut list = self.txn(owner).changes;
                    dl_delete(&mut self.changes, &mut list, work, |c| &mut c.node);
                    self.txn_mut(owner).changes = list;
                    self.toast_append_chunk(txn, relation, work)?;
                }
            }
        }

        if let Some(si) = specinsert.take() {
            self.free_change(si, true);
        }
        Ok(())
    }

    fn apply_message(&mut self, txn: TxnId, cur: ChangeId) -> PgResult<()> {
        let mut change = self.changes[cur as usize].take().expect("live change");
        let lsn = change.lsn;
        let r = match &change.data {
            ReorderBufferChangeData::Msg { prefix, message } => {
                { let cb = self.callbacks.message; cb(self, Some(txn), lsn, true, prefix.as_str(), message) }
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

        if self.txn(txn).is_streamed() {
            unported("rb->stream_abort (streaming): phase-2");
        }

        self.txn_mut(txn).final_lsn = lsn;
        self.cleanup_txn(txn);
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
                if self.txn(txn).is_streamed() {
                    unported("rb->stream_abort (streaming): phase-2");
                }
                self.cleanup_txn(txn);
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

        self.cleanup_txn(txn);
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
