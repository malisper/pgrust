#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

mod iter;
mod replay;
mod spill;
mod startup;
mod stream;
mod toast;
mod visibility;

#[cfg(test)]
mod tests;

pub use startup::{init_seams, StartupReorderBuffer};
pub use toast::ReorderBufferToastEnt;
pub use visibility::{
    ResolveCminCmaxDuringDecoding, ReorderBufferTupleCidEnt, ReorderBufferTupleCidKey,
    TupleCidHash,
};

use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;

use heaptuple::HeapTuple;
use mcx::{Mcx, MemoryContext, PgFxHashMap, PgString, PgVec};
use snapmgr::Snapshot;
use types_core::{
    CommandId, InvalidCommandId, InvalidTransactionId, InvalidXLogRecPtr, Oid, RepOriginId,
    TimestampTz, TransactionId, XLogRecPtr,
};
use types_error::{PgError, PgResult};
use types_rel::RelationData;
use types_storage::{RelFileLocator, SharedInvalidationMessage};
use types_tuple::{ItemPointerData, SizeofHeapTupleHeader};

#[cold]
#[inline(never)]
pub(crate) fn unported(what: &str) -> ! {
    panic!("unported callee reached from reorderbuffer.c: {what}")
}

#[cold]
#[inline(never)]
pub(crate) fn rb_error(msg: String) -> Box<PgError> {
    PgError::error(msg).into()
}

thread_local! {
    // rb->context family collapsed to one long-lived AllocSet; per-object
    // frees go through Rust drops instead of C's slab/generation contexts.
    static RB_CTX: &'static MemoryContext = ::mcx::session_root("ReorderBuffer");
}

pub(crate) fn rb_mcx() -> Mcx<'static> {
    RB_CTX.with(|c| c.mcx())
}

pub type TxnId = u32;
pub type ChangeId = u32;
pub const INVALID_ID: u32 = u32::MAX;

pub const RBTXN_HAS_CATALOG_CHANGES: u32 = 0x0001;
pub const RBTXN_IS_SUBXACT: u32 = 0x0002;
pub const RBTXN_IS_SERIALIZED: u32 = 0x0004;
pub const RBTXN_IS_SERIALIZED_CLEAR: u32 = 0x0008;
pub const RBTXN_IS_STREAMED: u32 = 0x0010;
pub const RBTXN_HAS_PARTIAL_CHANGE: u32 = 0x0020;
pub const RBTXN_IS_PREPARED: u32 = 0x0040;
pub const RBTXN_SKIPPED_PREPARE: u32 = 0x0080;
pub const RBTXN_HAS_STREAMABLE_CHANGE: u32 = 0x0100;
pub const RBTXN_SENT_PREPARE: u32 = 0x0200;
pub const RBTXN_IS_COMMITTED: u32 = 0x0400;
pub const RBTXN_IS_ABORTED: u32 = 0x0800;
pub const RBTXN_DISTR_INVAL_OVERFLOWED: u32 = 0x1000;

pub const RBTXN_PREPARE_STATUS_MASK: u32 =
    RBTXN_IS_PREPARED | RBTXN_SKIPPED_PREPARE | RBTXN_SENT_PREPARE;

pub(crate) const MAX_DISTR_INVAL_MSG_PER_TXN: usize =
    (8 * 1024 * 1024) / std::mem::size_of::<SharedInvalidationMessage>();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum ReorderBufferChangeType {
    Insert = 0,
    Update,
    Delete,
    Message,
    Invalidation,
    InternalSnapshot,
    InternalCommandId,
    InternalTupleCid,
    InternalSpecInsert,
    InternalSpecConfirm,
    InternalSpecAbort,
    Truncate,
}

pub use ReorderBufferChangeType::*;

pub enum ReorderBufferChangeData {
    Tp {
        rlocator: RelFileLocator,
        clear_toast_afterwards: bool,
        oldtuple: Option<HeapTuple<'static>>,
        newtuple: Option<HeapTuple<'static>>,
    },
    Truncate {
        cascade: bool,
        restart_seqs: bool,
        relids: PgVec<'static, Oid>,
    },
    Msg {
        prefix: PgString<'static>,
        message: PgVec<'static, u8>,
    },
    Snapshot(Snapshot),
    CommandId(CommandId),
    TupleCid {
        locator: RelFileLocator,
        tid: ItemPointerData,
        cmin: CommandId,
        cmax: CommandId,
        combocid: CommandId,
    },
    Inval {
        invalidations: PgVec<'static, SharedInvalidationMessage>,
    },
}

#[derive(Clone, Copy)]
pub(crate) struct Links {
    pub(crate) prev: u32,
    pub(crate) next: u32,
}

impl Default for Links {
    fn default() -> Self {
        Links { prev: INVALID_ID, next: INVALID_ID }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ListHead {
    pub(crate) head: u32,
    pub(crate) tail: u32,
}

impl ListHead {
    pub(crate) const EMPTY: ListHead = ListHead { head: INVALID_ID, tail: INVALID_ID };

    pub(crate) fn is_empty(self) -> bool {
        self.head == INVALID_ID
    }
}

impl Default for ListHead {
    fn default() -> Self {
        ListHead::EMPTY
    }
}

pub(crate) fn dl_push_tail<T>(
    slab: &mut [Option<T>],
    list: &mut ListHead,
    id: u32,
    links: impl Fn(&mut T) -> &mut Links + Copy,
) {
    let tail = list.tail;
    {
        let l = links(slab[id as usize].as_mut().expect("live slab entry"));
        l.prev = tail;
        l.next = INVALID_ID;
    }
    if tail == INVALID_ID {
        list.head = id;
    } else {
        links(slab[tail as usize].as_mut().expect("live slab entry")).next = id;
    }
    list.tail = id;
}

pub(crate) fn dl_delete<T>(
    slab: &mut [Option<T>],
    list: &mut ListHead,
    id: u32,
    links: impl Fn(&mut T) -> &mut Links + Copy,
) {
    let l = *links(slab[id as usize].as_mut().expect("live slab entry"));
    if l.prev == INVALID_ID {
        list.head = l.next;
    } else {
        links(slab[l.prev as usize].as_mut().expect("live slab entry")).next = l.next;
    }
    if l.next == INVALID_ID {
        list.tail = l.prev;
    } else {
        links(slab[l.next as usize].as_mut().expect("live slab entry")).prev = l.prev;
    }
}

pub(crate) fn dl_insert_before<T>(
    slab: &mut [Option<T>],
    list: &mut ListHead,
    before: u32,
    id: u32,
    links: impl Fn(&mut T) -> &mut Links + Copy,
) {
    let prev = links(slab[before as usize].as_mut().expect("live slab entry")).prev;
    {
        let l = links(slab[id as usize].as_mut().expect("live slab entry"));
        l.prev = prev;
        l.next = before;
    }
    links(slab[before as usize].as_mut().expect("live slab entry")).prev = id;
    if prev == INVALID_ID {
        list.head = id;
    } else {
        links(slab[prev as usize].as_mut().expect("live slab entry")).next = id;
    }
}

pub(crate) fn dl_iter<'a, T>(
    slab: &'a [Option<T>],
    list: ListHead,
    links: impl Fn(&T) -> Links + Copy + 'a,
) -> impl Iterator<Item = u32> + 'a {
    let mut cur = list.head;
    std::iter::from_fn(move || {
        if cur == INVALID_ID {
            return None;
        }
        let id = cur;
        cur = links(slab[id as usize].as_ref().expect("live slab entry")).next;
        Some(id)
    })
}

pub struct ReorderBufferChange {
    pub lsn: XLogRecPtr,
    pub action: ReorderBufferChangeType,
    pub origin_id: RepOriginId,
    pub data: ReorderBufferChangeData,
    pub(crate) txn: TxnId,
    pub(crate) node: Links,
}

impl ReorderBufferChange {
    pub fn new(action: ReorderBufferChangeType, data: ReorderBufferChangeData) -> Self {
        ReorderBufferChange {
            lsn: InvalidXLogRecPtr,
            action,
            origin_id: 0,
            data,
            txn: INVALID_ID,
            node: Links::default(),
        }
    }

    pub fn txn(&self) -> TxnId {
        self.txn
    }
}

pub struct ReorderBufferTXN {
    pub txn_flags: u32,
    pub xid: TransactionId,
    pub toplevel_xid: TransactionId,
    pub gid: Option<String>,
    pub first_lsn: XLogRecPtr,
    pub final_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub(crate) toptxn: TxnId,
    pub restart_decoding_lsn: XLogRecPtr,
    pub origin_id: RepOriginId,
    pub origin_lsn: XLogRecPtr,
    pub xact_time: TimestampTz,
    pub base_snapshot: Option<Snapshot>,
    pub base_snapshot_lsn: XLogRecPtr,
    pub(crate) base_snapshot_node: Links,
    pub snapshot_now: Option<Snapshot>,
    pub command_id: CommandId,
    pub nentries: u64,
    pub nentries_mem: u64,
    pub(crate) changes: ListHead,
    pub(crate) tuplecids: ListHead,
    pub ntuplecids: u64,
    pub tuplecid_hash: Option<Rc<RefCell<TupleCidHash>>>,
    pub(crate) toast_hash: Option<PgFxHashMap<'static, Oid, toast::ReorderBufferToastEnt>>,
    pub(crate) subtxns: ListHead,
    pub nsubtxns: u32,
    pub invalidations: Vec<SharedInvalidationMessage>,
    pub invalidations_distributed: Vec<SharedInvalidationMessage>,
    pub(crate) node: Links,
    pub(crate) catchange_node: Links,
    pub size: usize,
    pub total_size: usize,
    pub output_plugin_private: usize,
}

impl ReorderBufferTXN {
    pub fn has_catalog_changes(&self) -> bool {
        self.txn_flags & RBTXN_HAS_CATALOG_CHANGES != 0
    }
    pub fn is_known_subxact(&self) -> bool {
        self.txn_flags & RBTXN_IS_SUBXACT != 0
    }
    pub fn is_serialized(&self) -> bool {
        self.txn_flags & RBTXN_IS_SERIALIZED != 0
    }
    pub fn has_partial_change(&self) -> bool {
        self.txn_flags & RBTXN_HAS_PARTIAL_CHANGE != 0
    }
    pub fn has_streamable_change(&self) -> bool {
        self.txn_flags & RBTXN_HAS_STREAMABLE_CHANGE != 0
    }
    pub fn is_streamed(&self) -> bool {
        self.txn_flags & RBTXN_IS_STREAMED != 0
    }
    pub fn is_prepared(&self) -> bool {
        self.txn_flags & RBTXN_IS_PREPARED != 0
    }
    pub fn sent_prepare(&self) -> bool {
        self.txn_flags & RBTXN_SENT_PREPARE != 0
    }
    pub fn is_committed(&self) -> bool {
        self.txn_flags & RBTXN_IS_COMMITTED != 0
    }
    pub fn is_aborted(&self) -> bool {
        self.txn_flags & RBTXN_IS_ABORTED != 0
    }
    pub fn distr_inval_overflowed(&self) -> bool {
        self.txn_flags & RBTXN_DISTR_INVAL_OVERFLOWED != 0
    }
    pub fn is_toptxn(&self) -> bool {
        self.toptxn == INVALID_ID
    }
    pub fn is_subtxn(&self) -> bool {
        self.toptxn != INVALID_ID
    }
    pub fn ninvalidations(&self) -> u32 {
        self.invalidations.len() as u32
    }
}

fn cb_txn_unset(_: &mut ReorderBuffer, _: TxnId) -> PgResult<()> {
    unported("ReorderBuffer callback slot not installed (logical.c)")
}
fn cb_apply_change_unset(
    _: &mut ReorderBuffer,
    _: TxnId,
    _: &RelationData<'static>,
    _: &mut ReorderBufferChange,
) -> PgResult<()> {
    unported("rb->apply_change callback not installed (logical.c)")
}
fn cb_apply_truncate_unset(
    _: &mut ReorderBuffer,
    _: TxnId,
    _: &[Rc<RelationData<'static>>],
    _: &mut ReorderBufferChange,
) -> PgResult<()> {
    unported("rb->apply_truncate callback not installed (logical.c)")
}
fn cb_lsn_unset(_: &mut ReorderBuffer, _: TxnId, _: XLogRecPtr) -> PgResult<()> {
    unported("ReorderBuffer callback slot not installed (logical.c)")
}
fn cb_message_unset(
    _: &mut ReorderBuffer,
    _: Option<TxnId>,
    _: XLogRecPtr,
    _: bool,
    _: &str,
    _: &[u8],
) -> PgResult<()> {
    unported("rb->message callback not installed (logical.c)")
}
fn cb_rollback_prepared_unset(
    _: &mut ReorderBuffer,
    _: TxnId,
    _: XLogRecPtr,
    _: TimestampTz,
) -> PgResult<()> {
    unported("rb->rollback_prepared callback not installed (logical.c)")
}

pub struct ReorderBufferCallbacks {
    pub begin: fn(&mut ReorderBuffer, TxnId) -> PgResult<()>,
    pub apply_change: fn(
        &mut ReorderBuffer,
        TxnId,
        &RelationData<'static>,
        &mut ReorderBufferChange,
    ) -> PgResult<()>,
    pub apply_truncate: fn(
        &mut ReorderBuffer,
        TxnId,
        &[Rc<RelationData<'static>>],
        &mut ReorderBufferChange,
    ) -> PgResult<()>,
    pub commit: fn(&mut ReorderBuffer, TxnId, XLogRecPtr) -> PgResult<()>,
    pub message:
        fn(&mut ReorderBuffer, Option<TxnId>, XLogRecPtr, bool, &str, &[u8]) -> PgResult<()>,
    pub begin_prepare: fn(&mut ReorderBuffer, TxnId) -> PgResult<()>,
    pub prepare: fn(&mut ReorderBuffer, TxnId, XLogRecPtr) -> PgResult<()>,
    pub commit_prepared: fn(&mut ReorderBuffer, TxnId, XLogRecPtr) -> PgResult<()>,
    pub rollback_prepared: fn(&mut ReorderBuffer, TxnId, XLogRecPtr, TimestampTz) -> PgResult<()>,
    pub stream_start: Option<fn(&mut ReorderBuffer, TxnId, XLogRecPtr) -> PgResult<()>>,
    pub stream_stop: Option<fn(&mut ReorderBuffer, TxnId, XLogRecPtr) -> PgResult<()>>,
    pub stream_abort: Option<fn(&mut ReorderBuffer, TxnId, XLogRecPtr) -> PgResult<()>>,
    pub stream_prepare: Option<fn(&mut ReorderBuffer, TxnId, XLogRecPtr) -> PgResult<()>>,
    pub stream_commit: Option<fn(&mut ReorderBuffer, TxnId, XLogRecPtr) -> PgResult<()>>,
    pub stream_change: Option<
        fn(&mut ReorderBuffer, TxnId, &RelationData<'static>, &mut ReorderBufferChange)
            -> PgResult<()>,
    >,
    pub stream_message: Option<
        fn(&mut ReorderBuffer, Option<TxnId>, XLogRecPtr, bool, &str, &[u8]) -> PgResult<()>,
    >,
    pub stream_truncate: Option<
        fn(&mut ReorderBuffer, TxnId, &[Rc<RelationData<'static>>], &mut ReorderBufferChange)
            -> PgResult<()>,
    >,
    pub update_progress_txn: fn(&mut ReorderBuffer, TxnId, XLogRecPtr) -> PgResult<()>,
}

impl ReorderBufferCallbacks {
    pub fn unset() -> Self {
        ReorderBufferCallbacks {
            begin: cb_txn_unset,
            apply_change: cb_apply_change_unset,
            apply_truncate: cb_apply_truncate_unset,
            commit: cb_lsn_unset,
            message: cb_message_unset,
            begin_prepare: cb_txn_unset,
            prepare: cb_lsn_unset,
            commit_prepared: cb_lsn_unset,
            rollback_prepared: cb_rollback_prepared_unset,
            stream_start: None,
            stream_stop: None,
            stream_abort: None,
            stream_prepare: None,
            stream_commit: None,
            stream_change: None,
            stream_message: None,
            stream_truncate: None,
            update_progress_txn: cb_lsn_unset,
        }
    }
}

pub struct ReorderBuffer {
    pub(crate) mcx: Mcx<'static>,
    pub(crate) slot_name: String,
    // Droppy payload owners (HeapTuple/Rc/PgVec) live in plain-heap Vec slabs
    // per docs/no-drop.md; C's slab contexts have no accounting role here
    // (rb->size is tracked explicitly, as in C).
    pub(crate) txns: Vec<Option<ReorderBufferTXN>>,
    pub(crate) txns_free: Vec<u32>,
    pub(crate) changes: Vec<Option<ReorderBufferChange>>,
    pub(crate) changes_free: Vec<u32>,
    pub(crate) by_txn: PgFxHashMap<'static, TransactionId, TxnId>,
    pub(crate) toplevel_by_lsn: ListHead,
    pub(crate) txns_by_base_snapshot_lsn: ListHead,
    pub(crate) catchange_txns: ListHead,
    pub(crate) catchange_count: usize,
    pub(crate) by_txn_last_xid: TransactionId,
    pub(crate) by_txn_last_txn: TxnId,
    pub callbacks: ReorderBufferCallbacks,
    pub private_data: usize,
    // C flushes spill/stream statistics straight to pgstat from inside the
    // serialize path via rb->private_data's decoding context
    // (reorderbuffer.c:4042); the owning context installs this hook at
    // startup. None (no context yet) flushes nothing, like C before
    // private_data is set.
    pub update_stats: Option<fn(&mut ReorderBuffer)>,
    // The decoding-context half of ReorderBufferCanStartStreaming
    // (reorderbuffer.c:4285): snapbuild consistent and not skipping the
    // record being decoded. Mirrored here by the decode loop before each
    // record; the builder itself lives across the crate boundary.
    pub streaming_ready: bool,
    pub output_rewrites: bool,
    pub(crate) current_restart_decoding_lsn: XLogRecPtr,
    pub size: usize,
    pub spillTxns: i64,
    pub spillCount: i64,
    pub spillBytes: i64,
    pub streamTxns: i64,
    pub streamCount: i64,
    pub streamBytes: i64,
    pub totalTxns: i64,
    pub totalBytes: i64,
}

impl ReorderBuffer {
    pub fn allocate(slot_name: &str) -> PgResult<ReorderBuffer> {
        let mcx = rb_mcx();
        let rb = ReorderBuffer {
            mcx,
            slot_name: slot_name.to_string(),
            txns: Vec::new(),
            txns_free: Vec::new(),
            changes: Vec::new(),
            changes_free: Vec::new(),
            by_txn: PgFxHashMap::with_hasher_in(Default::default(), mcx),
            toplevel_by_lsn: ListHead::EMPTY,
            txns_by_base_snapshot_lsn: ListHead::EMPTY,
            catchange_txns: ListHead::EMPTY,
            catchange_count: 0,
            by_txn_last_xid: InvalidTransactionId,
            by_txn_last_txn: INVALID_ID,
            callbacks: ReorderBufferCallbacks::unset(),
            private_data: 0,
            update_stats: None,
            streaming_ready: false,
            output_rewrites: false,
            current_restart_decoding_lsn: InvalidXLogRecPtr,
            size: 0,
            spillTxns: 0,
            spillCount: 0,
            spillBytes: 0,
            streamTxns: 0,
            streamCount: 0,
            streamBytes: 0,
            totalTxns: 0,
            totalBytes: 0,
        };
        startup::ReorderBufferCleanupSerializedTXNs(slot_name)?;
        Ok(rb)
    }

    pub fn free(self) -> PgResult<()> {
        let slot_name = self.slot_name.clone();
        drop(self);
        startup::ReorderBufferCleanupSerializedTXNs(&slot_name)
    }

    pub fn txn(&self, id: TxnId) -> &ReorderBufferTXN {
        self.txns[id as usize].as_ref().expect("live txn")
    }

    pub fn txn_mut(&mut self, id: TxnId) -> &mut ReorderBufferTXN {
        self.txns[id as usize].as_mut().expect("live txn")
    }

    pub fn change(&self, id: ChangeId) -> &ReorderBufferChange {
        self.changes[id as usize].as_ref().expect("live change")
    }

    pub fn change_mut(&mut self, id: ChangeId) -> &mut ReorderBufferChange {
        self.changes[id as usize].as_mut().expect("live change")
    }

    // rbtxn_get_toptxn (reorderbuffer.h): the toplevel txn of a (sub)txn.
    pub fn toptxn_id(&self, id: TxnId) -> TxnId {
        let txn = self.txn(id);
        if txn.toptxn == INVALID_ID {
            id
        } else {
            txn.toptxn
        }
    }

    pub fn alloc_tuple_buf(&self, tuple_len: usize) -> PgResult<HeapTuple<'static>> {
        HeapTuple::alloc_zeroed(self.mcx, tuple_len + SizeofHeapTupleHeader)
    }

    fn alloc_txn(&mut self, xid: TransactionId, lsn: XLogRecPtr) -> TxnId {
        let txn = ReorderBufferTXN {
            txn_flags: 0,
            xid,
            toplevel_xid: InvalidTransactionId,
            gid: None,
            first_lsn: lsn,
            final_lsn: InvalidXLogRecPtr,
            end_lsn: InvalidXLogRecPtr,
            toptxn: INVALID_ID,
            restart_decoding_lsn: self.current_restart_decoding_lsn,
            origin_id: 0,
            origin_lsn: InvalidXLogRecPtr,
            xact_time: 0,
            base_snapshot: None,
            base_snapshot_lsn: InvalidXLogRecPtr,
            base_snapshot_node: Links::default(),
            snapshot_now: None,
            command_id: InvalidCommandId,
            nentries: 0,
            nentries_mem: 0,
            changes: ListHead::EMPTY,
            tuplecids: ListHead::EMPTY,
            ntuplecids: 0,
            tuplecid_hash: None,
            toast_hash: None,
            subtxns: ListHead::EMPTY,
            nsubtxns: 0,
            invalidations: Vec::new(),
            invalidations_distributed: Vec::new(),
            node: Links::default(),
            catchange_node: Links::default(),
            size: 0,
            total_size: 0,
            output_plugin_private: 0,
        };
        match self.txns_free.pop() {
            Some(id) => {
                self.txns[id as usize] = Some(txn);
                id
            }
            None => {
                self.txns.push(Some(txn));
                (self.txns.len() - 1) as TxnId
            }
        }
    }

    pub(crate) fn free_txn(&mut self, id: TxnId) {
        if self.by_txn_last_xid == self.txn(id).xid {
            self.by_txn_last_xid = InvalidTransactionId;
            self.by_txn_last_txn = INVALID_ID;
        }
        self.txn_mut(id).gid = None;
        self.txn_mut(id).tuplecid_hash = None;
        self.txn_mut(id).invalidations = Vec::new();
        self.txn_mut(id).invalidations_distributed = Vec::new();
        self.toast_reset(id);
        debug_assert_eq!(self.txn(id).size, 0);
        self.txns[id as usize] = None;
        self.txns_free.push(id);
    }

    pub(crate) fn alloc_change_slot(&mut self, change: ReorderBufferChange) -> ChangeId {
        match self.changes_free.pop() {
            Some(id) => {
                self.changes[id as usize] = Some(change);
                id
            }
            None => {
                self.changes.push(Some(change));
                (self.changes.len() - 1) as ChangeId
            }
        }
    }

    // ReorderBufferFreeChange; the caller must already have unlinked `id`
    // from any change list.
    pub(crate) fn free_change(&mut self, id: ChangeId, upd_mem: bool) {
        if upd_mem {
            let sz = self.change_size(id);
            let txn = self.change(id).txn;
            self.change_memory_update(Some(id), Some(txn), false, sz);
        }
        self.changes[id as usize] = None;
        self.changes_free.push(id);
    }

    pub(crate) fn txn_by_xid(
        &mut self,
        xid: TransactionId,
        create: bool,
        lsn: XLogRecPtr,
        create_as_top: bool,
    ) -> (Option<TxnId>, bool) {
        debug_assert!(xid != InvalidTransactionId);

        if self.by_txn_last_xid != InvalidTransactionId && self.by_txn_last_xid == xid {
            if self.by_txn_last_txn != INVALID_ID {
                return (Some(self.by_txn_last_txn), false);
            }
            if !create {
                return (None, false);
            }
        }

        let found = self.by_txn.get(&xid).copied();
        let (txn, is_new) = match found {
            Some(id) => (Some(id), false),
            None if create => {
                debug_assert!(lsn != InvalidXLogRecPtr);
                let id = self.alloc_txn(xid, lsn);
                self.by_txn.insert(xid, id);
                if create_as_top {
                    let mut list = self.toplevel_by_lsn;
                    dl_push_tail(&mut self.txns, &mut list, id, |t| &mut t.node);
                    self.toplevel_by_lsn = list;
                    self.assert_txn_lsn_order();
                }
                (Some(id), true)
            }
            None => (None, false),
        };

        self.by_txn_last_xid = xid;
        self.by_txn_last_txn = txn.unwrap_or(INVALID_ID);
        debug_assert!(!create || txn.is_some());
        (txn, is_new)
    }

    pub(crate) fn assert_txn_lsn_order(&self) {
        #[cfg(debug_assertions)]
        {
            // C additionally gates on SnapBuildXactNeedsSkip via private_data;
            // pre-consistent-point records never reach this port's callers yet.
            let mut prev_first_lsn = InvalidXLogRecPtr;
            for id in dl_iter(&self.txns, self.toplevel_by_lsn, |t| t.node) {
                let cur = self.txn(id);
                debug_assert!(cur.first_lsn != InvalidXLogRecPtr);
                if cur.end_lsn != InvalidXLogRecPtr {
                    debug_assert!(cur.first_lsn <= cur.end_lsn);
                }
                if prev_first_lsn != InvalidXLogRecPtr {
                    debug_assert!(prev_first_lsn < cur.first_lsn);
                }
                debug_assert!(!cur.is_known_subxact());
                prev_first_lsn = cur.first_lsn;
            }
            let mut prev_base_snap_lsn = InvalidXLogRecPtr;
            for id in dl_iter(&self.txns, self.txns_by_base_snapshot_lsn, |t| t.base_snapshot_node)
            {
                let cur = self.txn(id);
                debug_assert!(cur.base_snapshot.is_some());
                debug_assert!(cur.base_snapshot_lsn != InvalidXLogRecPtr);
                if prev_base_snap_lsn != InvalidXLogRecPtr {
                    debug_assert!(prev_base_snap_lsn < cur.base_snapshot_lsn);
                }
                debug_assert!(!cur.is_known_subxact());
                prev_base_snap_lsn = cur.base_snapshot_lsn;
            }
        }
    }

    pub(crate) fn assert_change_lsn_order(&self, txn: TxnId) {
        #[cfg(debug_assertions)]
        {
            let t = self.txn(txn);
            let mut prev_lsn = t.first_lsn;
            for cid in dl_iter(&self.changes, t.changes, |c| c.node) {
                let cur = self.change(cid);
                debug_assert!(t.first_lsn != InvalidXLogRecPtr);
                debug_assert!(cur.lsn != InvalidXLogRecPtr);
                debug_assert!(t.first_lsn <= cur.lsn);
                if t.end_lsn != InvalidXLogRecPtr {
                    debug_assert!(cur.lsn <= t.end_lsn);
                }
                debug_assert!(prev_lsn <= cur.lsn);
                prev_lsn = cur.lsn;
            }
        }
        #[cfg(not(debug_assertions))]
        let _ = txn;
    }

    pub fn get_oldest_txn(&self) -> Option<TxnId> {
        self.assert_txn_lsn_order();
        if self.toplevel_by_lsn.is_empty() {
            return None;
        }
        let id = self.toplevel_by_lsn.head;
        debug_assert!(!self.txn(id).is_known_subxact());
        debug_assert!(self.txn(id).first_lsn != InvalidXLogRecPtr);
        Some(id)
    }

    pub fn get_oldest_xmin(&self) -> TransactionId {
        self.assert_txn_lsn_order();
        if self.txns_by_base_snapshot_lsn.is_empty() {
            return InvalidTransactionId;
        }
        let txn = self.txn(self.txns_by_base_snapshot_lsn.head);
        txn.base_snapshot.as_ref().expect("base snapshot set").xmin
    }

    pub fn set_restart_point(&mut self, ptr: XLogRecPtr) {
        self.current_restart_decoding_lsn = ptr;
    }

    pub fn current_restart_decoding_lsn(&self) -> XLogRecPtr {
        self.current_restart_decoding_lsn
    }

    pub fn toplevel_txns(&self) -> impl Iterator<Item = TxnId> + '_ {
        dl_iter(&self.txns, self.toplevel_by_lsn, |t| t.node)
    }

    pub fn assign_child(&mut self, xid: TransactionId, subxid: TransactionId, lsn: XLogRecPtr) {
        let (txn, _new_top) = self.txn_by_xid(xid, true, lsn, true);
        let txn = txn.expect("created");
        let (subtxn, new_sub) = self.txn_by_xid(subxid, true, lsn, false);
        let subtxn = subtxn.expect("created");

        if !new_sub {
            if self.txn(subtxn).is_known_subxact() {
                return;
            }
            let mut list = self.toplevel_by_lsn;
            dl_delete(&mut self.txns, &mut list, subtxn, |t| &mut t.node);
            self.toplevel_by_lsn = list;
        }

        {
            let sub = self.txn_mut(subtxn);
            sub.txn_flags |= RBTXN_IS_SUBXACT;
            sub.toplevel_xid = xid;
            debug_assert_eq!(sub.nsubtxns, 0);
            sub.toptxn = txn;
        }
        let mut list = self.txn(txn).subtxns;
        dl_push_tail(&mut self.txns, &mut list, subtxn, |t| &mut t.node);
        self.txn_mut(txn).subtxns = list;
        self.txn_mut(txn).nsubtxns += 1;

        self.transfer_snap_to_parent(txn, subtxn);
        self.assert_txn_lsn_order();
    }

    pub(crate) fn transfer_snap_to_parent(&mut self, txn: TxnId, subtxn: TxnId) {
        debug_assert_eq!(self.txn(subtxn).toplevel_xid, self.txn(txn).xid);

        if self.txn(subtxn).base_snapshot.is_none() {
            return;
        }
        let sub_lsn = self.txn(subtxn).base_snapshot_lsn;
        if self.txn(txn).base_snapshot.is_none() || sub_lsn < self.txn(txn).base_snapshot_lsn {
            if self.txn(txn).base_snapshot.is_some() {
                self.txn_mut(txn).base_snapshot = None;
                let mut list = self.txns_by_base_snapshot_lsn;
                dl_delete(&mut self.txns, &mut list, txn, |t| &mut t.base_snapshot_node);
                self.txns_by_base_snapshot_lsn = list;
            }
            let snap = self.txn_mut(subtxn).base_snapshot.take();
            self.txn_mut(txn).base_snapshot = snap;
            self.txn_mut(txn).base_snapshot_lsn = sub_lsn;
            let mut list = self.txns_by_base_snapshot_lsn;
            dl_insert_before(&mut self.txns, &mut list, subtxn, txn, |t| {
                &mut t.base_snapshot_node
            });
            dl_delete(&mut self.txns, &mut list, subtxn, |t| &mut t.base_snapshot_node);
            self.txns_by_base_snapshot_lsn = list;
            self.txn_mut(subtxn).base_snapshot_lsn = InvalidXLogRecPtr;
        } else {
            self.txn_mut(subtxn).base_snapshot = None;
            self.txn_mut(subtxn).base_snapshot_lsn = InvalidXLogRecPtr;
            let mut list = self.txns_by_base_snapshot_lsn;
            dl_delete(&mut self.txns, &mut list, subtxn, |t| &mut t.base_snapshot_node);
            self.txns_by_base_snapshot_lsn = list;
        }
    }

    pub fn commit_child(
        &mut self,
        xid: TransactionId,
        subxid: TransactionId,
        commit_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
    ) {
        let (subtxn, _) = self.txn_by_xid(subxid, false, InvalidXLogRecPtr, false);
        let Some(subtxn) = subtxn else {
            return;
        };
        self.txn_mut(subtxn).final_lsn = commit_lsn;
        self.txn_mut(subtxn).end_lsn = end_lsn;
        self.assign_child(xid, subxid, InvalidXLogRecPtr);
    }

    pub fn queue_change(
        &mut self,
        xid: TransactionId,
        lsn: XLogRecPtr,
        mut change: ReorderBufferChange,
        toast_insert: bool,
    ) -> PgResult<()> {
        let (txn, _) = self.txn_by_xid(xid, true, lsn, true);
        let txn = txn.expect("created");

        if self.txn(txn).is_aborted() {
            drop(change);
            return Ok(());
        }

        if matches!(
            change.action,
            Insert | Update | Delete | InternalSpecInsert | Truncate | Message
        ) {
            let top = self.toptxn_id(txn);
            self.txn_mut(top).txn_flags |= RBTXN_HAS_STREAMABLE_CHANGE;
        }

        change.lsn = lsn;
        change.txn = txn;
        debug_assert!(lsn != InvalidXLogRecPtr);

        let cid = self.alloc_change_slot(change);
        let mut list = self.txn(txn).changes;
        dl_push_tail(&mut self.changes, &mut list, cid, |c| &mut c.node);
        self.txn_mut(txn).changes = list;
        self.txn_mut(txn).nentries += 1;
        self.txn_mut(txn).nentries_mem += 1;

        let sz = self.change_size(cid);
        self.change_memory_update(Some(cid), None, true, sz);
        self.process_partial_change(txn, cid, toast_insert)?;
        self.check_memory_limit()?;
        Ok(())
    }

    pub(crate) fn can_stream(&self) -> bool {
        self.callbacks.stream_start.is_some()
    }

    fn process_partial_change(&mut self, txn: TxnId, cid: ChangeId, toast_insert: bool) -> PgResult<()> {
        if !self.can_stream() {
            return Ok(());
        }
        let toptxn = self.toptxn_id(txn);
        let action = self.change(cid).action;
        let clear_toast = matches!(
            self.change(cid).data,
            ReorderBufferChangeData::Tp { clear_toast_afterwards: true, .. }
        );

        if toast_insert {
            self.txn_mut(toptxn).txn_flags |= RBTXN_HAS_PARTIAL_CHANGE;
        } else if self.txn(toptxn).has_partial_change()
            && matches!(action, Insert | Update | InternalSpecInsert)
            && clear_toast
        {
            self.txn_mut(toptxn).txn_flags &= !RBTXN_HAS_PARTIAL_CHANGE;
        }

        if action == InternalSpecInsert {
            self.txn_mut(toptxn).txn_flags |= RBTXN_HAS_PARTIAL_CHANGE;
        } else if self.txn(toptxn).has_partial_change()
            && matches!(action, InternalSpecConfirm | InternalSpecAbort)
        {
            self.txn_mut(toptxn).txn_flags &= !RBTXN_HAS_PARTIAL_CHANGE;
        }

        // Stream the transaction if it was serialized before and its changes
        // are now complete at the toplevel (reorderbuffer.c:794): otherwise a
        // later eviction pass could try to spill an incomplete-change txn.
        if self.can_start_streaming()
            && !self.txn(toptxn).has_partial_change()
            && self.txn(txn).is_serialized()
            && self.txn(toptxn).has_streamable_change()
        {
            self.stream_txn(toptxn)?;
        }
        Ok(())
    }

    pub fn queue_message(
        &mut self,
        xid: TransactionId,
        snap: Option<Snapshot>,
        lsn: XLogRecPtr,
        transactional: bool,
        prefix: &str,
        message: &[u8],
    ) -> PgResult<()> {
        if transactional {
            debug_assert!(xid != InvalidTransactionId);
            debug_assert!(snap.is_none());

            let mut msg = PgVec::new_in(self.mcx);
            mcx::vec_append_bytes(&mut msg, message)?;
            let change = ReorderBufferChange::new(
                Message,
                ReorderBufferChangeData::Msg {
                    prefix: PgString::from_str_in(prefix, self.mcx)?,
                    message: msg,
                },
            );
            self.queue_change(xid, lsn, change, false)
        } else {
            let snapshot_now = snap.expect("non-transactional message requires a snapshot");
            let txn = if xid != InvalidTransactionId {
                self.txn_by_xid(xid, true, lsn, true).0
            } else {
                None
            };

            snapmgr::SetupHistoricSnapshot(snapshot_now, None);
            let cb = self.callbacks.message;
            let r = cb(self, txn, lsn, false, prefix, message);
            snapmgr::TeardownHistoricSnapshot(r.is_err());
            r
        }
    }

    pub fn process_xid(&mut self, xid: TransactionId, lsn: XLogRecPtr) {
        if xid != InvalidTransactionId {
            self.txn_by_xid(xid, true, lsn, true);
        }
    }

    pub fn add_snapshot(&mut self, xid: TransactionId, lsn: XLogRecPtr, snap: Snapshot) -> PgResult<()> {
        let change =
            ReorderBufferChange::new(InternalSnapshot, ReorderBufferChangeData::Snapshot(snap));
        self.queue_change(xid, lsn, change, false)
    }

    pub fn set_base_snapshot(&mut self, xid: TransactionId, lsn: XLogRecPtr, snap: Snapshot) {
        let (txn, _is_new) = self.txn_by_xid(xid, true, lsn, true);
        let mut txn = txn.expect("created");
        if self.txn(txn).is_known_subxact() {
            let top_xid = self.txn(txn).toplevel_xid;
            txn = self
                .txn_by_xid(top_xid, false, InvalidXLogRecPtr, false)
                .0
                .expect("toplevel txn exists");
        }
        debug_assert!(self.txn(txn).base_snapshot.is_none());
        self.txn_mut(txn).base_snapshot = Some(snap);
        self.txn_mut(txn).base_snapshot_lsn = lsn;
        let mut list = self.txns_by_base_snapshot_lsn;
        dl_push_tail(&mut self.txns, &mut list, txn, |t| &mut t.base_snapshot_node);
        self.txns_by_base_snapshot_lsn = list;
        self.assert_txn_lsn_order();
    }

    pub fn add_new_command_id(
        &mut self,
        xid: TransactionId,
        lsn: XLogRecPtr,
        cid: CommandId,
    ) -> PgResult<()> {
        let change =
            ReorderBufferChange::new(InternalCommandId, ReorderBufferChangeData::CommandId(cid));
        self.queue_change(xid, lsn, change, false)
    }

    pub fn add_new_tuple_cids(
        &mut self,
        xid: TransactionId,
        lsn: XLogRecPtr,
        locator: RelFileLocator,
        tid: ItemPointerData,
        cmin: CommandId,
        cmax: CommandId,
        combocid: CommandId,
    ) {
        let (txn, _) = self.txn_by_xid(xid, true, lsn, true);
        let txn = txn.expect("created");

        let mut change = ReorderBufferChange::new(
            InternalTupleCid,
            ReorderBufferChangeData::TupleCid { locator, tid, cmin, cmax, combocid },
        );
        change.lsn = lsn;
        change.txn = txn;
        let cid_id = self.alloc_change_slot(change);
        let mut list = self.txn(txn).tuplecids;
        dl_push_tail(&mut self.changes, &mut list, cid_id, |c| &mut c.node);
        self.txn_mut(txn).tuplecids = list;
        self.txn_mut(txn).ntuplecids += 1;
    }

    fn queue_invalidations(
        &mut self,
        xid: TransactionId,
        lsn: XLogRecPtr,
        msgs: &[SharedInvalidationMessage],
    ) -> PgResult<()> {
        let mut invals = PgVec::new_in(self.mcx);
        invals.extend_from_slice(msgs);
        let change = ReorderBufferChange::new(
            Invalidation,
            ReorderBufferChangeData::Inval { invalidations: invals },
        );
        self.queue_change(xid, lsn, change, false)
    }

    pub fn add_invalidations(
        &mut self,
        xid: TransactionId,
        lsn: XLogRecPtr,
        msgs: &[SharedInvalidationMessage],
    ) -> PgResult<()> {
        let (txn, _) = self.txn_by_xid(xid, true, lsn, true);
        let txn = self.toptxn_id(txn.expect("created"));
        debug_assert!(!msgs.is_empty());
        self.txn_mut(txn).invalidations.extend_from_slice(msgs);
        self.queue_invalidations(xid, lsn, msgs)
    }

    pub fn add_distributed_invalidations(
        &mut self,
        xid: TransactionId,
        lsn: XLogRecPtr,
        msgs: &[SharedInvalidationMessage],
    ) -> PgResult<()> {
        let (txn, _) = self.txn_by_xid(xid, true, lsn, true);
        let txn = self.toptxn_id(txn.expect("created"));
        debug_assert!(!msgs.is_empty());

        if !self.txn(txn).distr_inval_overflowed() {
            if self.txn(txn).invalidations_distributed.len() + msgs.len()
                >= MAX_DISTR_INVAL_MSG_PER_TXN
            {
                self.txn_mut(txn).txn_flags |= RBTXN_DISTR_INVAL_OVERFLOWED;
                self.txn_mut(txn).invalidations_distributed = Vec::new();
            } else {
                self.txn_mut(txn)
                    .invalidations_distributed
                    .extend_from_slice(msgs);
            }
        }
        self.queue_invalidations(xid, lsn, msgs)
    }

    pub fn xid_set_catalog_changes(&mut self, xid: TransactionId, lsn: XLogRecPtr) {
        let (txn, _) = self.txn_by_xid(xid, true, lsn, true);
        let txn = txn.expect("created");

        if !self.txn(txn).has_catalog_changes() {
            self.txn_mut(txn).txn_flags |= RBTXN_HAS_CATALOG_CHANGES;
            let mut list = self.catchange_txns;
            dl_push_tail(&mut self.txns, &mut list, txn, |t| &mut t.catchange_node);
            self.catchange_txns = list;
            self.catchange_count += 1;
        }

        if self.txn(txn).is_subtxn() {
            let toptxn = self.toptxn_id(txn);
            if !self.txn(toptxn).has_catalog_changes() {
                self.txn_mut(toptxn).txn_flags |= RBTXN_HAS_CATALOG_CHANGES;
                let mut list = self.catchange_txns;
                dl_push_tail(&mut self.txns, &mut list, toptxn, |t| &mut t.catchange_node);
                self.catchange_txns = list;
                self.catchange_count += 1;
            }
        }
    }

    pub fn get_catalog_changes_xacts(&self) -> Vec<TransactionId> {
        let mut xids: Vec<TransactionId> = dl_iter(&self.txns, self.catchange_txns, |t| {
            t.catchange_node
        })
        .map(|id| self.txn(id).xid)
        .collect();
        debug_assert_eq!(xids.len(), self.catchange_count);
        // xidComparator: plain uint32 order.
        xids.sort_unstable();
        xids
    }

    pub fn xid_has_catalog_changes(&mut self, xid: TransactionId) -> bool {
        match self.txn_by_xid(xid, false, InvalidXLogRecPtr, false).0 {
            Some(txn) => self.txn(txn).has_catalog_changes(),
            None => false,
        }
    }

    pub fn xid_has_base_snapshot(&mut self, xid: TransactionId) -> bool {
        let Some(mut txn) = self.txn_by_xid(xid, false, InvalidXLogRecPtr, false).0 else {
            return false;
        };
        if self.txn(txn).is_known_subxact() {
            let top_xid = self.txn(txn).toplevel_xid;
            txn = self
                .txn_by_xid(top_xid, false, InvalidXLogRecPtr, false)
                .0
                .expect("toplevel txn exists");
        }
        self.txn(txn).base_snapshot.is_some()
    }

    pub fn get_invalidations(&mut self, xid: TransactionId) -> &[SharedInvalidationMessage] {
        match self.txn_by_xid(xid, false, InvalidXLogRecPtr, false).0 {
            Some(txn) => &self.txn(txn).invalidations,
            None => &[],
        }
    }

    // ReorderBufferRememberPrepareInfo (reorderbuffer.c:2897): record the
    // prepare information and mark the transaction prepared. Returns false
    // for an unknown transaction.
    pub fn remember_prepare_info(
        &mut self,
        xid: TransactionId,
        prepare_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        prepare_time: TimestampTz,
        origin_id: RepOriginId,
        origin_lsn: XLogRecPtr,
    ) -> bool {
        let Some(txn) = self.txn_by_xid(xid, false, InvalidXLogRecPtr, false).0 else {
            return false;
        };

        // Remember the prepare information to be later used by commit
        // prepared in case we skip doing prepare.
        let t = self.txn_mut(txn);
        t.final_lsn = prepare_lsn;
        t.end_lsn = end_lsn;
        t.xact_time = prepare_time;
        t.origin_id = origin_id;
        t.origin_lsn = origin_lsn;

        debug_assert_eq!(t.txn_flags & RBTXN_PREPARE_STATUS_MASK, 0);
        t.txn_flags |= RBTXN_IS_PREPARED;
        true
    }

    // ReorderBufferSkipPrepare (reorderbuffer.c:2929).
    pub fn skip_prepare(&mut self, xid: TransactionId) {
        let Some(txn) = self.txn_by_xid(xid, false, InvalidXLogRecPtr, false).0 else {
            return;
        };
        let t = self.txn_mut(txn);
        debug_assert_eq!(t.txn_flags & RBTXN_PREPARE_STATUS_MASK, RBTXN_IS_PREPARED);
        t.txn_flags |= RBTXN_SKIPPED_PREPARE;
    }

    // ReorderBufferPrepare (reorderbuffer.c:2950): replay the transaction at
    // PREPARE time and send the prepare callback.
    pub fn prepare(&mut self, xid: TransactionId, gid: &str) -> PgResult<()> {
        let Some(txn) = self.txn_by_xid(xid, false, InvalidXLogRecPtr, false).0 else {
            return Ok(());
        };

        debug_assert_eq!(
            self.txn(txn).txn_flags & RBTXN_PREPARE_STATUS_MASK,
            RBTXN_IS_PREPARED
        );
        debug_assert!(self.txn(txn).final_lsn != InvalidXLogRecPtr);

        self.txn_mut(txn).gid = Some(gid.to_string());

        let (final_lsn, end_lsn, prepare_time, origin_id, origin_lsn) = {
            let t = self.txn(txn);
            (t.final_lsn, t.end_lsn, t.xact_time, t.origin_id, t.origin_lsn)
        };
        self.replay(txn, final_lsn, end_lsn, prepare_time, origin_id, origin_lsn)?;

        // Send a prepare if not already done so. This might occur if we have
        // detected a concurrent abort while replaying the non-streaming
        // transaction.
        if !self.txn(txn).sent_prepare() {
            let cb = self.callbacks.prepare;
            cb(self, txn, final_lsn)?;
            self.txn_mut(txn).txn_flags |= RBTXN_SENT_PREPARE;
        }
        Ok(())
    }

    // ReorderBufferFinishPrepared (reorderbuffer.c:3007): handle
    // COMMIT/ROLLBACK PREPARED, replaying the transaction first when its
    // prepare predates two_phase_at (skipped-prepare arm, reorderbuffer.c:3025).
    #[allow(clippy::too_many_arguments)]
    pub fn finish_prepared(
        &mut self,
        xid: TransactionId,
        commit_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        two_phase_at: XLogRecPtr,
        commit_time: TimestampTz,
        origin_id: RepOriginId,
        origin_lsn: XLogRecPtr,
        gid: &str,
        is_commit: bool,
    ) -> PgResult<()> {
        let Some(txn) = self.txn_by_xid(xid, false, commit_lsn, false).0 else {
            return Ok(());
        };

        // By this time the txn has the prepare record information; remember
        // it to be later used for rollback.
        let prepare_end_lsn = self.txn(txn).end_lsn;
        let prepare_time = self.txn(txn).xact_time;

        self.txn_mut(txn).gid = Some(gid.to_string());

        // It is possible that this transaction is not decoded at prepare
        // time, either because by that time we didn't have a consistent
        // snapshot, or two_phase was not enabled, or it was decoded earlier
        // but we have restarted. We only need to send the prepare if it was
        // not decoded earlier. We don't need to decode the xact for aborts if
        // it is not done already.
        if self.txn(txn).final_lsn < two_phase_at && is_commit {
            debug_assert_eq!(
                self.txn(txn).txn_flags & RBTXN_PREPARE_STATUS_MASK,
                RBTXN_IS_PREPARED | RBTXN_SKIPPED_PREPARE
            );
            debug_assert!(self.txn(txn).final_lsn != InvalidXLogRecPtr);

            // Use the prepare record's information for the replay so the
            // downstream gets accurate positions (not the commit record's).
            let (final_lsn, p_end_lsn, p_time, p_origin_id, p_origin_lsn) = {
                let t = self.txn(txn);
                (t.final_lsn, t.end_lsn, t.xact_time, t.origin_id, t.origin_lsn)
            };
            self.replay(txn, final_lsn, p_end_lsn, p_time, p_origin_id, p_origin_lsn)?;
        }

        {
            let t = self.txn_mut(txn);
            t.final_lsn = commit_lsn;
            t.end_lsn = end_lsn;
            t.xact_time = commit_time;
            t.origin_id = origin_id;
            t.origin_lsn = origin_lsn;
        }

        if is_commit {
            let cb = self.callbacks.commit_prepared;
            cb(self, txn, commit_lsn)?;
        } else {
            let cb = self.callbacks.rollback_prepared;
            cb(self, txn, prepare_end_lsn, prepare_time)?;
        }

        // cleanup: make sure there's no cache pollution.
        let invals = std::mem::take(&mut self.txn_mut(txn).invalidations);
        replay::execute_invalidations(&invals)?;
        self.txn_mut(txn).invalidations = invals;
        self.cleanup_txn(txn)?;
        Ok(())
    }

    pub(crate) fn tuplecid_hash_any(&self, txn: TxnId) -> Option<Rc<dyn Any>> {
        self.txn(txn)
            .tuplecid_hash
            .clone()
            .map(|h| h as Rc<dyn Any>)
    }
}
