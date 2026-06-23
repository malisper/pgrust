//! `replication/logical/reorderbuffer.c` — the logical-decoding reorder buffer.
//!
//! # Family decomposition
//!
//! reorderbuffer.c is ~5600 LOC. This crate lands the **foundational family**
//! (the `ReorderBuffer` / `ReorderBufferTXN` data structures, the xid → txn
//! lookup table with its one-entry cache, the txn lifecycle, the toplevel /
//! base-snapshot / catalog-change txn lists, and the txn-level accessors and
//! small queue helpers the historic-snapshot builder `snapbuild.c` and
//! `logical.c` reach through this crate's seam crate) plus the
//! **snapshot-management family**: the per-txn tuplecid hash
//! (`ReorderBufferBuildTupleCidHash`), private snapshot copy/free
//! (`ReorderBufferCopySnap` / `ReorderBufferFreeSnap`), and the combo-CID
//! resolution `ResolveCminCmaxDuringDecoding` consumed by
//! `HeapTupleSatisfiesHistoricMVCC` — the crate's 26th and final inward seam
//! (see [`snapshot`]).
//!
//! The remaining families (recorded in the crate's memory note) are filled in
//! later ports; their entry points are present here as crate-internal helpers
//! that panic loudly (mirror-PG-and-panic), never silent stubs:
//!
//! * **change replay** — `ReorderBufferProcessTXN` / `ReorderBufferReplay` /
//!   `ReorderBufferCommit` / iterator (`ReorderBufferIterTXN*`).
//! * **spill-to-disk codec** (landed, see [`spill`]) —
//!   `ReorderBufferSerializeTXN` / `ReorderBufferSerializeChange` /
//!   `ReorderBufferRestoreChanges` / `ReorderBufferRestoreChange` /
//!   `ReorderBufferRestoreCleanup` / `ReorderBufferCleanupSerializedTXNs` /
//!   `ReorderBufferSerializedPath`. The eviction *driver*
//!   (`ReorderBufferCheckMemoryLimit` + `txn_heap` `LargestTXN` /
//!   `LargestStreamableTopTXN`) stays seam-panic: it reads `rb->private_data`
//!   (`ReorderBufferCanStartStreaming`), the unmodeled `LogicalDecodingContext`.
//! * **streaming** — `ReorderBufferStreamTXN` / `ReorderBufferStreamCommit`.
//! * **toast reassembly** — `ReorderBufferToast*`.
//! * **cleanup / commit-time** — `ReorderBufferCleanupTXN` /
//!   `ReorderBufferTruncateTXN` / abort / forget / prepare.
//!
//! # Handle model
//!
//! `ReorderBuffer *` and `ReorderBufferTXN *` are forwarded across subsystem
//! boundaries (logical.c, snapbuild.c, slotsync, heapam_visibility) which only
//! pass them back to this owner. They are modeled as the opaque
//! [`types_logical::ReorderBufferHandle`] / [`types_logical::TxnHandle`]
//! resolved through a backend-local registry in [`registry`].

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::vec::Vec;
use std::collections::HashMap;

use types_core::primitive::{InvalidRepOriginId, RepOriginId, TimestampTz, TransactionId, XLogRecPtr};
use types_core::xact::{CommandId, InvalidCommandId, InvalidTransactionId, InvalidXLogRecPtr};
use snapshot::SnapshotData;
use types_storage::sinval::SharedInvalidationMessage;
use types_storage::RelFileLocator;
use types_tuple::ItemPointerData;

mod registry;
mod replay;
mod snapshot;
mod spill;
mod toast;
pub use registry::{init_seams, with_buffer, with_buffer_opt};
pub use snapshot::{ReorderBufferTupleCidEnt, ReorderBufferTupleCidKey, TupleCidHash};
pub use toast::ReorderBufferToastEnt;

/// `MAX_DISTR_INVAL_MSG_PER_TXN` — `(8 * 1024 * 1024) /
/// sizeof(SharedInvalidationMessage)`. Each txn caps distributed invalidation
/// messages at an 8MB budget; over it the txn is marked overflowed.
const MAX_DISTR_INVAL_MSG_PER_TXN: usize =
    (8 * 1024 * 1024) / core::mem::size_of::<SharedInvalidationMessage>();

// ---------------------------------------------------------------------------
// GUC-backed globals owned by reorderbuffer.c.
//
// `int logical_decoding_work_mem;` and
// `int debug_logical_replication_streaming = DEBUG_LOGICAL_REP_STREAMING_BUFFERED;`
// are plain process-local `int` globals defined in reorderbuffer.c (lines 225 /
// 229). The GUC machinery reads/writes them through the `config_int` /
// `config_enum` table entries' `conf->variable` pointer (guc_tables.c:2604 /
// 5418); the reorder buffer's eviction driver
// (`ReorderBufferCheckMemoryLimit`) and the streaming-vs-serialize decision
// read the current value out of the same global. So reorderbuffer.c owns the
// backing storage and installs the `conf->variable` accessors into the GUC
// slots, exactly as globals.c does for `NBuffers` etc.
//
// Modeled as backend-local `Cell`s (the C globals are per-backend); seeded to
// the C boot defaults so a read before `InitializeGUCOptions` matches C.
// ---------------------------------------------------------------------------

thread_local! {
    /// `int logical_decoding_work_mem;` (reorderbuffer.c:225). Boot value
    /// 65536 (guc_tables.c:2611) in KB.
    static LOGICAL_DECODING_WORK_MEM: core::cell::Cell<i32> = const { core::cell::Cell::new(65536) };
    /// `int debug_logical_replication_streaming = DEBUG_LOGICAL_REP_STREAMING_BUFFERED;`
    /// (reorderbuffer.c:229). `DEBUG_LOGICAL_REP_STREAMING_BUFFERED == 0`.
    static DEBUG_LOGICAL_REPLICATION_STREAMING: core::cell::Cell<i32> = const { core::cell::Cell::new(0) };
}

/// `int logical_decoding_work_mem;` — read the current GUC value.
#[inline]
pub fn logical_decoding_work_mem() -> i32 {
    LOGICAL_DECODING_WORK_MEM.with(core::cell::Cell::get)
}

/// `conf->variable` write for `logical_decoding_work_mem` (the GUC assign path).
#[inline]
pub fn set_logical_decoding_work_mem(value: i32) {
    LOGICAL_DECODING_WORK_MEM.with(|c| c.set(value));
}

/// `int debug_logical_replication_streaming;` — read the current GUC value
/// (a `DEBUG_LOGICAL_REP_STREAMING_*` enum member).
#[inline]
pub fn debug_logical_replication_streaming() -> i32 {
    DEBUG_LOGICAL_REPLICATION_STREAMING.with(core::cell::Cell::get)
}

/// `conf->variable` write for `debug_logical_replication_streaming`.
#[inline]
pub fn set_debug_logical_replication_streaming(value: i32) {
    DEBUG_LOGICAL_REPLICATION_STREAMING.with(|c| c.set(value));
}

// ---------------------------------------------------------------------------
// ReorderBufferTXN txn_flags (reorderbuffer.h)
// ---------------------------------------------------------------------------

/// `RBTXN_HAS_CATALOG_CHANGES`.
pub const RBTXN_HAS_CATALOG_CHANGES: u32 = 0x0001;
/// `RBTXN_IS_SUBXACT`.
pub const RBTXN_IS_SUBXACT: u32 = 0x0002;
/// `RBTXN_IS_SERIALIZED`.
pub const RBTXN_IS_SERIALIZED: u32 = 0x0004;
/// `RBTXN_IS_SERIALIZED_CLEAR`.
pub const RBTXN_IS_SERIALIZED_CLEAR: u32 = 0x0008;
/// `RBTXN_IS_STREAMED`.
pub const RBTXN_IS_STREAMED: u32 = 0x0010;
/// `RBTXN_HAS_PARTIAL_CHANGE`.
pub const RBTXN_HAS_PARTIAL_CHANGE: u32 = 0x0020;
/// `RBTXN_IS_PREPARED`.
pub const RBTXN_IS_PREPARED: u32 = 0x0040;
/// `RBTXN_SKIPPED_PREPARE`.
pub const RBTXN_SKIPPED_PREPARE: u32 = 0x0080;
/// `RBTXN_HAS_STREAMABLE_CHANGE`.
pub const RBTXN_HAS_STREAMABLE_CHANGE: u32 = 0x0100;
/// `RBTXN_SENT_PREPARE`.
pub const RBTXN_SENT_PREPARE: u32 = 0x0200;
/// `RBTXN_IS_COMMITTED`.
pub const RBTXN_IS_COMMITTED: u32 = 0x0400;
/// `RBTXN_IS_ABORTED`.
pub const RBTXN_IS_ABORTED: u32 = 0x0800;
/// `RBTXN_DISTR_INVAL_OVERFLOWED`.
pub const RBTXN_DISTR_INVAL_OVERFLOWED: u32 = 0x1000;
/// `RBTXN_PREPARE_STATUS_MASK` — the prepare-state flag group.
pub const RBTXN_PREPARE_STATUS_MASK: u32 =
    RBTXN_IS_PREPARED | RBTXN_SKIPPED_PREPARE | RBTXN_SENT_PREPARE;

// ---------------------------------------------------------------------------
// ReorderBufferChange (reorderbuffer.h)
// ---------------------------------------------------------------------------

/// `ReorderBufferChangeType` (reorderbuffer.h).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReorderBufferChangeType {
    Insert,
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

/// `ReorderBufferTupleBuf` (reorderbuffer.h) — a decoded tuple owned by the
/// reorder buffer.
///
/// In C this is `{ HeapTupleData tuple; Size alloc_tuple_size; HeapTupleHeaderData
/// header; char data[FLEXIBLE_ARRAY_MEMBER]; }`: a self-contained allocation in
/// the reorder buffer's own memory context (NOT the per-record decoding `'mcx`),
/// into which `decode.c`'s `DecodeXLogTuple` `memcpy`s the WAL tuple bytes. The
/// reorder buffer holds these across many WAL records and replays them later, so
/// they outlive any single `'mcx` arena — hence an owned (`'static`) byte buffer
/// rather than an `'mcx`-bound [`types_tuple::FormedTuple`]. The fixed
/// `HeapTupleData` fields (`t_len`/`t_self`/`t_tableOid`) are carried explicitly;
/// `tuple.t_data` points at the inline `data` block, modeled here as the owned
/// `data` `Vec<u8>` (the full on-disk tuple image: header + nulls bitmap +
/// user data).
#[derive(Clone, Debug, Default)]
pub struct ReorderBufferTupleBuf {
    /// `tuple.t_len` — length of the tuple image in `data`.
    pub t_len: u32,
    /// `tuple.t_self` — item pointer (origin of the tuple).
    pub t_self: ItemPointerData,
    /// `tuple.t_tableOid` — table OID.
    pub t_table_oid: types_core::Oid,
    /// The contiguous tuple image (`header` + `data[]` in C): the
    /// `HeapTupleHeaderData` bytes followed by the nulls bitmap and user-data
    /// area, exactly as `DecodeXLogTuple` lays it out. Owned by the reorder
    /// buffer.
    pub data: Vec<u8>,
}

/// Per-`action` payload of a [`ReorderBufferChange`] (the C anonymous `union`).
#[derive(Clone, Debug, Default)]
pub enum ReorderBufferChangeData {
    /// No payload yet assigned.
    #[default]
    None,
    /// `tp` — INSERT / UPDATE / DELETE / SPEC_INSERT. `oldtuple`/`newtuple` are
    /// the decoded tuple images the reorder buffer owns (C: `HeapTuple` pointing
    /// into a `ReorderBufferTupleBuf`).
    Tp {
        rlocator: RelFileLocator,
        clear_toast_afterwards: bool,
        oldtuple: Option<ReorderBufferTupleBuf>,
        newtuple: Option<ReorderBufferTupleBuf>,
    },
    /// `truncate` — REORDER_BUFFER_CHANGE_TRUNCATE.
    Truncate {
        cascade: bool,
        restart_seqs: bool,
        relids: Vec<types_core::Oid>,
    },
    /// `msg` — REORDER_BUFFER_CHANGE_MESSAGE.
    Msg {
        prefix: Vec<u8>,
        message: Vec<u8>,
    },
    /// `snapshot` — REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT.
    Snapshot(SnapshotData),
    /// `command_id` — REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID.
    CommandId(CommandId),
    /// `tuplecid` — REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID.
    TupleCid {
        locator: RelFileLocator,
        tid: ItemPointerData,
        cmin: CommandId,
        cmax: CommandId,
        combocid: CommandId,
    },
    /// `inval` — REORDER_BUFFER_CHANGE_INVALIDATION.
    Inval(Vec<SharedInvalidationMessage>),
}

/// `ReorderBufferChange` (reorderbuffer.h). The intrusive `dlist_node node` is
/// dropped — changes are owned in their txn's `changes` / `tuplecids` `Vec`.
#[derive(Debug)]
pub struct ReorderBufferChange {
    /// `XLogRecPtr lsn`.
    pub lsn: XLogRecPtr,
    /// `ReorderBufferChangeType action`.
    pub action: ReorderBufferChangeType,
    /// `RepOriginId origin_id`.
    pub origin_id: RepOriginId,
    /// `union data`.
    pub data: ReorderBufferChangeData,
}

impl ReorderBufferChange {
    /// Clone the change for yielding through the iterator. The C hands out the
    /// borrowed `ReorderBufferChange *` it owns in the txn list; our changes are
    /// owned in the txn `changes` Vec, so the iterator yields an owned copy of
    /// the same value (the original stays in the list and is freed at cleanup).
    fn shallow_clone(&self) -> ReorderBufferChange {
        ReorderBufferChange {
            lsn: self.lsn,
            action: self.action,
            origin_id: self.origin_id,
            data: self.data.clone(),
        }
    }

    /// `ReorderBufferAllocChange(rb)` — `memset(change, 0, ...)`.
    fn alloc() -> ReorderBufferChange {
        ReorderBufferChange {
            lsn: InvalidXLogRecPtr,
            // C zeroes the struct; action 0 == REORDER_BUFFER_CHANGE_INSERT.
            action: ReorderBufferChangeType::Insert,
            origin_id: InvalidRepOriginId,
            data: ReorderBufferChangeData::None,
        }
    }
}

// ---------------------------------------------------------------------------
// ReorderBufferTXN (reorderbuffer.h)
// ---------------------------------------------------------------------------

/// `ReorderBufferTXN` (reorderbuffer.h). Intrusive list links and the slab
/// allocation become owned `Vec`s / indices managed by [`ReorderBuffer`].
///
/// The parent/child relationship (C `toptxn` pointer) is modeled by
/// [`toplevel_xid`] (already in the C struct) plus the toplevel/subxact flags;
/// the foundational family only needs the toplevel lookup, performed by
/// re-resolving `toplevel_xid` through `by_txn`.
#[derive(Debug)]
pub struct ReorderBufferTXN {
    /// `bits32 txn_flags`.
    pub txn_flags: u32,
    /// `TransactionId xid`.
    pub xid: TransactionId,
    /// `TransactionId toplevel_xid` — top-level xid, if known.
    pub toplevel_xid: TransactionId,
    /// `char *gid` — set only for two-phase txns (`None` == C NULL).
    pub gid: Option<Vec<u8>>,
    /// `XLogRecPtr first_lsn`.
    pub first_lsn: XLogRecPtr,
    /// `XLogRecPtr final_lsn`.
    pub final_lsn: XLogRecPtr,
    /// `XLogRecPtr end_lsn`.
    pub end_lsn: XLogRecPtr,
    /// `XLogRecPtr restart_decoding_lsn`.
    pub restart_decoding_lsn: XLogRecPtr,
    /// `RepOriginId origin_id`.
    pub origin_id: RepOriginId,
    /// `XLogRecPtr origin_lsn`.
    pub origin_lsn: XLogRecPtr,
    /// `union xact_time` — commit / prepare / abort time (same storage).
    pub xact_time: TimestampTz,
    /// `Snapshot base_snapshot` (`None` == C NULL).
    pub base_snapshot: Option<SnapshotData>,
    /// `XLogRecPtr base_snapshot_lsn`.
    pub base_snapshot_lsn: XLogRecPtr,
    /// `Snapshot snapshot_now` (`None` == C NULL).
    pub snapshot_now: Option<SnapshotData>,
    /// `CommandId command_id`.
    pub command_id: CommandId,
    /// `uint64 nentries`.
    pub nentries: u64,
    /// `uint64 nentries_mem`.
    pub nentries_mem: u64,
    /// `dlist_head changes`.
    pub changes: Vec<ReorderBufferChange>,
    /// `dlist_head tuplecids`.
    pub tuplecids: Vec<ReorderBufferChange>,
    /// `uint64 ntuplecids`.
    pub ntuplecids: u64,
    /// `HTAB *tuplecid_hash` — `(relfilelocator, ctid) -> (cmin, cmax)` lookup
    /// built lazily by `ReorderBufferBuildTupleCidHash` for catalog-modifying
    /// txns; `None` == C NULL.
    pub tuplecid_hash: Option<crate::snapshot::TupleCidHash>,
    /// `HTAB *toast_hash` — `chunk_id -> ReorderBufferToastEnt` reassembly hash
    /// built lazily by `ReorderBufferToastAppendChunk`; `None` == C NULL.
    pub toast_hash: Option<HashMap<types_core::Oid, crate::toast::ReorderBufferToastEnt>>,
    /// `dlist_head subtxns`— xids of non-aborted subtransactions.
    pub subtxns: Vec<TransactionId>,
    /// `uint32 nsubtxns`.
    pub nsubtxns: u32,
    /// `uint32 ninvalidations` + `SharedInvalidationMessage *invalidations`.
    pub invalidations: Vec<SharedInvalidationMessage>,
    /// `uint32 ninvalidations_distributed` +
    /// `SharedInvalidationMessage *invalidations_distributed`.
    pub invalidations_distributed: Vec<SharedInvalidationMessage>,
    /// `Size size` — bytes of in-memory changes.
    pub size: usize,
    /// `Size total_size` — including subtransactions.
    pub total_size: usize,
}

impl ReorderBufferTXN {
    /// `ReorderBufferAllocTXN(rb)` — `memset(txn, 0, ...)` then the few
    /// non-zero initializers (`command_id = InvalidCommandId`).
    fn alloc() -> ReorderBufferTXN {
        ReorderBufferTXN {
            txn_flags: 0,
            xid: InvalidTransactionId,
            toplevel_xid: InvalidTransactionId,
            gid: None,
            first_lsn: InvalidXLogRecPtr,
            final_lsn: InvalidXLogRecPtr,
            end_lsn: InvalidXLogRecPtr,
            restart_decoding_lsn: InvalidXLogRecPtr,
            origin_id: InvalidRepOriginId,
            origin_lsn: InvalidXLogRecPtr,
            xact_time: 0,
            base_snapshot: None,
            base_snapshot_lsn: InvalidXLogRecPtr,
            snapshot_now: None,
            command_id: InvalidCommandId,
            nentries: 0,
            nentries_mem: 0,
            changes: Vec::new(),
            tuplecids: Vec::new(),
            ntuplecids: 0,
            tuplecid_hash: None,
            toast_hash: None,
            subtxns: Vec::new(),
            nsubtxns: 0,
            invalidations: Vec::new(),
            invalidations_distributed: Vec::new(),
            size: 0,
            total_size: 0,
        }
    }

    /// `rbtxn_has_catalog_changes(txn)`.
    pub fn has_catalog_changes(&self) -> bool {
        self.txn_flags & RBTXN_HAS_CATALOG_CHANGES != 0
    }
    /// `rbtxn_is_known_subxact(txn)`.
    pub fn is_known_subxact(&self) -> bool {
        self.txn_flags & RBTXN_IS_SUBXACT != 0
    }
    /// `rbtxn_is_aborted(txn)`.
    pub fn is_aborted(&self) -> bool {
        self.txn_flags & RBTXN_IS_ABORTED != 0
    }
    /// `rbtxn_is_prepared(txn)`.
    pub fn is_prepared(&self) -> bool {
        self.txn_flags & RBTXN_IS_PREPARED != 0
    }
    /// `rbtxn_sent_prepare(txn)`.
    pub fn sent_prepare(&self) -> bool {
        self.txn_flags & RBTXN_SENT_PREPARE != 0
    }
    /// `rbtxn_is_committed(txn)`.
    pub fn is_committed(&self) -> bool {
        self.txn_flags & RBTXN_IS_COMMITTED != 0
    }
    /// `rbtxn_distr_inval_overflowed(txn)`.
    pub fn distr_inval_overflowed(&self) -> bool {
        self.txn_flags & RBTXN_DISTR_INVAL_OVERFLOWED != 0
    }
    /// `rbtxn_is_subtxn(txn)` — `toptxn != NULL`, i.e. known subxact with a
    /// recorded top-level xid.
    pub fn is_subtxn(&self) -> bool {
        self.is_known_subxact()
    }
    /// `rbtxn_is_serialized(txn)`.
    pub fn is_serialized(&self) -> bool {
        self.txn_flags & RBTXN_IS_SERIALIZED != 0
    }
    /// `rbtxn_is_serialized_clear(txn)`.
    pub fn is_serialized_clear(&self) -> bool {
        self.txn_flags & RBTXN_IS_SERIALIZED_CLEAR != 0
    }
    /// `rbtxn_is_streamed(txn)`.
    pub fn is_streamed(&self) -> bool {
        self.txn_flags & RBTXN_IS_STREAMED != 0
    }
}

// ---------------------------------------------------------------------------
// ReorderBuffer (reorderbuffer.h)
// ---------------------------------------------------------------------------

/// `ReorderBuffer` (reorderbuffer.h). The slab/generation memory contexts and
/// the `outbuf` scratch buffer belong to families that allocate decoded tuples
/// and spill to disk; the foundational family models the txn store and the
/// three ordered lists plus the one-entry lookup cache and stat counters.
///
/// The output-plugin callbacks (`begin`/`apply_change`/...) are owned and
/// installed by `logical.c` through `wire_reorderbuffer_callbacks`; this struct
/// holds [`output_rewrites`] and [`callbacks_wired`] so the replay family can
/// later dispatch them.
pub struct ReorderBuffer {
    /// `HTAB *by_txn` — xid → owned `ReorderBufferTXN`.
    by_txn: HashMap<TransactionId, ReorderBufferTXN>,
    /// `dlist_head toplevel_by_lsn` — toplevel txn xids, first-LSN order.
    toplevel_by_lsn: Vec<TransactionId>,
    /// `dlist_head txns_by_base_snapshot_lsn` — base-snapshot-bearing txn xids,
    /// base-snapshot-LSN order.
    txns_by_base_snapshot_lsn: Vec<TransactionId>,
    /// `dclist_head catchange_txns` — catalog-modifying txn xids.
    catchange_txns: Vec<TransactionId>,
    /// `TransactionId by_txn_last_xid` — one-entry lookup cache key.
    by_txn_last_xid: TransactionId,
    /// `ReorderBufferTXN *by_txn_last_txn` — cache value: `Some(xid)` when the
    /// cache holds a live txn, `None` for a cached "does not exist".
    by_txn_last_txn: Option<TransactionId>,

    /// `bool output_rewrites`.
    output_rewrites: bool,
    /// True once `logical.c` has wired the callbacks (`private_data` set).
    callbacks_wired: bool,

    /// `XLogRecPtr current_restart_decoding_lsn`.
    current_restart_decoding_lsn: XLogRecPtr,
    /// `Size size` — total in-memory bytes.
    size: usize,

    /// Spill statistics.
    spill_txns: i64,
    spill_count: i64,
    spill_bytes: i64,
    /// Streaming statistics.
    stream_txns: i64,
    stream_count: i64,
    stream_bytes: i64,
    /// Totals.
    total_txns: i64,
    total_bytes: i64,
}

impl ReorderBuffer {
    // -----------------------------------------------------------------------
    // ReorderBufferAllocate / ReorderBufferFree
    // -----------------------------------------------------------------------

    /// `ReorderBufferAllocate(void)`.
    ///
    /// The C builds dedicated memory contexts (`change_context`, `txn_context`,
    /// `tup_context`) and a `txn_heap` pairing heap for the spill/streaming
    /// families; those structures are introduced when those families land.
    /// `ReorderBufferCleanupSerializedTXNs` (stale-spill cleanup) belongs to
    /// the spill family and is reached through [`cleanup_serialized_txns`].
    pub fn allocate() -> ReorderBuffer {
        let mut buffer = ReorderBuffer {
            by_txn: HashMap::new(),
            toplevel_by_lsn: Vec::new(),
            txns_by_base_snapshot_lsn: Vec::new(),
            catchange_txns: Vec::new(),
            by_txn_last_xid: InvalidTransactionId,
            by_txn_last_txn: None,
            output_rewrites: false,
            callbacks_wired: false,
            current_restart_decoding_lsn: InvalidXLogRecPtr,
            size: 0,
            spill_txns: 0,
            spill_count: 0,
            spill_bytes: 0,
            stream_txns: 0,
            stream_count: 0,
            stream_bytes: 0,
            total_txns: 0,
            total_bytes: 0,
        };
        // The C clears stale on-disk data here via
        // ReorderBufferCleanupSerializedTXNs(slot name); that lives in the
        // spill family. Touch the field set so it is plainly initialized.
        let _ = &mut buffer.by_txn;
        buffer
    }

    // -----------------------------------------------------------------------
    // ReorderBufferTXNByXid — xid -> txn with the one-entry cache
    // -----------------------------------------------------------------------

    /// `ReorderBufferTXNByXid(rb, xid, create, is_new, lsn, create_as_top)`.
    ///
    /// Returns the xid of the resolved txn (the C returns the `ReorderBufferTXN
    /// *`; callers then use [`with_txn`] to operate on it). `is_new` mirrors the
    /// C out-parameter. Returns `None` when not found and `create` is false.
    fn txn_by_xid(
        &mut self,
        xid: TransactionId,
        create: bool,
        is_new: &mut Option<bool>,
        lsn: XLogRecPtr,
        create_as_top: bool,
    ) -> Option<TransactionId> {
        debug_assert!(xid != InvalidTransactionId, "TransactionIdIsValid(xid)");

        // One-entry lookup cache.
        if self.by_txn_last_xid != InvalidTransactionId && self.by_txn_last_xid == xid {
            match self.by_txn_last_txn {
                Some(cached) => {
                    if let Some(slot) = is_new.as_mut() {
                        *slot = false;
                    }
                    return Some(cached);
                }
                None => {
                    // cached as non-existent
                    if !create {
                        return None;
                    }
                    // otherwise fall through to create it
                }
            }
        }

        let found = self.by_txn.contains_key(&xid);
        let resolved: Option<TransactionId> = if found {
            Some(xid)
        } else if create {
            debug_assert!(lsn != InvalidXLogRecPtr, "lsn != InvalidXLogRecPtr");
            let mut txn = ReorderBufferTXN::alloc();
            txn.xid = xid;
            txn.first_lsn = lsn;
            txn.restart_decoding_lsn = self.current_restart_decoding_lsn;
            self.by_txn.insert(xid, txn);

            if create_as_top {
                self.toplevel_by_lsn.push(xid);
                self.assert_txn_lsn_order();
            }
            Some(xid)
        } else {
            None
        };

        // update cache
        self.by_txn_last_xid = xid;
        self.by_txn_last_txn = resolved;

        if let Some(slot) = is_new.as_mut() {
            *slot = !found;
        }

        debug_assert!(!create || resolved.is_some());
        resolved
    }

    /// Immutable lookup of `xid`'s txn (`None` == C NULL).
    pub(crate) fn by_txn_get(&self, xid: TransactionId) -> Option<&ReorderBufferTXN> {
        self.by_txn.get(&xid)
    }

    /// Mutable lookup of `xid`'s txn (`None` == C NULL).
    pub(crate) fn by_txn_get_mut(&mut self, xid: TransactionId) -> Option<&mut ReorderBufferTXN> {
        self.by_txn.get_mut(&xid)
    }

    /// Borrow the live txn for `xid` (the C dereferences the `ReorderBufferTXN
    /// *` for the same span).
    fn with_txn<R>(&mut self, xid: TransactionId, f: impl FnOnce(&mut ReorderBufferTXN) -> R) -> R {
        let txn = self
            .by_txn
            .get_mut(&xid)
            .expect("ReorderBufferTXN missing for xid");
        f(txn)
    }

    /// `AssertTXNLsnOrder(rb)` — assertion-only LSN-ordering invariant on the
    /// toplevel / base-snapshot lists. The C body is `#ifdef
    /// USE_ASSERT_CHECKING`; we mirror it under `debug_assertions`. The C early
    /// return on `SnapBuildXactNeedsSkip` guards against not-yet-associated
    /// sub/top txns sharing an LSN; that check needs the decoding context and
    /// is the responsibility of the change-replay family, so we conservatively
    /// only assert the *non-decreasing* base-snapshot order (which holds at all
    /// times) and skip the strict toplevel `first_lsn` check.
    fn assert_txn_lsn_order(&self) {
        #[cfg(debug_assertions)]
        {
            let mut prev_base = InvalidXLogRecPtr;
            for xid in &self.txns_by_base_snapshot_lsn {
                let txn = match self.by_txn.get(xid) {
                    Some(t) => t,
                    None => continue,
                };
                debug_assert!(txn.base_snapshot.is_some());
                debug_assert!(txn.base_snapshot_lsn != InvalidXLogRecPtr);
                if prev_base != InvalidXLogRecPtr {
                    debug_assert!(prev_base <= txn.base_snapshot_lsn);
                }
                debug_assert!(!txn.is_known_subxact());
                prev_base = txn.base_snapshot_lsn;
            }
        }
    }

    /// Resolve `xid`'s top-level txn xid: a known subxact maps to its
    /// `toplevel_xid`, otherwise itself (`rbtxn_get_toptxn`).
    fn toptxn_xid(&self, xid: TransactionId) -> TransactionId {
        match self.by_txn.get(&xid) {
            Some(t) if t.is_known_subxact() => t.toplevel_xid,
            _ => xid,
        }
    }

    // -----------------------------------------------------------------------
    // ReorderBufferGetOldestTXN / GetOldestXmin / SetRestartPoint
    // -----------------------------------------------------------------------

    /// `ReorderBufferGetOldestTXN(rb)` — the oldest toplevel txn's xid.
    pub fn get_oldest_txn(&self) -> Option<TransactionId> {
        self.assert_txn_lsn_order();
        let &xid = self.toplevel_by_lsn.first()?;
        debug_assert!(self
            .by_txn
            .get(&xid)
            .map(|t| !t.is_known_subxact())
            .unwrap_or(true));
        Some(xid)
    }

    /// `ReorderBufferGetOldestXmin(rb)`.
    pub fn get_oldest_xmin(&self) -> TransactionId {
        self.assert_txn_lsn_order();
        match self.txns_by_base_snapshot_lsn.first() {
            None => InvalidTransactionId,
            Some(xid) => self
                .by_txn
                .get(xid)
                .and_then(|t| t.base_snapshot.as_ref())
                .map(|s| s.xmin)
                .unwrap_or(InvalidTransactionId),
        }
    }

    /// `ReorderBufferSetRestartPoint(rb, ptr)`.
    pub fn set_restart_point(&mut self, ptr: XLogRecPtr) {
        self.current_restart_decoding_lsn = ptr;
    }

    // -----------------------------------------------------------------------
    // ReorderBufferProcessXid
    // -----------------------------------------------------------------------

    /// `ReorderBufferProcessXid(rb, xid, lsn)`.
    pub fn process_xid(&mut self, xid: TransactionId, lsn: XLogRecPtr) {
        if xid != InvalidTransactionId {
            self.txn_by_xid(xid, true, &mut None, lsn, true);
        }
    }

    // -----------------------------------------------------------------------
    // Queueing helpers (ReorderBufferQueueChange and its callers)
    // -----------------------------------------------------------------------

    /// `ReorderBufferQueueChange(rb, xid, lsn, change, toast_insert)`.
    ///
    /// The memory-accounting (`ReorderBufferChangeMemoryUpdate` /
    /// `ReorderBufferChangeSize`), partial-change tracking
    /// (`ReorderBufferProcessPartialChange`) and eviction
    /// (`ReorderBufferCheckMemoryLimit`) steps belong to the spill family and
    /// are reached through their crate-internal entry points, which panic until
    /// that family lands. The foundational queueing — the abort short-circuit,
    /// streamable-change flag, list append and entry counters — is ported here.
    pub(crate) fn queue_change(
        &mut self,
        xid: TransactionId,
        lsn: XLogRecPtr,
        mut change: ReorderBufferChange,
        toast_insert: bool,
    ) {
        let txn_xid = self
            .txn_by_xid(xid, true, &mut None, lsn, true)
            .expect("create == true yields a txn");

        if self.with_txn(txn_xid, |t| t.is_aborted()) {
            // Change not yet queued; no memory accounting to undo.
            return;
        }

        if matches!(
            change.action,
            ReorderBufferChangeType::Insert
                | ReorderBufferChangeType::Update
                | ReorderBufferChangeType::Delete
                | ReorderBufferChangeType::InternalSpecInsert
                | ReorderBufferChangeType::Truncate
                | ReorderBufferChangeType::Message
        ) {
            let top = self.toptxn_xid(txn_xid);
            self.with_txn(top, |t| t.txn_flags |= RBTXN_HAS_STREAMABLE_CHANGE);
        }

        change.lsn = lsn;
        debug_assert!(lsn != InvalidXLogRecPtr);

        let size = change_size(&change);
        self.with_txn(txn_xid, |t| {
            t.changes.push(change);
            t.nentries += 1;
            t.nentries_mem += 1;
        });

        // update memory accounting information
        self.change_memory_update_add(txn_xid, size);

        // process partial change
        self.process_partial_change(txn_xid, toast_insert);

        // check the memory limits and evict something if needed
        self.check_memory_limit();
    }

    // -----------------------------------------------------------------------
    // ReorderBufferAddSnapshot / AddNewCommandId / AddNewTupleCids
    // -----------------------------------------------------------------------

    /// `ReorderBufferAddSnapshot(rb, xid, lsn, snap)`.
    pub fn add_snapshot(&mut self, xid: TransactionId, lsn: XLogRecPtr, snap: SnapshotData) {
        let mut change = ReorderBufferChange::alloc();
        change.data = ReorderBufferChangeData::Snapshot(snap);
        change.action = ReorderBufferChangeType::InternalSnapshot;
        self.queue_change(xid, lsn, change, false);
    }

    /// `ReorderBufferSetBaseSnapshot(rb, xid, lsn, snap)`.
    pub fn set_base_snapshot(&mut self, xid: TransactionId, lsn: XLogRecPtr, snap: SnapshotData) {
        let mut is_new = None;
        let mut txn_xid = self
            .txn_by_xid(xid, true, &mut is_new, lsn, true)
            .expect("create == true yields a txn");

        if self.with_txn(txn_xid, |t| t.is_known_subxact()) {
            let top = self.with_txn(txn_xid, |t| t.toplevel_xid);
            txn_xid = self
                .txn_by_xid(top, false, &mut None, InvalidXLogRecPtr, false)
                .expect("toplevel txn of a known subxact exists");
        }

        debug_assert!(self.with_txn(txn_xid, |t| t.base_snapshot.is_none()));
        self.with_txn(txn_xid, |t| {
            t.base_snapshot = Some(snap);
            t.base_snapshot_lsn = lsn;
        });
        self.txns_by_base_snapshot_lsn.push(txn_xid);
        self.assert_txn_lsn_order();
    }

    /// `ReorderBufferAddNewCommandId(rb, xid, lsn, cid)`.
    pub fn add_new_command_id(&mut self, xid: TransactionId, lsn: XLogRecPtr, cid: CommandId) {
        let mut change = ReorderBufferChange::alloc();
        change.data = ReorderBufferChangeData::CommandId(cid);
        change.action = ReorderBufferChangeType::InternalCommandId;
        self.queue_change(xid, lsn, change, false);
    }

    /// `ReorderBufferAddNewTupleCids(rb, xid, lsn, locator, tid, cmin, cmax,
    /// combocid)`.
    #[allow(clippy::too_many_arguments)]
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
        let txn_xid = self
            .txn_by_xid(xid, true, &mut None, lsn, true)
            .expect("create == true yields a txn");

        let mut change = ReorderBufferChange::alloc();
        change.data = ReorderBufferChangeData::TupleCid {
            locator,
            tid,
            cmin,
            cmax,
            combocid,
        };
        change.lsn = lsn;
        change.action = ReorderBufferChangeType::InternalTupleCid;

        self.with_txn(txn_xid, |t| {
            t.tuplecids.push(change);
            t.ntuplecids += 1;
        });
    }

    // -----------------------------------------------------------------------
    // Invalidation accumulation
    // -----------------------------------------------------------------------

    /// `ReorderBufferQueueInvalidations(rb, xid, lsn, nmsgs, msgs)`.
    fn queue_invalidations(
        &mut self,
        xid: TransactionId,
        lsn: XLogRecPtr,
        msgs: &[SharedInvalidationMessage],
    ) {
        let mut change = ReorderBufferChange::alloc();
        change.action = ReorderBufferChangeType::Invalidation;
        change.data = ReorderBufferChangeData::Inval(msgs.to_vec());
        self.queue_change(xid, lsn, change, false);
    }

    /// `ReorderBufferAddDistributedInvalidations(rb, xid, lsn, nmsgs, msgs)`.
    pub fn add_distributed_invalidations(
        &mut self,
        xid: TransactionId,
        lsn: XLogRecPtr,
        msgs: Vec<SharedInvalidationMessage>,
    ) {
        let txn_xid = self
            .txn_by_xid(xid, true, &mut None, lsn, true)
            .expect("create == true yields a txn");
        // Collect under the top transaction.
        let top = self.toptxn_xid(txn_xid);
        debug_assert!(!msgs.is_empty(), "nmsgs > 0");

        let overflowed = self.with_txn(top, |t| t.distr_inval_overflowed());
        if !overflowed {
            let (cur_len, will_overflow) = self.with_txn(top, |t| {
                (
                    t.invalidations_distributed.len(),
                    t.invalidations_distributed.len() + msgs.len() >= MAX_DISTR_INVAL_MSG_PER_TXN,
                )
            });
            let _ = cur_len;
            if will_overflow {
                self.with_txn(top, |t| {
                    t.txn_flags |= RBTXN_DISTR_INVAL_OVERFLOWED;
                    t.invalidations_distributed.clear();
                });
            } else {
                self.with_txn(top, |t| {
                    accumulate_invalidations(&mut t.invalidations_distributed, &msgs);
                });
            }
        }

        // Queue the invalidation messages into the transaction.
        self.queue_invalidations(xid, lsn, &msgs);
    }

    // -----------------------------------------------------------------------
    // Public change-replay entry points (consumed by decode.c)
    // -----------------------------------------------------------------------

    /// `ReorderBufferQueueChange(rb, xid, lsn, change, toast_insert)` for a
    /// decoded heap `tp` change. decode.c can't construct the owner-private
    /// `ReorderBufferChange`, so the seam conveys the change discriminant, the
    /// relation locator and the decoded old/new tuple images; this method
    /// assembles the change and forwards to the internal queueing path.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn queue_decoded_change(
        &mut self,
        xid: TransactionId,
        lsn: XLogRecPtr,
        kind: reorderbuffer_seams::DecodedChangeKind,
        rlocator: RelFileLocator,
        oldtuple: Option<reorderbuffer_seams::DecodedTuple>,
        newtuple: Option<reorderbuffer_seams::DecodedTuple>,
        toast_insert: bool,
    ) {
        use reorderbuffer_seams::DecodedChangeKind as K;
        let action = match kind {
            K::Insert => ReorderBufferChangeType::Insert,
            K::Update => ReorderBufferChangeType::Update,
            K::Delete => ReorderBufferChangeType::Delete,
            K::SpecInsert => ReorderBufferChangeType::InternalSpecInsert,
            K::SpecConfirm => ReorderBufferChangeType::InternalSpecConfirm,
            K::SpecAbort => ReorderBufferChangeType::InternalSpecAbort,
            K::Truncate => ReorderBufferChangeType::Truncate,
        };
        // C `clear_toast_afterwards` defaults to true: ReorderBufferToastReplace
        // resets it for the toast assembly chain. The toast family owns that
        // reset; the foundational image keeps the C default.
        let mut change = ReorderBufferChange::alloc();
        change.action = action;
        change.data = ReorderBufferChangeData::Tp {
            rlocator,
            clear_toast_afterwards: true,
            oldtuple: oldtuple.map(decoded_tuple_to_buf),
            newtuple: newtuple.map(decoded_tuple_to_buf),
        };
        self.queue_change(xid, lsn, change, toast_insert);
    }

    /// `ReorderBufferQueueChange(rb, xid, lsn, change, false)` for a
    /// `REORDER_BUFFER_CHANGE_TRUNCATE` change (decode.c:DecodeTruncate builds
    /// the `truncate` payload and queues it). Separate seam because the payload
    /// is shaped differently from the per-tuple `tp` change.
    pub(crate) fn queue_truncate(
        &mut self,
        xid: TransactionId,
        lsn: XLogRecPtr,
        cascade: bool,
        restart_seqs: bool,
        relids: Vec<types_core::Oid>,
    ) {
        let mut change = ReorderBufferChange::alloc();
        change.action = ReorderBufferChangeType::Truncate;
        change.data = ReorderBufferChangeData::Truncate {
            cascade,
            restart_seqs,
            relids,
        };
        self.queue_change(xid, lsn, change, false);
    }

    /// `ReorderBufferQueueMessage(rb, xid, snap, lsn, transactional, prefix,
    /// message_size, message)`. The transactional message is queued to be
    /// processed on commit; the non-transactional path replays the message
    /// immediately through the output plugin's `message` callback under a
    /// historic snapshot.
    pub(crate) fn queue_message(
        &mut self,
        xid: TransactionId,
        lsn: XLogRecPtr,
        transactional: bool,
        prefix: Vec<u8>,
        message: Vec<u8>,
    ) {
        if transactional {
            debug_assert!(xid != InvalidTransactionId);
            let mut change = ReorderBufferChange::alloc();
            change.action = ReorderBufferChangeType::Message;
            change.data = ReorderBufferChangeData::Msg { prefix, message };
            self.queue_change(xid, lsn, change, false);
        } else {
            // Non-transactional changes require a valid snapshot and are
            // replayed immediately through the output plugin `message`
            // callback (rb->message) under SetupHistoricSnapshot. The seam
            // doesn't carry the snapshot and the output-plugin dispatch is
            // unported (logical.c handle facade), so this path panics.
            let _ = (xid, lsn, prefix, message);
            panic!(
                "ReorderBufferQueueMessage non-transactional path: rb->message \
                 output-plugin callback dispatch + SetupHistoricSnapshot(snap) \
                 not yet modeled (logical.c handle facade)"
            );
        }
    }

    /// `ReorderBufferAddInvalidations(rb, xid, lsn, nmsgs, msgs)` — accumulate
    /// the txn's cache invalidations under its top transaction and queue them.
    pub(crate) fn add_invalidations(
        &mut self,
        xid: TransactionId,
        lsn: XLogRecPtr,
        msgs: Vec<SharedInvalidationMessage>,
    ) {
        let txn_xid = self
            .txn_by_xid(xid, true, &mut None, lsn, true)
            .expect("create == true yields a txn");

        // Collect all invalidations under the top transaction.
        let top = self.toptxn_xid(txn_xid);
        debug_assert!(!msgs.is_empty(), "nmsgs > 0");

        self.with_txn(top, |t| {
            accumulate_invalidations(&mut t.invalidations, &msgs);
        });

        self.queue_invalidations(xid, lsn, &msgs);
    }

    /// `ReorderBufferImmediateInvalidation(rb, ninvalidations, invalidations)`
    /// — execute cache invalidations outside the context of a decoded
    /// transaction (xid-less commits, or uninteresting transactions via
    /// `ReorderBufferForget`). Invalidations are forced to happen outside a
    /// valid transaction so entries are just marked invalid without catalog
    /// access.
    pub(crate) fn immediate_invalidation(&mut self, invalidations: &[SharedInvalidationMessage]) {
        let use_subtxn =
            transam_xact_seams::is_transaction_or_transaction_block::call();

        if use_subtxn {
            transam_xact_seams::begin_internal_sub_transaction::call(Some("replay"))
                .expect("BeginInternalSubTransaction(\"replay\")");
        }

        // Force invalidations to happen outside of a valid transaction.
        if use_subtxn {
            transam_xact_seams::abort_current_transaction::call()
                .expect("AbortCurrentTransaction");
        }

        for msg in invalidations {
            inval_seams::local_execute_invalidation_message::call(msg)
                .expect("LocalExecuteInvalidationMessage");
        }

        if use_subtxn {
            transam_xact_seams::rollback_and_release_current_sub_transaction::call()
                .expect("RollbackAndReleaseCurrentSubTransaction");
        }
    }

    /// `ReorderBufferAbort(rb, xid, lsn, abort_time)` — abort a transaction and
    /// its subtransactions.
    pub(crate) fn abort(
        &mut self,
        xid: TransactionId,
        lsn: XLogRecPtr,
        abort_time: TimestampTz,
    ) {
        let txn_xid =
            match self.txn_by_xid(xid, false, &mut None, InvalidXLogRecPtr, false) {
                None => return, // unknown, nothing to remove
                Some(x) => x,
            };

        self.with_txn(txn_xid, |t| t.xact_time = abort_time);

        // For streamed transactions notify the remote node about the abort.
        if self.with_txn(txn_xid, |t| t.is_streamed()) {
            self.stream_abort_output(txn_xid, lsn);

            // Execute the inval messages so future transactions don't reuse
            // this txn's (possibly DDL-poisoned) cache entries.
            let ninval = self.with_txn(txn_xid, |t| t.invalidations.len());
            if ninval > 0 {
                let msgs = self.with_txn(txn_xid, |t| t.invalidations.clone());
                self.immediate_invalidation(&msgs);
            }
        }

        // cosmetic...
        self.with_txn(txn_xid, |t| t.final_lsn = lsn);

        // remove potential on-disk data, and deallocate
        self.cleanup_txn(txn_xid);
    }

    /// `ReorderBufferAbortOld(rb, oldestRunningXid)` — abort all toplevel txns
    /// older than `oldest_running_xid` (server crash/immediate restart cleanup;
    /// no invalidation handling here).
    pub(crate) fn abort_old(&mut self, oldest_running_xid: TransactionId) {
        // Iterate toplevel txns in LSN order, aborting all older than what can
        // possibly still be running; stop at the first live one.
        let candidates: Vec<TransactionId> = self.toplevel_by_lsn.clone();
        for txn_xid in candidates {
            // The toplevel list holds xids; resolve to the live txn.
            if self.by_txn_get(txn_xid).is_none() {
                continue;
            }
            if transaction_id_precedes(txn_xid, oldest_running_xid) {
                // Notify the remote node about the crash/immediate restart.
                if self.with_txn(txn_xid, |t| t.is_streamed()) {
                    self.stream_abort_output(txn_xid, InvalidXLogRecPtr);
                }
                self.cleanup_txn(txn_xid);
            } else {
                return;
            }
        }
    }

    /// `ReorderBufferForget(rb, xid, lsn)` — discard a transaction we aren't
    /// interested in (committed but uninteresting), still applying its cache
    /// invalidations. Must be called after the commit record is read.
    pub(crate) fn forget(&mut self, xid: TransactionId, lsn: XLogRecPtr) {
        let txn_xid =
            match self.txn_by_xid(xid, false, &mut None, InvalidXLogRecPtr, false) {
                None => return, // unknown, nothing to forget
                Some(x) => x,
            };

        // this transaction mustn't be streamed
        debug_assert!(!self.with_txn(txn_xid, |t| t.is_streamed()));

        // cosmetic...
        self.with_txn(txn_xid, |t| t.final_lsn = lsn);

        // Process only cache invalidation messages (the txn could have
        // manipulated the catalog).
        let (has_base, ninval) =
            self.with_txn(txn_xid, |t| (t.base_snapshot.is_some(), t.invalidations.len()));
        if has_base && ninval > 0 {
            let msgs = self.with_txn(txn_xid, |t| t.invalidations.clone());
            self.immediate_invalidation(&msgs);
        } else {
            debug_assert!(ninval == 0);
        }

        // remove potential on-disk data, and deallocate
        self.cleanup_txn(txn_xid);
    }

    /// `ReorderBufferInvalidate(rb, xid, lsn)` — execute the txn's accumulated
    /// cache invalidations without replaying its changes and *without* cleaning
    /// up the txn (a prepared txn we decided to skip, see `DecodePrepare`).
    pub(crate) fn invalidate(&mut self, xid: TransactionId, _lsn: XLogRecPtr) {
        let txn_xid =
            match self.txn_by_xid(xid, false, &mut None, InvalidXLogRecPtr, false) {
                None => return, // unknown, nothing to do
                Some(x) => x,
            };

        let (has_base, ninval) =
            self.with_txn(txn_xid, |t| (t.base_snapshot.is_some(), t.invalidations.len()));
        if has_base && ninval > 0 {
            let msgs = self.with_txn(txn_xid, |t| t.invalidations.clone());
            self.immediate_invalidation(&msgs);
        } else {
            debug_assert!(ninval == 0);
        }
    }

    /// `ReorderBufferRememberPrepareInfo(rb, xid, prepare_lsn, end_lsn,
    /// prepare_time, origin_id, origin_lsn)` — stash the metadata needed to
    /// later replay the prepared transaction's commit/abort. Returns whether
    /// the prepare should proceed (false if the txn is unknown).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn remember_prepare_info(
        &mut self,
        xid: TransactionId,
        prepare_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
        prepare_time: TimestampTz,
        origin_id: RepOriginId,
        origin_lsn: XLogRecPtr,
    ) -> bool {
        let txn_xid =
            match self.txn_by_xid(xid, false, &mut None, InvalidXLogRecPtr, false) {
                None => return false, // unknown transaction, nothing to do
                Some(x) => x,
            };

        self.with_txn(txn_xid, |t| {
            t.final_lsn = prepare_lsn;
            t.end_lsn = end_lsn;
            t.xact_time = prepare_time;
            t.origin_id = origin_id;
            t.origin_lsn = origin_lsn;
            debug_assert!(t.txn_flags & RBTXN_PREPARE_STATUS_MASK == 0);
            t.txn_flags |= RBTXN_IS_PREPARED;
        });
        true
    }

    /// `ReorderBufferSkipPrepare(rb, xid)` — mark that the prepare for `xid`
    /// was skipped (the plugin's `filter_prepare_cb` returned true).
    pub(crate) fn skip_prepare(&mut self, xid: TransactionId) {
        let txn_xid =
            match self.txn_by_xid(xid, false, &mut None, InvalidXLogRecPtr, false) {
                None => return, // unknown transaction, nothing to do
                Some(x) => x,
            };
        self.with_txn(txn_xid, |t| {
            debug_assert!(t.txn_flags & RBTXN_PREPARE_STATUS_MASK == RBTXN_IS_PREPARED);
            t.txn_flags |= RBTXN_SKIPPED_PREPARE;
        });
    }

    // -----------------------------------------------------------------------
    // Catalog-change / base-snapshot accessors
    // -----------------------------------------------------------------------

    /// `ReorderBufferXidSetCatalogChanges(rb, xid, lsn)`.
    pub fn xid_set_catalog_changes(&mut self, xid: TransactionId, lsn: XLogRecPtr) {
        let txn_xid = self
            .txn_by_xid(xid, true, &mut None, lsn, true)
            .expect("create == true yields a txn");

        if !self.with_txn(txn_xid, |t| t.has_catalog_changes()) {
            self.with_txn(txn_xid, |t| t.txn_flags |= RBTXN_HAS_CATALOG_CHANGES);
            self.catchange_txns.push(txn_xid);
        }

        // Mark the top-level transaction too, if this is a subxact.
        if self.with_txn(txn_xid, |t| t.is_subtxn()) {
            let top = self.toptxn_xid(txn_xid);
            if !self.with_txn(top, |t| t.has_catalog_changes()) {
                self.with_txn(top, |t| t.txn_flags |= RBTXN_HAS_CATALOG_CHANGES);
                self.catchange_txns.push(top);
            }
        }
    }

    /// `ReorderBufferGetCatalogChangesXacts(rb)` — sorted (`xidComparator`).
    pub fn get_catalog_changes_xacts(&self) -> Vec<TransactionId> {
        if self.catchange_txns.is_empty() {
            return Vec::new();
        }
        let mut xids: Vec<TransactionId> = self
            .catchange_txns
            .iter()
            .map(|xid| {
                debug_assert!(self
                    .by_txn
                    .get(xid)
                    .map(|t| t.has_catalog_changes())
                    .unwrap_or(true));
                *xid
            })
            .collect();
        // xidComparator (xid.c): plain unsigned compare.
        xids.sort_by(|a, b| a.cmp(b));
        xids
    }

    /// `dclist_count(&rb->catchange_txns)`.
    pub fn catchange_count(&self) -> usize {
        self.catchange_txns.len()
    }

    /// `ReorderBufferXidHasCatalogChanges(rb, xid)`.
    pub fn xid_has_catalog_changes(&mut self, xid: TransactionId) -> bool {
        match self.txn_by_xid(xid, false, &mut None, InvalidXLogRecPtr, false) {
            None => false,
            Some(x) => self.with_txn(x, |t| t.has_catalog_changes()),
        }
    }

    /// `ReorderBufferXidHasBaseSnapshot(rb, xid)`.
    pub fn xid_has_base_snapshot(&mut self, xid: TransactionId) -> bool {
        let txn_xid = match self.txn_by_xid(xid, false, &mut None, InvalidXLogRecPtr, false) {
            None => return false,
            Some(x) => x,
        };
        let txn_xid = if self.with_txn(txn_xid, |t| t.is_known_subxact()) {
            let top = self.with_txn(txn_xid, |t| t.toplevel_xid);
            match self.txn_by_xid(top, false, &mut None, InvalidXLogRecPtr, false) {
                None => return false,
                Some(x) => x,
            }
        } else {
            txn_xid
        };
        self.with_txn(txn_xid, |t| t.base_snapshot.is_some())
    }

    /// `ReorderBufferGetInvalidations(rb, xid, &msgs)`.
    pub fn get_invalidations(&mut self, xid: TransactionId) -> Vec<SharedInvalidationMessage> {
        match self.txn_by_xid(xid, false, &mut None, InvalidXLogRecPtr, false) {
            None => Vec::new(),
            Some(x) => self.with_txn(x, |t| t.invalidations.clone()),
        }
    }

    /// `rb->current_restart_decoding_lsn`.
    pub fn current_restart_decoding_lsn(&self) -> XLogRecPtr {
        self.current_restart_decoding_lsn
    }

    /// Iterate `rb->toplevel_by_lsn` in order, returning each toplevel txn xid.
    pub fn toplevel_txns(&self) -> Vec<TransactionId> {
        self.toplevel_by_lsn.clone()
    }

    /// `txn->xid` (identity; the handle layer already keys by xid).
    pub fn txn_xid(&self, xid: TransactionId) -> TransactionId {
        xid
    }

    /// `txn->restart_decoding_lsn`.
    pub fn txn_restart_decoding_lsn(&self, xid: TransactionId) -> XLogRecPtr {
        self.by_txn
            .get(&xid)
            .map(|t| t.restart_decoding_lsn)
            .unwrap_or(InvalidXLogRecPtr)
    }

    /// `rbtxn_is_prepared(txn)`.
    pub fn txn_is_prepared(&self, xid: TransactionId) -> bool {
        self.by_txn.get(&xid).map(|t| t.is_prepared()).unwrap_or(false)
    }

    /// `txn->gid` — the 2PC commit GID bytes (NUL-stripped), or empty when
    /// unset. `Some` only when `xid` names a live txn in this buffer.
    pub fn txn_gid(&self, xid: TransactionId) -> Option<Vec<u8>> {
        self.by_txn.get(&xid).map(|t| t.gid.clone().unwrap_or_default())
    }

    /// `txn->xact_time` (commit/prepare/abort time union). `Some` only when
    /// `xid` names a live txn in this buffer.
    pub fn txn_xact_time(&self, xid: TransactionId) -> Option<TimestampTz> {
        self.by_txn.get(&xid).map(|t| t.xact_time)
    }

    // -----------------------------------------------------------------------
    // Output-plugin wiring + statistics (logical.c seams)
    // -----------------------------------------------------------------------

    /// Mark the output-plugin callbacks as wired (logical.c installs the real
    /// trampolines and sets `private_data`).
    pub fn wire_callbacks(&mut self) {
        self.callbacks_wired = true;
    }

    /// `rb->output_rewrites = value`.
    pub fn set_output_rewrites(&mut self, value: bool) {
        self.output_rewrites = value;
    }

    /// `rb->output_rewrites` — whether the output plugin asked to receive the
    /// transient heaps created during DDL rewrites (the change-replay screening
    /// reads it to decide whether to skip `relrewrite` relations).
    pub(crate) fn output_rewrites_pub(&self) -> bool {
        self.output_rewrites
    }

    /// Read the eight stat counters (`UpdateDecodingStats`).
    pub fn stats(&self) -> types_logical::ReorderBufferStats {
        types_logical::ReorderBufferStats {
            spill_txns: self.spill_txns,
            spill_count: self.spill_count,
            spill_bytes: self.spill_bytes,
            stream_txns: self.stream_txns,
            stream_count: self.stream_count,
            stream_bytes: self.stream_bytes,
            total_txns: self.total_txns,
            total_bytes: self.total_bytes,
        }
    }

    /// Zero the eight stat counters after reporting.
    pub fn reset_stats(&mut self) {
        self.spill_txns = 0;
        self.spill_count = 0;
        self.spill_bytes = 0;
        self.stream_txns = 0;
        self.stream_count = 0;
        self.stream_bytes = 0;
        self.total_txns = 0;
        self.total_bytes = 0;
    }

    // -----------------------------------------------------------------------
    // Spill / partial-change family entry points (not yet ported)
    // -----------------------------------------------------------------------

    /// `ReorderBufferChangeMemoryUpdate(rb, change, NULL, true, sz)` — the
    /// addition path. Belongs to the spill family (it also reorders the
    /// `txn_heap` pairing heap and accumulates `rb->size`). Until that family
    /// lands the foundational queueing still records the per-change size on the
    /// txn so the data is not lost, but the heap/global accounting is deferred.
    pub(crate) fn change_memory_update_add(&mut self, txn_xid: TransactionId, sz: usize) {
        self.with_txn(txn_xid, |t| {
            t.size += sz;
            t.total_size += sz;
        });
        self.size += sz;
    }

    /// `ReorderBufferProcessPartialChange(rb, txn, change, toast_insert)` —
    /// spill/streaming family; not reachable in the foundational paths
    /// (snapbuild queues only snapshot / command-id / invalidation changes,
    /// none of which set `toast_insert` or partial-change state).
    fn process_partial_change(&mut self, _txn_xid: TransactionId, toast_insert: bool) {
        if toast_insert {
            panic!(
                "ReorderBufferProcessPartialChange: toast/partial-change path \
                 not yet ported (spill/streaming family)"
            );
        }
    }

    /// `ReorderBufferCheckMemoryLimit(rb)` — spill/streaming eviction family.
    /// The foundational family never exceeds the limit (the internal changes
    /// snapbuild queues are tiny and decoding flushes them on commit), so this
    /// is a no-op here; the real eviction lands with the spill family.
    fn check_memory_limit(&mut self) {
        // No-op until the spill family ports the eviction loop.
    }

    // -----------------------------------------------------------------------
    // Change-replay support: txn-access + list manipulation reused by replay.rs
    // -----------------------------------------------------------------------

    /// `ReorderBufferTXNByXid` exposed to the change-replay module.
    pub(crate) fn txn_by_xid_pub(
        &mut self,
        xid: TransactionId,
        create: bool,
        is_new: &mut Option<bool>,
        lsn: XLogRecPtr,
        create_as_top: bool,
    ) -> Option<TransactionId> {
        self.txn_by_xid(xid, create, is_new, lsn, create_as_top)
    }

    /// Borrow the live txn for `xid` (change-replay module access to `with_txn`).
    pub(crate) fn with_txn_pub<R>(
        &mut self,
        xid: TransactionId,
        f: impl FnOnce(&mut ReorderBufferTXN) -> R,
    ) -> R {
        self.with_txn(xid, f)
    }

    /// `AssertTXNLsnOrder(rb)` for the change-replay module.
    pub(crate) fn assert_txn_lsn_order_pub(&self) {
        self.assert_txn_lsn_order();
    }

    /// `AssertChangeLsnOrder(txn)` — assertion-only ordering of a txn's changes.
    pub(crate) fn assert_change_lsn_order(&self, xid: TransactionId) {
        #[cfg(debug_assertions)]
        {
            let txn = match self.by_txn.get(&xid) {
                Some(t) => t,
                None => return,
            };
            let mut prev_lsn = txn.first_lsn;
            for change in &txn.changes {
                debug_assert!(txn.first_lsn != InvalidXLogRecPtr);
                debug_assert!(change.lsn != InvalidXLogRecPtr);
                debug_assert!(txn.first_lsn <= change.lsn);
                if txn.end_lsn != InvalidXLogRecPtr {
                    debug_assert!(change.lsn <= txn.end_lsn);
                }
                debug_assert!(prev_lsn <= change.lsn);
                prev_lsn = change.lsn;
            }
        }
        #[cfg(not(debug_assertions))]
        let _ = xid;
    }

    /// `dlist_delete(&txn->node)` from the toplevel-by-LSN list.
    pub(crate) fn toplevel_by_lsn_remove(&mut self, xid: TransactionId) {
        if let Some(pos) = self.toplevel_by_lsn.iter().position(|&x| x == xid) {
            self.toplevel_by_lsn.remove(pos);
        }
    }

    /// `dlist_delete(&subtxn->node)` from a top-level txn's subtxn list.
    pub(crate) fn subtxns_remove(&mut self, top_xid: TransactionId, sub_xid: TransactionId) {
        if let Some(t) = self.by_txn.get_mut(&top_xid) {
            if let Some(pos) = t.subtxns.iter().position(|&x| x == sub_xid) {
                t.subtxns.remove(pos);
            }
        }
    }

    /// `dlist_delete(&txn->base_snapshot_node)` from the base-snapshot list.
    pub(crate) fn txns_by_base_snapshot_lsn_remove(&mut self, xid: TransactionId) {
        if let Some(pos) = self.txns_by_base_snapshot_lsn.iter().position(|&x| x == xid) {
            self.txns_by_base_snapshot_lsn.remove(pos);
        }
    }

    /// `dlist_insert_before(&before->base_snapshot_node, &xid->...)` — place
    /// `xid` immediately before `before` in the base-snapshot list (used by
    /// `ReorderBufferTransferSnapToParent` to move a top txn into the position
    /// previously held by its subxact).
    pub(crate) fn txns_by_base_snapshot_lsn_insert_before(
        &mut self,
        before: TransactionId,
        xid: TransactionId,
    ) {
        let pos = self
            .txns_by_base_snapshot_lsn
            .iter()
            .position(|&x| x == before)
            .unwrap_or(self.txns_by_base_snapshot_lsn.len());
        self.txns_by_base_snapshot_lsn.insert(pos, xid);
    }

    /// `dclist_delete_from(&rb->catchange_txns, &txn->catchange_node)`.
    pub(crate) fn catchange_remove(&mut self, xid: TransactionId) {
        if let Some(pos) = self.catchange_txns.iter().position(|&x| x == xid) {
            self.catchange_txns.remove(pos);
        }
    }

    /// `hash_search(rb->by_txn, &txn->xid, HASH_REMOVE, &found)`.
    pub(crate) fn by_txn_remove(&mut self, xid: TransactionId) {
        let removed = self.by_txn.remove(&xid);
        debug_assert!(removed.is_some(), "found");
    }

    /// Clear the one-entry lookup cache if it points at `xid`
    /// (`ReorderBufferFreeTXN`'s `by_txn_last_*` reset).
    pub(crate) fn invalidate_by_txn_cache(&mut self, xid: TransactionId) {
        if self.by_txn_last_xid == xid {
            self.by_txn_last_xid = InvalidTransactionId;
            self.by_txn_last_txn = None;
        }
    }

    /// Take and remove a txn's toast hash (`txn->toast_hash` -> NULL).
    pub(crate) fn toast_hash_take(
        &mut self,
        xid: TransactionId,
    ) -> Option<HashMap<types_core::Oid, toast::ReorderBufferToastEnt>> {
        self.by_txn.get_mut(&xid).and_then(|t| t.toast_hash.take())
    }

    /// `SnapBuildSnapDecRefcount(txn->base_snapshot)` — the base snapshot's
    /// refcount lives in the snapshot builder; this owner only holds its own
    /// copy and drops it here. The builder-side decrement is performed
    /// caller-side, mirroring `ReorderBufferSetBaseSnapshot`'s discipline.
    pub(crate) fn base_snapshot_dec_refcount(&mut self, xid: TransactionId) {
        if let Some(t) = self.by_txn.get_mut(&xid) {
            let _ = t.base_snapshot.take();
        }
    }

    // ----- memory accounting (spill family owns the global heap; counters kept)

    /// `ReorderBufferChangeMemoryUpdate(rb, NULL, txn, false, mem_freed)` — the
    /// batched subtraction path used by cleanup/truncate.
    pub(crate) fn change_memory_update_sub_txn(&mut self, txn_xid: TransactionId, sz: usize) {
        self.with_txn(txn_xid, |t| {
            t.size = t.size.saturating_sub(sz);
            t.total_size = t.total_size.saturating_sub(sz);
        });
        self.size = self.size.saturating_sub(sz);
    }

    /// `ReorderBufferChangeMemoryUpdate(rb, change, NULL, false, sz)` — the
    /// single-change subtraction path used by `ReorderBufferFreeChange`.
    pub(crate) fn change_memory_update_sub(&mut self, sz: usize) {
        self.size = self.size.saturating_sub(sz);
    }

    /// `rb->totalBytes += n`.
    pub(crate) fn totalbytes_add(&mut self, n: i64) {
        self.total_bytes += n;
    }

    /// `rb->totalTxns += n`.
    pub(crate) fn total_txns_add(&mut self, n: i64) {
        self.total_txns += n;
    }

    // ----- spill statistics (UpdateDecodingStats reads these via the
    //       reorderbuffer_stats seam; the spill codec increments them) --------

    /// `rb->spillCount += n`.
    pub(crate) fn spill_count_add(&mut self, n: i64) {
        self.spill_count += n;
    }
    /// `rb->spillBytes += n`.
    pub(crate) fn spill_bytes_add(&mut self, n: i64) {
        self.spill_bytes += n;
    }
    /// `rb->spillTxns += n`.
    pub(crate) fn spill_txns_add(&mut self, n: i64) {
        self.spill_txns += n;
    }

    // ----- streaming family entry points -----------------------------------

    /// `ReorderBufferStreamCommit(rb, txn)` — commit a (partially) streamed txn.
    /// Streaming family; not yet ported.
    pub(crate) fn stream_commit(&mut self, _xid: TransactionId) {
        panic!("ReorderBufferStreamCommit: streaming family not yet ported");
    }

    // ----- output-plugin callbacks (logical.c dispatch seam) ---------------
    //
    // These re-enter logical decoding through
    // `logical_seams::dispatch_reorderbuffer_callback`.
    // That inward seam needs the reorderbuffer owner to hold the live
    // `LogicalDecodingContext` (`rb->private_data`) and to produce the
    // `RelationHandle`/`ChangeHandle` the wrappers read; neither is modeled yet,
    // so each callback panics loudly until that keystone lands.

    /// `rb->begin(rb, txn)` / `rb->begin_prepare(rb, txn)` (non-streaming).
    /// The C `ReorderBufferProcessTXN` only sends begin/begin-prepare for
    /// non-streamed transactions; the caller has already checked `!streaming`.
    pub(crate) fn begin_output(&mut self, xid: TransactionId, streaming: bool) {
        debug_assert!(!streaming);
        let (txn_first_lsn, is_prepared) =
            self.with_txn_pub(xid, |t| (t.first_lsn, t.is_prepared()));
        let txn = crate::registry::txn_handle_for_xid(xid);
        let cb = if is_prepared {
            types_logical::ReorderBufferCallback::BeginPrepare {
                txn,
                txn_first_lsn,
                txn_xid: xid,
            }
        } else {
            types_logical::ReorderBufferCallback::Begin {
                txn,
                txn_first_lsn,
                txn_xid: xid,
            }
        };
        logical_seams::dispatch_reorderbuffer_callback::call(cb)
            .expect("begin/begin_prepare output-plugin callback");
    }

    /// `rb->stream_start(rb, txn, first_lsn)`.
    pub(crate) fn stream_start_output(&mut self, _xid: TransactionId, _first_lsn: XLogRecPtr) {
        panic!("ReorderBuffer stream_start callback: logical.c dispatch not yet modeled");
    }

    /// `rb->stream_abort(rb, txn, abort_lsn)` — notify the remote node of a
    /// streamed transaction's abort (reached from `ReorderBufferAbort` /
    /// `ReorderBufferAbortOld` only for already-streamed txns).
    pub(crate) fn stream_abort_output(&mut self, _xid: TransactionId, _abort_lsn: XLogRecPtr) {
        panic!("ReorderBuffer stream_abort callback: logical.c dispatch not yet modeled");
    }

    /// `rb->stream_stop(rb, txn, last_lsn)`.
    pub(crate) fn stream_stop_output(&mut self, _xid: TransactionId, _last_lsn: XLogRecPtr) {
        panic!("ReorderBuffer stream_stop callback: logical.c dispatch not yet modeled");
    }

    /// `ReorderBufferApplyChange(rb, txn, relation, change, streaming)`
    /// (reorderbuffer.c:2070) — dispatch one decoded heap change to the output
    /// plugin's `stream_change`/`apply_change` callback. The relation has been
    /// resolved+pinned and the change published (its `ChangeHandle`) by the
    /// caller [`process_tp_change`]; this mirrors the C 2-line helper.
    pub(crate) fn apply_decoded_change(
        &mut self,
        xid: TransactionId,
        relation: types_logical::RelationHandle,
        change: types_logical::ChangeHandle,
        change_lsn: XLogRecPtr,
        streaming: bool,
    ) {
        let txn = crate::registry::txn_handle_for_xid(xid);
        let cb = if streaming {
            types_logical::ReorderBufferCallback::StreamChange {
                txn,
                txn_xid: xid,
                relation,
                change,
                change_lsn,
            }
        } else {
            types_logical::ReorderBufferCallback::ApplyChange {
                txn,
                txn_xid: xid,
                relation,
                change,
                change_lsn,
            }
        };
        logical_seams::dispatch_reorderbuffer_callback::call(cb)
            .expect("apply_change/stream_change output-plugin callback");
    }

    /// `ReorderBufferApplyTruncate(rb, txn, nrelations, relations, change,
    /// streaming)` (reorderbuffer.c:2085) — dispatch a decoded TRUNCATE to the
    /// output plugin's `stream_truncate`/`apply_truncate` callback.
    pub(crate) fn apply_truncate(
        &mut self,
        xid: TransactionId,
        nrelations: i32,
        relations: types_logical::RelationsHandle,
        change: types_logical::ChangeHandle,
        change_lsn: XLogRecPtr,
        streaming: bool,
    ) {
        let txn = crate::registry::txn_handle_for_xid(xid);
        let cb = if streaming {
            types_logical::ReorderBufferCallback::StreamTruncate {
                txn,
                txn_xid: xid,
                nrelations,
                relations,
                change,
                change_lsn,
            }
        } else {
            types_logical::ReorderBufferCallback::ApplyTruncate {
                txn,
                txn_xid: xid,
                nrelations,
                relations,
                change,
                change_lsn,
            }
        };
        logical_seams::dispatch_reorderbuffer_callback::call(cb)
            .expect("apply_truncate/stream_truncate output-plugin callback");
    }

    /// `ReorderBufferApplyMessage(rb, txn, change, streaming)`
    /// (reorderbuffer.c:2099) — dispatch a decoded logical message to the output
    /// plugin's `stream_message`/`message` callback. For a transactional message
    /// the callback carries the owning txn (`txn != NULL` in C); the
    /// `transactional` flag is always `true` here because the in-band MESSAGE
    /// change only exists for a transactional message (non-transactional ones go
    /// straight to `ReorderBufferQueueMessage`'s immediate dispatch).
    pub(crate) fn apply_message(
        &mut self,
        xid: TransactionId,
        change: &ReorderBufferChange,
        streaming: bool,
    ) {
        let (prefix, message) = match &change.data {
            ReorderBufferChangeData::Msg { prefix, message } => (prefix.clone(), message.clone()),
            _ => unreachable!("MESSAGE change carries a msg payload"),
        };
        let message_size = message.len();
        let txn = crate::registry::txn_handle_for_xid(xid);
        let change_lsn = change.lsn;
        crate::registry::with_message_published(prefix, message, |prefix_h, message_h| {
            let cb = if streaming {
                types_logical::ReorderBufferCallback::StreamMessage {
                    txn: Some((txn, xid)),
                    message_lsn: change_lsn,
                    transactional: true,
                    prefix: prefix_h,
                    message_size,
                    message: message_h,
                }
            } else {
                types_logical::ReorderBufferCallback::Message {
                    txn: Some((txn, xid)),
                    message_lsn: change_lsn,
                    transactional: true,
                    prefix: prefix_h,
                    message_size,
                    message: message_h,
                }
            };
            logical_seams::dispatch_reorderbuffer_callback::call(cb)
                .expect("message/stream_message output-plugin callback");
        });
    }

    /// `rb->update_progress_txn(rb, txn, lsn)` keepalive.
    pub(crate) fn update_progress_txn(&mut self, xid: TransactionId, lsn: XLogRecPtr) {
        let cb = types_logical::ReorderBufferCallback::UpdateProgressTxn { txn_xid: xid, lsn };
        logical_seams::dispatch_reorderbuffer_callback::call(cb)
            .expect("update_progress_txn output-plugin callback");
    }

    /// `SetupHistoricSnapshot(snapshot_now, txn->tuplecid_hash)` — install the
    /// historic snapshot and the txn's `(relfilelocator, ctid) -> (cmin, cmax)`
    /// tuplecid map in snapmgr (the owner of the active `tuplecid_data`), via
    /// the `setup_historic_snapshot` seam.
    pub(crate) fn setup_historic_snapshot(&mut self, snapshot: &SnapshotData, xid: TransactionId) {
        let tuplecids = self
            .by_txn_get(xid)
            .and_then(|t| t.tuplecid_hash.clone());
        snapmgr_seams::setup_historic_snapshot::call(snapshot.clone(), tuplecids);
    }

}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

/// Convert a [`DecodedTuple`](reorderbuffer_seams::DecodedTuple)
/// image conveyed over the queue seam into the owner's
/// [`ReorderBufferTupleBuf`]. decode.c `DecodeXLogTuple`s the WAL tuple bytes
/// into the reorder buffer's own context; both sides carry the same fields.
fn decoded_tuple_to_buf(
    t: reorderbuffer_seams::DecodedTuple,
) -> ReorderBufferTupleBuf {
    ReorderBufferTupleBuf {
        t_len: t.t_len,
        t_self: t.t_self,
        t_table_oid: t.t_table_oid,
        data: t.data,
    }
}

/// The inverse of [`decoded_tuple_to_buf`]: project the owner's
/// [`ReorderBufferTupleBuf`] back into the seam-boundary `DecodedTuple` image
/// the output-plugin facade hands to a change callback (`ChangeHandle`
/// resolution). Same fields on both sides.
fn buf_to_decoded_tuple(
    buf: &ReorderBufferTupleBuf,
) -> reorderbuffer_seams::DecodedTuple {
    reorderbuffer_seams::DecodedTuple {
        t_len: buf.t_len,
        t_self: buf.t_self,
        t_table_oid: buf.t_table_oid,
        data: buf.data.clone(),
    }
}

/// `TransactionIdPrecedes(id1, id2)` (transam.c) — `id1` is logically earlier
/// than `id2`, accounting for xid wraparound. Special-cased for the non-normal
/// (bootstrap/frozen) xids exactly as the C does. Inlined here to avoid a
/// transam dependency for the single `ReorderBufferAbortOld` call.
fn transaction_id_precedes(id1: TransactionId, id2: TransactionId) -> bool {
    // If either ID is a permanent XID then we can just do unsigned comparison.
    const FIRST_NORMAL_TRANSACTION_ID: TransactionId = 3;
    let normal1 = id1 >= FIRST_NORMAL_TRANSACTION_ID;
    let normal2 = id2 >= FIRST_NORMAL_TRANSACTION_ID;
    if !normal1 || !normal2 {
        return id1 < id2;
    }
    // Wraparound-aware comparison: cast the difference to signed.
    let diff = id1.wrapping_sub(id2) as i32;
    diff < 0
}

/// `ReorderBufferChangeSize(change)` — bytes attributed to a change for memory
/// accounting (`sizeof(ReorderBufferChange)` plus the per-action payload).
/// Mirrors the C switch arm for arm.
pub(crate) fn change_size(change: &ReorderBufferChange) -> usize {
    let base = core::mem::size_of::<ReorderBufferChange>();
    // C uses HeapTupleData header size for each decoded tuple; we carry the
    // decoded image in ReorderBufferTupleBuf (t_len + the contiguous bytes).
    let heap_tuple_data = core::mem::size_of::<ItemPointerData>()
        + core::mem::size_of::<u32>()
        + core::mem::size_of::<types_core::Oid>();
    match &change.data {
        ReorderBufferChangeData::Tp {
            oldtuple, newtuple, ..
        } => {
            let mut sz = base;
            if let Some(t) = oldtuple {
                sz += heap_tuple_data + t.t_len as usize;
            }
            if let Some(t) = newtuple {
                sz += heap_tuple_data + t.t_len as usize;
            }
            sz
        }
        ReorderBufferChangeData::Inval(msgs) => {
            base + msgs.len() * core::mem::size_of::<SharedInvalidationMessage>()
        }
        ReorderBufferChangeData::Msg { prefix, message } => {
            // C: prefix_size (= strlen+1) + message_size + sizeof(Size)*2.
            base + (prefix.len() + 1) + message.len() + 2 * core::mem::size_of::<usize>()
        }
        ReorderBufferChangeData::Snapshot(snap) => {
            base + core::mem::size_of::<SnapshotData>()
                + core::mem::size_of::<TransactionId>() * snap.xcnt as usize
                + core::mem::size_of::<TransactionId>() * snap.subxcnt.max(0) as usize
        }
        ReorderBufferChangeData::Truncate { relids, .. } => {
            base + core::mem::size_of::<types_core::Oid>() * relids.len()
        }
        ReorderBufferChangeData::CommandId(_)
        | ReorderBufferChangeData::TupleCid { .. }
        | ReorderBufferChangeData::None => base,
    }
}

/// `ReorderBufferAccumulateInvalidations(invals_out, ninvals_out, msgs_new,
/// nmsgs_new)` — append `msgs_new` onto the growable `invals_out` array.
fn accumulate_invalidations(out: &mut Vec<SharedInvalidationMessage>, new: &[SharedInvalidationMessage]) {
    out.extend_from_slice(new);
}

#[cfg(test)]
mod tests;
