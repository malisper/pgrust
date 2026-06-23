//! Shared C-ABI definitions for the heap access method (`access/heap`).
//!
//! These are the on-disk / shared / `Datum`-facing types and constants used by
//! more than one heap crate: tuple-visibility result codes, visibility-map
//! flag bits, freeze plans, prune/freeze results, and `heap_insert` option
//! flags.  Internal-only working structs stay idiomatic in their owning crate;
//! anything that crosses a C ABI boundary or is shared between heap crates
//! lives here with `#[repr(C)]` and compile-time layout assertions.

use crate::{
    uint16, uint32, uint8, CommandId, HeapTupleHeaderData, ItemPointerData, MaxHeapTuplesPerPage,
    MultiXactId, OffsetNumber, Oid, RelFileLocator, RmgrId, SharedInvalidationMessage,
    TransactionId,
};

/* ----------------------------------------------------------------
 * storage/itemptr.h
 * ---------------------------------------------------------------- */

/// `typedef ItemPointerData *ItemPointer`.
pub type ItemPointer = *mut ItemPointerData;

/* ----------------------------------------------------------------
 * access/sdir.h: scan direction
 * ---------------------------------------------------------------- */

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum ScanDirection {
    BackwardScanDirection = -1,
    NoMovementScanDirection = 0,
    ForwardScanDirection = 1,
}
pub use ScanDirection::*;

/* ----------------------------------------------------------------
 * nodes/lockoptions.h: row-level lock modes and wait policy
 * ---------------------------------------------------------------- */

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum LockTupleMode {
    /// SELECT FOR KEY SHARE
    LockTupleKeyShare = 0,
    /// SELECT FOR SHARE
    LockTupleShare,
    /// SELECT FOR NO KEY UPDATE, and UPDATEs that don't modify key columns
    LockTupleNoKeyExclusive,
    /// SELECT FOR UPDATE, UPDATEs that modify key columns, and DELETE
    LockTupleExclusive,
}
pub use LockTupleMode::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum LockWaitPolicy {
    /// Wait for the lock to become available (default behavior)
    LockWaitBlock = 0,
    /// Skip rows that can't be locked (SKIP LOCKED)
    LockWaitSkip,
    /// Raise an error if a row cannot be locked (NOWAIT)
    LockWaitError,
}
pub use LockWaitPolicy::*;

/* ----------------------------------------------------------------
 * access/tableam.h: TU_UpdateIndexes and TM_FailureData
 * ---------------------------------------------------------------- */

/// Result codes for `table_update(..., update_indexes*..)`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum TU_UpdateIndexes {
    /// No indexed columns were updated (incl. TID addressing of tuple)
    TU_None = 0,
    /// A non-summarizing indexed column was updated, or the TID has changed
    TU_All,
    /// Only summarized columns were updated, TID is unchanged
    TU_Summarizing,
}
pub use TU_UpdateIndexes::*;

/// Output of `table_tuple_{update,delete,lock}` when the result is not `TM_Ok`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct TM_FailureData {
    pub ctid: ItemPointerData,
    pub xmax: TransactionId,
    pub cmax: CommandId,
    pub traversed: bool,
}

/* ----------------------------------------------------------------
 * htup_details.h: heap-page geometry
 *
 * `MaxHeapTupleSize` (toast.rs) and `MaxHeapTuplesPerPage` (storage.rs) already
 * live in the FFI crate and are reused here.
 * ---------------------------------------------------------------- */

/// `SizeofHeapTupleHeader` == `offsetof(HeapTupleHeaderData, t_bits)`.
pub const SizeofHeapTupleHeader: usize = core::mem::offset_of!(HeapTupleHeaderData, t_bits);

/// `TOAST_INDEX_TARGET` (`access/heaptoast.h`) == `MaxHeapTupleSize / 16`.
pub const TOAST_INDEX_TARGET: usize = crate::MaxHeapTupleSize / 16;

/* ----------------------------------------------------------------
 * visibilitymapdefs.h
 * ---------------------------------------------------------------- */

/// Number of bits used per heap block in the visibility map.
pub const BITS_PER_HEAPBLOCK: i32 = 2;

pub const VISIBILITYMAP_ALL_VISIBLE: uint8 = 0x01;
pub const VISIBILITYMAP_ALL_FROZEN: uint8 = 0x02;
/// OR of all valid visibility-map flag bits.
pub const VISIBILITYMAP_VALID_BITS: uint8 = 0x03;
/// Catalog-relation marker carried only inside `xl_heap_visible.flags`.
pub const VISIBILITYMAP_XLOG_CATALOG_REL: uint8 = 0x04;
pub const VISIBILITYMAP_XLOG_VALID_BITS: uint8 =
    VISIBILITYMAP_VALID_BITS | VISIBILITYMAP_XLOG_CATALOG_REL;

/* ----------------------------------------------------------------
 * heapam.h: HTSV_Result (HeapTupleSatisfiesVacuum result codes)
 * ---------------------------------------------------------------- */

/// Result codes for `HeapTupleSatisfiesVacuum`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum HTSV_Result {
    /// tuple is dead and deletable
    HEAPTUPLE_DEAD = 0,
    /// tuple is live (committed, no deleter)
    HEAPTUPLE_LIVE,
    /// tuple is dead, but not deletable yet
    HEAPTUPLE_RECENTLY_DEAD,
    /// inserting xact is still in progress
    HEAPTUPLE_INSERT_IN_PROGRESS,
    /// deleting xact is still in progress
    HEAPTUPLE_DELETE_IN_PROGRESS,
}
pub use HTSV_Result::*;

/* ----------------------------------------------------------------
 * tableam.h: TM_Result (table_update/delete/lock_tuple result codes)
 * ---------------------------------------------------------------- */

/// Result codes for `table_tuple_{update,delete,lock}` and the heap routines
/// that implement them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum TM_Result {
    /// update/delete performed, or lock acquired
    TM_Ok = 0,
    /// affected tuple wasn't visible to the relevant snapshot
    TM_Invisible,
    /// affected tuple was already modified by the calling backend
    TM_SelfModified,
    /// affected tuple was updated by another transaction
    TM_Updated,
    /// affected tuple was deleted by another transaction
    TM_Deleted,
    /// affected tuple is currently being modified by another session
    TM_BeingModified,
    /// lock couldn't be acquired, action skipped (only used by lock_tuple)
    TM_WouldBlock,
}
pub use TM_Result::*;

/* ----------------------------------------------------------------
 * heapam.h: heap_insert option flags (mirror of TABLE_INSERT_*)
 * ---------------------------------------------------------------- */

pub const HEAP_INSERT_SKIP_FSM: i32 = 0x0002;
pub const HEAP_INSERT_FROZEN: i32 = 0x0004;
pub const HEAP_INSERT_NO_LOGICAL: i32 = 0x0008;
pub const HEAP_INSERT_SPECULATIVE: i32 = 0x0010;

/* ----------------------------------------------------------------
 * hio.h: bulk-insert state (shared between heapam.c and hio.c)
 * ---------------------------------------------------------------- */

/// `typedef struct BulkInsertStateData *BulkInsertState`.
pub type BulkInsertState = *mut BulkInsertStateData;

/// State maintained across a bulk insert into a heap relation.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct BulkInsertStateData {
    /// our BULKWRITE strategy object
    pub strategy: crate::BufferAccessStrategy,
    /// current insertion target page
    pub current_buf: crate::Buffer,
    /// next unused page from the last bulk extension
    pub next_free: crate::BlockNumber,
    /// last unused page from the last bulk extension
    pub last_free: crate::BlockNumber,
    /// number of pages this bulk insert already extended by
    pub already_extended_by: crate::uint32,
}

/* ----------------------------------------------------------------
 * heapam.h: freeze plan / page-freeze state
 * ---------------------------------------------------------------- */

/// `heap_prepare_freeze_tuple` may request that the executor recheck a tuple's
/// to-be-frozen xmin and/or xmax status using pg_xact.
pub const HEAP_FREEZE_CHECK_XMIN_COMMITTED: uint8 = 0x01;
pub const HEAP_FREEZE_CHECK_XMAX_ABORTED: uint8 = 0x02;

/// `heap_prepare_freeze_tuple` state describing how to freeze a tuple.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct HeapTupleFreeze {
    /// New xmax value, when relevant.
    pub xmax: TransactionId,
    pub t_infomask2: uint16,
    pub t_infomask: uint16,
    pub frzflags: uint8,
    /// xmin/xmax recheck flags (`HEAP_FREEZE_CHECK_*`).
    pub checkflags: uint8,
    /// Page offset number for the tuple.
    pub offset: OffsetNumber,
}

/// State used by VACUUM to track the details of freezing all eligible tuples
/// on a given heap page.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct HeapPageFreeze {
    /// Is `heap_prepare_freeze_tuple` caller required to freeze the page?
    pub freeze_required: bool,

    /// "Freeze" NewRelfrozenXid/NewRelminMxid trackers.
    pub FreezePageRelfrozenXid: TransactionId,
    pub FreezePageRelminMxid: MultiXactId,

    /// "No freeze" NewRelfrozenXid/NewRelminMxid trackers.
    pub NoFreezePageRelfrozenXid: TransactionId,
    pub NoFreezePageRelminMxid: MultiXactId,
}

/// `struct VacuumCutoffs` (commands/vacuum.h) — the freeze/removal cutoffs a
/// VACUUM computes once per relation and threads into the freeze machinery.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct VacuumCutoffs {
    /// Existing `pg_class.relfrozenxid` at VACUUM start.
    pub relfrozenxid: TransactionId,
    /// Existing `pg_class.relminmxid` at VACUUM start.
    pub relminmxid: MultiXactId,
    /// XID below which committed-deleted tuples are DEAD (not RECENTLY_DEAD).
    pub OldestXmin: TransactionId,
    /// MXID below which multis are invisible to all running transactions.
    pub OldestMxact: MultiXactId,
    /// XID below which all XIDs are definitely frozen/removed.
    pub FreezeLimit: TransactionId,
    /// MXID below which all multis are definitely removed from xmax.
    pub MultiXactCutoff: MultiXactId,
}

/* ----------------------------------------------------------------
 * heapam.h: heap_page_prune_and_freeze() result
 * ---------------------------------------------------------------- */

/// `reason` codes for `heap_page_prune_and_freeze()`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum PruneReason {
    /// on-access pruning
    PRUNE_ON_ACCESS = 0,
    /// VACUUM 1st heap pass
    PRUNE_VACUUM_SCAN,
    /// VACUUM 2nd heap pass
    PRUNE_VACUUM_CLEANUP,
}
pub use PruneReason::*;

/// Per-page state returned by `heap_page_prune_and_freeze()`.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct PruneFreezeResult {
    /// Number of tuples deleted from the page.
    pub ndeleted: i32,
    /// Number of newly LP_DEAD items.
    pub nnewlpdead: i32,
    /// Number of tuples we froze.
    pub nfrozen: i32,

    /// Number of live tuples on the page, after pruning.
    pub live_tuples: i32,
    /// Number of recently-dead tuples on the page, after pruning.
    pub recently_dead_tuples: i32,

    /// Whether the all-visible bit can be set for this page after pruning.
    pub all_visible: bool,
    /// Whether the all-frozen bit can be set for this page after pruning.
    pub all_frozen: bool,
    /// Newest xmin of live tuples on the page (valid only when frozen).
    pub vm_conflict_horizon: TransactionId,

    /// Whether the page makes rel truncation unsafe.
    pub hastup: bool,

    /// LP_DEAD items on the page after pruning (includes pre-existing ones).
    pub lpdead_items: i32,
    pub deadoffsets: [OffsetNumber; MaxHeapTuplesPerPage as usize],
}

/* ----------------------------------------------------------------
 * access/rmgrlist.h: resource manager ids used by heap WAL records
 * ---------------------------------------------------------------- */

/// `RM_HEAP2_ID` — second heap resource manager (prune/freeze, visible, etc.).
pub const RM_HEAP2_ID: RmgrId = 9;
/// `RM_HEAP_ID` — primary heap resource manager (insert/update/delete/lock).
pub const RM_HEAP_ID: RmgrId = 10;

/* ----------------------------------------------------------------
 * access/heapam_xlog.h: heap2 prune/freeze WAL record opcodes
 *
 * There's no logical difference between these three; the separate opcodes only
 * record *why* the WAL record was emitted, for debugging and analysis.
 * ---------------------------------------------------------------- */

pub const XLOG_HEAP2_PRUNE_ON_ACCESS: uint8 = 0x10;
pub const XLOG_HEAP2_PRUNE_VACUUM_SCAN: uint8 = 0x20;
pub const XLOG_HEAP2_PRUNE_VACUUM_CLEANUP: uint8 = 0x30;

/* ----------------------------------------------------------------
 * access/heapam_xlog.h: XLOG_HEAP2_PRUNE_FREEZE record layout
 *
 * `xl_heap_prune` is the main record; the `XLHP_HAS_*` flags indicate which
 * "sub-records" are present in block reference 0's data, and the other
 * `XLHP_*` flags provide additional information about the conditions for
 * replay.  These are on-the-wire ABI structs and so are `#[repr(C)]`.
 * ---------------------------------------------------------------- */

/* to handle recovery conflict during logical decoding on standby */
pub const XLHP_IS_CATALOG_REL: uint8 = 1 << 1;
/* does replaying the record require a cleanup-lock? */
pub const XLHP_CLEANUP_LOCK: uint8 = 1 << 2;
/* a snapshot conflict horizon XID follows the main record, unaligned */
pub const XLHP_HAS_CONFLICT_HORIZON: uint8 = 1 << 3;
/* an xlhp_freeze_plans sub-record and one or more xlhp_freeze_plan are present */
pub const XLHP_HAS_FREEZE_PLANS: uint8 = 1 << 4;
/* xlhp_prune_items sub-records with redirected/dead/unused offsets are present */
pub const XLHP_HAS_REDIRECTIONS: uint8 = 1 << 5;
pub const XLHP_HAS_DEAD_ITEMS: uint8 = 1 << 6;
pub const XLHP_HAS_NOW_UNUSED_ITEMS: uint8 = 1 << 7;

/* flags for xlhp_freeze_plan.frzflags (0x01 was XLH_FREEZE_XMIN) */
pub const XLH_FREEZE_XVAC: uint8 = 0x02;
pub const XLH_INVALID_XVAC: uint8 = 0x04;

/// `xl_heap_prune` — the main XLOG_HEAP2_PRUNE_FREEZE record.
///
/// If `XLHP_HAS_CONFLICT_HORIZON` is set, the conflict horizon XID follows
/// (unaligned).  `SizeOfHeapPrune` excludes any trailing padding.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct xl_heap_prune {
    pub reason: uint8,
    pub flags: uint8,
}

/// `SizeOfHeapPrune` == `offsetof(xl_heap_prune, flags) + sizeof(uint8)`.
pub const SizeOfHeapPrune: usize = core::mem::offset_of!(xl_heap_prune, flags) + 1;

/// `xlhp_freeze_plan` — how to freeze a group of one or more heap tuples
/// (appears inside `xl_heap_prune`'s `xlhp_freeze_plans` sub-record).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct xlhp_freeze_plan {
    pub xmax: TransactionId,
    pub t_infomask2: uint16,
    pub t_infomask: uint16,
    pub frzflags: uint8,
    /// Length of the individual page-offset-numbers array for this plan.
    pub ntuples: uint16,
}

/// `xlhp_freeze_plans` — header for the array of `xlhp_freeze_plan` structs.
///
/// In C this has a `plans[FLEXIBLE_ARRAY_MEMBER]` tail; we store only the
/// fixed header (`offsetof(xlhp_freeze_plans, plans)` worth of bytes is what
/// gets registered) and serialize the plans separately.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct xlhp_freeze_plans {
    pub nplans: uint16,
    pub plans: [xlhp_freeze_plan; 0],
}

/// `xlhp_prune_items` — generic sub-record for redirect/dead/unused items.
///
/// For the `XLHP_HAS_REDIRECTIONS` variant the data array holds `2 * ntargets`
/// `OffsetNumber`s.  In C this has a `data[FLEXIBLE_ARRAY_MEMBER]` tail; we
/// store only the fixed header and serialize the offsets separately.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct xlhp_prune_items {
    pub ntargets: uint16,
    pub data: [OffsetNumber; 0],
}

/* ----------------------------------------------------------------
 * access/heapam_xlog.h: RM_HEAP2 opcode for the logical-decoding new-cid record
 * ---------------------------------------------------------------- */

/// `XLOG_HEAP2_NEW_CID` — records the cmin/cmax/combocid of a catalog tuple for
/// logical decoding (only emitted at `wal_level >= logical`).
pub const XLOG_HEAP2_NEW_CID: uint8 = 0x70;

/* ----------------------------------------------------------------
 * access/heapam_xlog.h: xl_heap_update flag values (8 bits available)
 * ---------------------------------------------------------------- */

/// PD_ALL_VISIBLE was cleared on the old tuple's page.
pub const XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED: uint8 = 1 << 0;
/// PD_ALL_VISIBLE was cleared on the new tuple's page (when different).
pub const XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED: uint8 = 1 << 1;
/// The full old tuple (REPLICA IDENTITY FULL) follows in the main data.
pub const XLH_UPDATE_CONTAINS_OLD_TUPLE: uint8 = 1 << 2;
/// The old replica-identity key follows in the main data.
pub const XLH_UPDATE_CONTAINS_OLD_KEY: uint8 = 1 << 3;
/// The new tuple is included even if a full-page image was taken.
pub const XLH_UPDATE_CONTAINS_NEW_TUPLE: uint8 = 1 << 4;
/// A common prefix copied from the old tuple precedes the new tuple data.
pub const XLH_UPDATE_PREFIX_FROM_OLD: uint8 = 1 << 5;
/// A common suffix copied from the old tuple precedes the new tuple data.
pub const XLH_UPDATE_SUFFIX_FROM_OLD: uint8 = 1 << 6;

/// `XLH_UPDATE_CONTAINS_OLD` — convenience mask: any form of old tuple logged.
pub const XLH_UPDATE_CONTAINS_OLD: uint8 =
    XLH_UPDATE_CONTAINS_OLD_TUPLE | XLH_UPDATE_CONTAINS_OLD_KEY;

/* ----------------------------------------------------------------
 * access/heapam_xlog.h: tuple-header / update / new-cid WAL records
 *
 * On-the-wire ABI structs, so `#[repr(C)]` with compile-time layout checks.
 * ---------------------------------------------------------------- */

/// `xl_heap_header` — the part of `HeapTupleHeaderData` we store in WAL for an
/// inserted or updated tuple.  The rest of the fixed header is reconstructed on
/// replay from elsewhere in the record.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct xl_heap_header {
    pub t_infomask2: uint16,
    pub t_infomask: uint16,
    pub t_hoff: uint8,
}

/// `SizeOfHeapHeader` == `offsetof(xl_heap_header, t_hoff) + sizeof(uint8)`.
pub const SizeOfHeapHeader: usize = core::mem::offset_of!(xl_heap_header, t_hoff) + 1;

/// `xl_heap_update` — the main `XLOG_HEAP_UPDATE` / `XLOG_HEAP_HOT_UPDATE`
/// record.  `SizeOfHeapUpdate` excludes any trailing padding (the C macro stops
/// at `new_offnum`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct xl_heap_update {
    /// xmax of the old tuple.
    pub old_xmax: TransactionId,
    /// old tuple's offset.
    pub old_offnum: OffsetNumber,
    /// infomask bits to set on the old tuple.
    pub old_infobits_set: uint8,
    pub flags: uint8,
    /// xmax of the new tuple.
    pub new_xmax: TransactionId,
    /// new tuple's offset.
    pub new_offnum: OffsetNumber,
}

/// `SizeOfHeapUpdate` == `offsetof(xl_heap_update, new_offnum) + sizeof(OffsetNumber)`.
pub const SizeOfHeapUpdate: usize =
    core::mem::offset_of!(xl_heap_update, new_offnum) + core::mem::size_of::<OffsetNumber>();

/// `xl_heap_new_cid` — records the cmin/cmax/combocid of a catalog tuple change
/// for logical decoding, plus the relfilelocator/ctid needed for lookup.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct xl_heap_new_cid {
    /// Toplevel xid so cids needn't be merged across transactions.
    pub top_xid: TransactionId,
    pub cmin: CommandId,
    pub cmax: CommandId,
    /// Just for debugging.
    pub combocid: CommandId,
    pub target_locator: RelFileLocator,
    pub target_tid: ItemPointerData,
}

/// `SizeOfHeapNewCid` == `offsetof(xl_heap_new_cid, target_tid) + sizeof(ItemPointerData)`.
pub const SizeOfHeapNewCid: usize =
    core::mem::offset_of!(xl_heap_new_cid, target_tid) + core::mem::size_of::<ItemPointerData>();

/* ----------------------------------------------------------------
 * access/heapam_xlog.h: RM_HEAP (heap1) WAL record opcodes
 *
 * WAL record definitions for heapam.c's WAL operations.  The opcodes live in
 * the high four bits of the record info byte; the low four (XLOG_HEAP_INIT_PAGE)
 * is an OR-able modifier.
 * ---------------------------------------------------------------- */

pub const XLOG_HEAP_INSERT: uint8 = 0x00;
pub const XLOG_HEAP_DELETE: uint8 = 0x10;
pub const XLOG_HEAP_UPDATE: uint8 = 0x20;
pub const XLOG_HEAP_TRUNCATE: uint8 = 0x30;
pub const XLOG_HEAP_HOT_UPDATE: uint8 = 0x40;
pub const XLOG_HEAP_CONFIRM: uint8 = 0x50;
pub const XLOG_HEAP_LOCK: uint8 = 0x60;
pub const XLOG_HEAP_INPLACE: uint8 = 0x70;

/// `XLOG_HEAP_OPMASK` — masks the opcode bits out of the record info byte.
pub const XLOG_HEAP_OPMASK: uint8 = 0x70;

/// `XLOG_HEAP_INIT_PAGE` — when set, the page is reinitialized from scratch (the
/// low-order bit of the info field is OR'd onto the opcode for both heap rmgrs).
pub const XLOG_HEAP_INIT_PAGE: uint8 = 0x80;

/* ----------------------------------------------------------------
 * access/heapam_xlog.h: RM_HEAP2 (heap2) WAL record opcodes
 * ---------------------------------------------------------------- */

pub const XLOG_HEAP2_REWRITE: uint8 = 0x00;
pub const XLOG_HEAP2_VISIBLE: uint8 = 0x40;
pub const XLOG_HEAP2_MULTI_INSERT: uint8 = 0x50;
pub const XLOG_HEAP2_LOCK_UPDATED: uint8 = 0x60;

/* ----------------------------------------------------------------
 * access/heapam_xlog.h: xl_heap_insert flag values (8 bits available)
 * ---------------------------------------------------------------- */

/// PD_ALL_VISIBLE was cleared.
pub const XLH_INSERT_ALL_VISIBLE_CLEARED: uint8 = 1 << 0;
pub const XLH_INSERT_LAST_IN_MULTI: uint8 = 1 << 1;
pub const XLH_INSERT_IS_SPECULATIVE: uint8 = 1 << 2;
pub const XLH_INSERT_CONTAINS_NEW_TUPLE: uint8 = 1 << 3;
pub const XLH_INSERT_ON_TOAST_RELATION: uint8 = 1 << 4;
/// All tuples on the page have been marked frozen and PD_ALL_VISIBLE all-frozen.
pub const XLH_INSERT_ALL_FROZEN_SET: uint8 = 1 << 5;

/* ----------------------------------------------------------------
 * access/heapam_xlog.h: xl_heap_delete flag values (8 bits available)
 * ---------------------------------------------------------------- */

pub const XLH_DELETE_ALL_VISIBLE_CLEARED: uint8 = 1 << 0;
pub const XLH_DELETE_CONTAINS_OLD_TUPLE: uint8 = 1 << 1;
pub const XLH_DELETE_CONTAINS_OLD_KEY: uint8 = 1 << 2;
pub const XLH_DELETE_IS_SUPER: uint8 = 1 << 3;
pub const XLH_DELETE_IS_PARTITION_MOVE: uint8 = 1 << 4;

/// `XLH_DELETE_CONTAINS_OLD` — convenience mask: any form of old tuple logged.
pub const XLH_DELETE_CONTAINS_OLD: uint8 =
    XLH_DELETE_CONTAINS_OLD_TUPLE | XLH_DELETE_CONTAINS_OLD_KEY;

/* ----------------------------------------------------------------
 * access/heapam_xlog.h: xl_heap_lock / xl_heap_lock_updated infobits + flags
 * ---------------------------------------------------------------- */

/* flags for infobits_set */
pub const XLHL_XMAX_IS_MULTI: uint8 = 0x01;
pub const XLHL_XMAX_LOCK_ONLY: uint8 = 0x02;
pub const XLHL_XMAX_EXCL_LOCK: uint8 = 0x04;
pub const XLHL_XMAX_KEYSHR_LOCK: uint8 = 0x08;
pub const XLHL_KEYS_UPDATED: uint8 = 0x10;

/// flag bits for xl_heap_lock / xl_heap_lock_updated's flag field
pub const XLH_LOCK_ALL_FROZEN_CLEARED: uint8 = 0x01;

/* ----------------------------------------------------------------
 * access/heapam_xlog.h: the remaining heap WAL records.
 *
 * On-the-wire ABI structs, so `#[repr(C)]` with compile-time layout checks.
 * ---------------------------------------------------------------- */

/// `xl_heap_delete` — the main `XLOG_HEAP_DELETE` record.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct xl_heap_delete {
    /// xmax of the deleted tuple.
    pub xmax: TransactionId,
    /// deleted tuple's offset.
    pub offnum: OffsetNumber,
    /// infomask bits.
    pub infobits_set: uint8,
    pub flags: uint8,
}

/// `SizeOfHeapDelete` == `offsetof(xl_heap_delete, flags) + sizeof(uint8)`.
pub const SizeOfHeapDelete: usize = core::mem::offset_of!(xl_heap_delete, flags) + 1;

/// `xl_heap_insert` — the main `XLOG_HEAP_INSERT` record.  The `xl_heap_header`
/// and tuple data follow in backup block 0.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct xl_heap_insert {
    /// inserted tuple's offset.
    pub offnum: OffsetNumber,
    pub flags: uint8,
}

/// `SizeOfHeapInsert` == `offsetof(xl_heap_insert, flags) + sizeof(uint8)`.
pub const SizeOfHeapInsert: usize = core::mem::offset_of!(xl_heap_insert, flags) + 1;

/// `xl_heap_multi_insert` — the main `XLOG_HEAP2_MULTI_INSERT` record.  The
/// `offsets` array (omitted when the page is reinitialized) and the per-tuple
/// `xl_multi_insert_tuple` + data live in block 0.  In C `offsets` is a flexible
/// array member; we store only the fixed header and read the offsets from the
/// raw record bytes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct xl_heap_multi_insert {
    pub flags: uint8,
    pub ntuples: uint16,
    pub offsets: [OffsetNumber; 0],
}

/// `SizeOfHeapMultiInsert` == `offsetof(xl_heap_multi_insert, offsets)`.
pub const SizeOfHeapMultiInsert: usize = core::mem::offset_of!(xl_heap_multi_insert, offsets);

/// `xl_multi_insert_tuple` — per-tuple header inside a multi-insert's block data,
/// followed by the tuple data.  Each one is `SHORTALIGN`ed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct xl_multi_insert_tuple {
    /// size of tuple data that follows.
    pub datalen: uint16,
    pub t_infomask2: uint16,
    pub t_infomask: uint16,
    pub t_hoff: uint8,
}

/// `SizeOfMultiInsertTuple` == `offsetof(xl_multi_insert_tuple, t_hoff) + sizeof(uint8)`.
pub const SizeOfMultiInsertTuple: usize = core::mem::offset_of!(xl_multi_insert_tuple, t_hoff) + 1;

/// `xl_heap_lock` — the main `XLOG_HEAP_LOCK` record.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct xl_heap_lock {
    /// might be a MultiXactId.
    pub xmax: TransactionId,
    /// locked tuple's offset on page.
    pub offnum: OffsetNumber,
    /// infomask and infomask2 bits to set.
    pub infobits_set: uint8,
    /// XLH_LOCK_* flag bits.
    pub flags: uint8,
}

/// `SizeOfHeapLock` == `offsetof(xl_heap_lock, flags) + sizeof(uint8)`.
pub const SizeOfHeapLock: usize = core::mem::offset_of!(xl_heap_lock, flags) + 1;

/// `xl_heap_lock_updated` — the main `XLOG_HEAP2_LOCK_UPDATED` record.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct xl_heap_lock_updated {
    pub xmax: TransactionId,
    pub offnum: OffsetNumber,
    pub infobits_set: uint8,
    pub flags: uint8,
}

/// `SizeOfHeapLockUpdated` == `offsetof(xl_heap_lock_updated, flags) + sizeof(uint8)`.
pub const SizeOfHeapLockUpdated: usize = core::mem::offset_of!(xl_heap_lock_updated, flags) + 1;

/// `xl_heap_confirm` — the main `XLOG_HEAP_CONFIRM` (speculative-insert
/// confirmation) record.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct xl_heap_confirm {
    /// confirmed tuple's offset on page.
    pub offnum: OffsetNumber,
}

/// `SizeOfHeapConfirm` == `offsetof(xl_heap_confirm, offnum) + sizeof(OffsetNumber)`.
pub const SizeOfHeapConfirm: usize =
    core::mem::offset_of!(xl_heap_confirm, offnum) + core::mem::size_of::<OffsetNumber>();

/// `xl_heap_inplace` — the main `XLOG_HEAP_INPLACE` record.  The shared-invalidation
/// messages are a flexible array member; we store only the fixed header (matching
/// `MinSizeOfHeapInplace`) and read the messages from the raw record bytes.
///
/// `SharedInvalidationMessage` is a `union` (no `Debug`/`Eq`), so this struct
/// only derives `Clone`/`Copy`.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct xl_heap_inplace {
    /// updated tuple's offset on page.
    pub offnum: OffsetNumber,
    /// MyDatabaseId.
    pub dbId: Oid,
    /// MyDatabaseTableSpace.
    pub tsId: Oid,
    /// invalidate relcache init files.
    pub relcacheInitFileInval: bool,
    /// number of shared inval msgs.
    pub nmsgs: i32,
    pub msgs: [SharedInvalidationMessage; 0],
}

/// `MinSizeOfHeapInplace` == `offsetof(xl_heap_inplace, nmsgs) + sizeof(int)`.
pub const MinSizeOfHeapInplace: usize =
    core::mem::offset_of!(xl_heap_inplace, nmsgs) + core::mem::size_of::<i32>();

/// `xl_heap_visible` — the main `XLOG_HEAP2_VISIBLE` record.  Backup block 0 is
/// the visibility-map buffer; backup block 1 is the heap buffer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct xl_heap_visible {
    pub snapshotConflictHorizon: TransactionId,
    pub flags: uint8,
}

/// `SizeOfHeapVisible` == `offsetof(xl_heap_visible, flags) + sizeof(uint8)`.
pub const SizeOfHeapVisible: usize = core::mem::offset_of!(xl_heap_visible, flags) + 1;

/// `xl_heap_truncate` flag values (8 bits available).
pub const XLH_TRUNCATE_CASCADE: uint8 = 1 << 0;
pub const XLH_TRUNCATE_RESTART_SEQS: uint8 = 1 << 1;

/// `xl_heap_truncate` — the main `XLOG_HEAP_TRUNCATE` record.  The truncated
/// relids (and any sequences to restart) are a flexible array member; we store
/// only the fixed header (matching `SizeOfHeapTruncate`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct xl_heap_truncate {
    pub dbId: Oid,
    pub nrelids: uint32,
    pub flags: uint8,
    pub relids: [Oid; 0],
}

/// `SizeOfHeapTruncate` == `offsetof(xl_heap_truncate, relids)`.
pub const SizeOfHeapTruncate: usize = core::mem::offset_of!(xl_heap_truncate, relids);

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn heap_tuple_freeze_layout() {
        // TransactionId + 2*uint16 + 2*uint8 + OffsetNumber, 4-byte aligned.
        assert_eq!(align_of::<HeapTupleFreeze>(), 4);
        assert_eq!(size_of::<HeapTupleFreeze>(), 12);
    }

    #[test]
    fn htsv_result_discriminants() {
        assert_eq!(HEAPTUPLE_DEAD as i32, 0);
        assert_eq!(HEAPTUPLE_DELETE_IN_PROGRESS as i32, 4);
    }

    #[test]
    fn vm_flag_bits() {
        assert_eq!(VISIBILITYMAP_VALID_BITS, 0x03);
        assert_eq!(VISIBILITYMAP_XLOG_VALID_BITS, 0x07);
    }

    #[test]
    fn prune_wal_record_layout() {
        // xl_heap_prune: two uint8 fields, no padding.
        assert_eq!(size_of::<xl_heap_prune>(), 2);
        assert_eq!(offset_of!(xl_heap_prune, reason), 0);
        assert_eq!(offset_of!(xl_heap_prune, flags), 1);
        assert_eq!(SizeOfHeapPrune, 2);

        // xlhp_freeze_plan: TransactionId(4) + 2*uint16 + uint8 + pad + uint16.
        assert_eq!(align_of::<xlhp_freeze_plan>(), 4);
        assert_eq!(size_of::<xlhp_freeze_plan>(), 12);
        assert_eq!(offset_of!(xlhp_freeze_plan, xmax), 0);
        assert_eq!(offset_of!(xlhp_freeze_plan, t_infomask2), 4);
        assert_eq!(offset_of!(xlhp_freeze_plan, t_infomask), 6);
        assert_eq!(offset_of!(xlhp_freeze_plan, frzflags), 8);
        assert_eq!(offset_of!(xlhp_freeze_plan, ntuples), 10);

        // The header part registered in the WAL record is offsetof(.., plans).
        assert_eq!(offset_of!(xlhp_freeze_plans, plans), 4);
        // The header part registered in the WAL record is offsetof(.., data).
        assert_eq!(offset_of!(xlhp_prune_items, data), 2);
    }

    #[test]
    fn prune_wal_flag_bits() {
        assert_eq!(XLHP_IS_CATALOG_REL, 0x02);
        assert_eq!(XLHP_CLEANUP_LOCK, 0x04);
        assert_eq!(XLHP_HAS_CONFLICT_HORIZON, 0x08);
        assert_eq!(XLHP_HAS_FREEZE_PLANS, 0x10);
        assert_eq!(XLHP_HAS_REDIRECTIONS, 0x20);
        assert_eq!(XLHP_HAS_DEAD_ITEMS, 0x40);
        assert_eq!(XLHP_HAS_NOW_UNUSED_ITEMS, 0x80);
    }

    #[test]
    fn heap_header_wal_layout() {
        // xl_heap_header: 2*uint16 + uint8 (no trailing pad needed for SizeOf).
        assert_eq!(offset_of!(xl_heap_header, t_infomask2), 0);
        assert_eq!(offset_of!(xl_heap_header, t_infomask), 2);
        assert_eq!(offset_of!(xl_heap_header, t_hoff), 4);
        assert_eq!(SizeOfHeapHeader, 5);
    }

    #[test]
    fn heap_update_wal_layout() {
        // TransactionId(4) + OffsetNumber(2) + 2*uint8 + TransactionId(4) + OffsetNumber(2).
        assert_eq!(align_of::<xl_heap_update>(), 4);
        assert_eq!(offset_of!(xl_heap_update, old_xmax), 0);
        assert_eq!(offset_of!(xl_heap_update, old_offnum), 4);
        assert_eq!(offset_of!(xl_heap_update, old_infobits_set), 6);
        assert_eq!(offset_of!(xl_heap_update, flags), 7);
        assert_eq!(offset_of!(xl_heap_update, new_xmax), 8);
        assert_eq!(offset_of!(xl_heap_update, new_offnum), 12);
        assert_eq!(SizeOfHeapUpdate, 14);
    }

    #[test]
    fn heap_new_cid_wal_layout() {
        // top_xid(4) + cmin(4) + cmax(4) + combocid(4) + RelFileLocator(12) + ItemPointerData(6).
        assert_eq!(offset_of!(xl_heap_new_cid, top_xid), 0);
        assert_eq!(offset_of!(xl_heap_new_cid, cmin), 4);
        assert_eq!(offset_of!(xl_heap_new_cid, cmax), 8);
        assert_eq!(offset_of!(xl_heap_new_cid, combocid), 12);
        assert_eq!(offset_of!(xl_heap_new_cid, target_locator), 16);
        assert_eq!(offset_of!(xl_heap_new_cid, target_tid), 28);
        assert_eq!(SizeOfHeapNewCid, 34);
    }

    #[test]
    fn heap_update_flag_bits() {
        assert_eq!(XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED, 0x01);
        assert_eq!(XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED, 0x02);
        assert_eq!(XLH_UPDATE_CONTAINS_OLD_TUPLE, 0x04);
        assert_eq!(XLH_UPDATE_CONTAINS_OLD_KEY, 0x08);
        assert_eq!(XLH_UPDATE_CONTAINS_NEW_TUPLE, 0x10);
        assert_eq!(XLH_UPDATE_PREFIX_FROM_OLD, 0x20);
        assert_eq!(XLH_UPDATE_SUFFIX_FROM_OLD, 0x40);
        assert_eq!(XLH_UPDATE_CONTAINS_OLD, 0x0c);
        assert_eq!(XLOG_HEAP2_NEW_CID, 0x70);
    }

    #[test]
    fn heap_xlog_record_opcodes() {
        assert_eq!(XLOG_HEAP_INSERT, 0x00);
        assert_eq!(XLOG_HEAP_DELETE, 0x10);
        assert_eq!(XLOG_HEAP_UPDATE, 0x20);
        assert_eq!(XLOG_HEAP_TRUNCATE, 0x30);
        assert_eq!(XLOG_HEAP_HOT_UPDATE, 0x40);
        assert_eq!(XLOG_HEAP_CONFIRM, 0x50);
        assert_eq!(XLOG_HEAP_LOCK, 0x60);
        assert_eq!(XLOG_HEAP_INPLACE, 0x70);
        assert_eq!(XLOG_HEAP_OPMASK, 0x70);
        assert_eq!(XLOG_HEAP_INIT_PAGE, 0x80);

        assert_eq!(XLOG_HEAP2_REWRITE, 0x00);
        assert_eq!(XLOG_HEAP2_VISIBLE, 0x40);
        assert_eq!(XLOG_HEAP2_MULTI_INSERT, 0x50);
        assert_eq!(XLOG_HEAP2_LOCK_UPDATED, 0x60);
    }

    #[test]
    fn heap_xlog_insert_delete_lock_flag_bits() {
        assert_eq!(XLH_INSERT_ALL_VISIBLE_CLEARED, 0x01);
        assert_eq!(XLH_INSERT_LAST_IN_MULTI, 0x02);
        assert_eq!(XLH_INSERT_IS_SPECULATIVE, 0x04);
        assert_eq!(XLH_INSERT_CONTAINS_NEW_TUPLE, 0x08);
        assert_eq!(XLH_INSERT_ON_TOAST_RELATION, 0x10);
        assert_eq!(XLH_INSERT_ALL_FROZEN_SET, 0x20);

        assert_eq!(XLH_DELETE_ALL_VISIBLE_CLEARED, 0x01);
        assert_eq!(XLH_DELETE_CONTAINS_OLD_TUPLE, 0x02);
        assert_eq!(XLH_DELETE_CONTAINS_OLD_KEY, 0x04);
        assert_eq!(XLH_DELETE_IS_SUPER, 0x08);
        assert_eq!(XLH_DELETE_IS_PARTITION_MOVE, 0x10);
        assert_eq!(XLH_DELETE_CONTAINS_OLD, 0x06);

        assert_eq!(XLHL_XMAX_IS_MULTI, 0x01);
        assert_eq!(XLHL_XMAX_LOCK_ONLY, 0x02);
        assert_eq!(XLHL_XMAX_EXCL_LOCK, 0x04);
        assert_eq!(XLHL_XMAX_KEYSHR_LOCK, 0x08);
        assert_eq!(XLHL_KEYS_UPDATED, 0x10);
        assert_eq!(XLH_LOCK_ALL_FROZEN_CLEARED, 0x01);

        assert_eq!(XLH_TRUNCATE_CASCADE, 0x01);
        assert_eq!(XLH_TRUNCATE_RESTART_SEQS, 0x02);
    }

    #[test]
    fn heap_xlog_record_layouts() {
        // xl_heap_delete: TransactionId(4) + OffsetNumber(2) + 2*uint8.
        assert_eq!(align_of::<xl_heap_delete>(), 4);
        assert_eq!(offset_of!(xl_heap_delete, xmax), 0);
        assert_eq!(offset_of!(xl_heap_delete, offnum), 4);
        assert_eq!(offset_of!(xl_heap_delete, infobits_set), 6);
        assert_eq!(offset_of!(xl_heap_delete, flags), 7);
        assert_eq!(SizeOfHeapDelete, 8);

        // xl_heap_insert: OffsetNumber(2) + uint8.
        assert_eq!(offset_of!(xl_heap_insert, offnum), 0);
        assert_eq!(offset_of!(xl_heap_insert, flags), 2);
        assert_eq!(SizeOfHeapInsert, 3);

        // xl_heap_multi_insert: uint8 + uint16 + offsets[]; SizeOf == offsetof(offsets).
        assert_eq!(offset_of!(xl_heap_multi_insert, flags), 0);
        assert_eq!(offset_of!(xl_heap_multi_insert, ntuples), 2);
        assert_eq!(SizeOfHeapMultiInsert, 4);

        // xl_multi_insert_tuple: 3*uint16 + uint8; SizeOf == offsetof(t_hoff)+1.
        assert_eq!(offset_of!(xl_multi_insert_tuple, datalen), 0);
        assert_eq!(offset_of!(xl_multi_insert_tuple, t_infomask2), 2);
        assert_eq!(offset_of!(xl_multi_insert_tuple, t_infomask), 4);
        assert_eq!(offset_of!(xl_multi_insert_tuple, t_hoff), 6);
        assert_eq!(SizeOfMultiInsertTuple, 7);

        // xl_heap_lock / xl_heap_lock_updated: TransactionId(4) + OffsetNumber(2) + 2*uint8.
        assert_eq!(offset_of!(xl_heap_lock, xmax), 0);
        assert_eq!(offset_of!(xl_heap_lock, offnum), 4);
        assert_eq!(offset_of!(xl_heap_lock, infobits_set), 6);
        assert_eq!(offset_of!(xl_heap_lock, flags), 7);
        assert_eq!(SizeOfHeapLock, 8);
        assert_eq!(offset_of!(xl_heap_lock_updated, flags), 7);
        assert_eq!(SizeOfHeapLockUpdated, 8);

        // xl_heap_confirm: a single OffsetNumber.
        assert_eq!(offset_of!(xl_heap_confirm, offnum), 0);
        assert_eq!(SizeOfHeapConfirm, 2);

        // xl_heap_visible: TransactionId(4) + uint8.
        assert_eq!(offset_of!(xl_heap_visible, snapshotConflictHorizon), 0);
        assert_eq!(offset_of!(xl_heap_visible, flags), 4);
        assert_eq!(SizeOfHeapVisible, 5);

        // xl_heap_inplace: OffsetNumber(2) + pad + Oid(4) + Oid(4) + bool(1) + pad + int(4).
        assert_eq!(offset_of!(xl_heap_inplace, offnum), 0);
        assert_eq!(offset_of!(xl_heap_inplace, dbId), 4);
        assert_eq!(offset_of!(xl_heap_inplace, tsId), 8);
        assert_eq!(offset_of!(xl_heap_inplace, relcacheInitFileInval), 12);
        assert_eq!(offset_of!(xl_heap_inplace, nmsgs), 16);
        assert_eq!(MinSizeOfHeapInplace, 20);

        // xl_heap_truncate: Oid(4) + uint32(4) + uint8 + relids[]; SizeOf ==
        // offsetof(relids) == align_up(9, alignof(Oid)=4) == 12.
        assert_eq!(offset_of!(xl_heap_truncate, dbId), 0);
        assert_eq!(offset_of!(xl_heap_truncate, nrelids), 4);
        assert_eq!(offset_of!(xl_heap_truncate, flags), 8);
        assert_eq!(SizeOfHeapTruncate, 12);
    }
}
