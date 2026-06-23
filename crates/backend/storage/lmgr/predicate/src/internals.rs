//! `predicate_internals.h` structs and tag/flag helpers — `#[repr(C)]`,
//! field-for-field with the C header, so the intrusive-`dlist` pointer
//! arithmetic in `engine.rs` is byte-compatible with C.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use ilist::{dlist_head, dlist_node};
use types_storage::LWLock;
use types_core::primitive::{BlockNumber, OffsetNumber, Oid, ProcNumber};
use types_core::TransactionId;
use types_core::VirtualTransactionId;

/// `SerCommitSeqNo` — commit sequence number (predicate_internals.h).
pub type SerCommitSeqNo = u64;

/// 0 is reserved (non-existent SLRU entry); `InvalidSerCommitSeqNo` is greater
/// than all valid ones so comparisons treat "not committed yet" as latest.
pub const InvalidSerCommitSeqNo: SerCommitSeqNo = u64::MAX;
/// Recovered-prepared transactions before a crash/recovery boundary.
pub const RecoverySerCommitSeqNo: SerCommitSeqNo = 1;
/// First sequence number a normal commit may use.
pub const FirstNormalSerCommitSeqNo: SerCommitSeqNo = 2;

// ---------------------------------------------------------------------------
// SXACT_FLAG_* — SERIALIZABLEXACT.flags bits (predicate_internals.h).
// ---------------------------------------------------------------------------

pub const SXACT_FLAG_COMMITTED: u32 = 0x0000_0001;
pub const SXACT_FLAG_PREPARED: u32 = 0x0000_0002;
pub const SXACT_FLAG_ROLLED_BACK: u32 = 0x0000_0004;
pub const SXACT_FLAG_DOOMED: u32 = 0x0000_0008;
/// Conflict out *to a transaction which committed ahead of it*.
pub const SXACT_FLAG_CONFLICT_OUT: u32 = 0x0000_0010;
pub const SXACT_FLAG_READ_ONLY: u32 = 0x0000_0020;
pub const SXACT_FLAG_DEFERRABLE_WAITING: u32 = 0x0000_0040;
pub const SXACT_FLAG_RO_SAFE: u32 = 0x0000_0080;
pub const SXACT_FLAG_RO_UNSAFE: u32 = 0x0000_0100;
pub const SXACT_FLAG_SUMMARY_CONFLICT_IN: u32 = 0x0000_0200;
pub const SXACT_FLAG_SUMMARY_CONFLICT_OUT: u32 = 0x0000_0400;
pub const SXACT_FLAG_PARTIALLY_RELEASED: u32 = 0x0000_0800;

/// `union { earliestOutConflictCommit; lastCommitBeforeSnapshot; }`
/// (predicate_internals.h: SERIALIZABLEXACT.SeqNo).
#[repr(C)]
#[derive(Clone, Copy)]
pub union SerializableXactSeqNo {
    /// when committed with conflict out
    pub earliestOutConflictCommit: SerCommitSeqNo,
    /// when not committed or no conflict out
    pub lastCommitBeforeSnapshot: SerCommitSeqNo,
}

/// `SERIALIZABLEXACT` (predicate_internals.h).
#[repr(C)]
pub struct SERIALIZABLEXACT {
    pub vxid: VirtualTransactionId,
    pub prepareSeqNo: SerCommitSeqNo,
    pub commitSeqNo: SerCommitSeqNo,
    pub SeqNo: SerializableXactSeqNo,
    pub outConflicts: dlist_head,
    pub inConflicts: dlist_head,
    pub predicateLocks: dlist_head,
    pub finishedLink: dlist_node,
    pub xactLink: dlist_node,
    pub perXactPredicateListLock: LWLock,
    pub possibleUnsafeConflicts: dlist_head,
    pub topXid: TransactionId,
    pub finishedBefore: TransactionId,
    pub xmin: TransactionId,
    pub flags: u32,
    pub pid: i32,
    pub pgprocno: i32,
}

/// `InvalidSerializableXact` == `(SERIALIZABLEXACT *) NULL`.
pub const InvalidSerializableXact: *mut SERIALIZABLEXACT = core::ptr::null_mut();

/// `PredXactListData` (predicate_internals.h).
#[repr(C)]
pub struct PredXactListData {
    pub availableList: dlist_head,
    pub activeList: dlist_head,
    pub SxactGlobalXmin: TransactionId,
    pub SxactGlobalXminCount: i32,
    pub WritableSxactCount: i32,
    pub LastSxactCommitSeqNo: SerCommitSeqNo,
    pub CanPartialClearThrough: SerCommitSeqNo,
    pub HavePartialClearedThrough: SerCommitSeqNo,
    pub OldCommittedSxact: *mut SERIALIZABLEXACT,
    pub element: *mut SERIALIZABLEXACT,
}

pub type PredXactList = *mut PredXactListData;

/// `PredXactListDataSize` == `MAXALIGN(sizeof(PredXactListData))`.
#[inline]
pub fn PredXactListDataSize() -> usize {
    maxalign(core::mem::size_of::<PredXactListData>())
}

/// `RWConflictData` (predicate_internals.h).
#[repr(C)]
pub struct RWConflictData {
    pub outLink: dlist_node,
    pub inLink: dlist_node,
    pub sxactOut: *mut SERIALIZABLEXACT,
    pub sxactIn: *mut SERIALIZABLEXACT,
}

pub type RWConflict = *mut RWConflictData;

/// `RWConflictDataSize` == `MAXALIGN(sizeof(RWConflictData))`.
#[inline]
pub fn RWConflictDataSize() -> usize {
    maxalign(core::mem::size_of::<RWConflictData>())
}

/// `RWConflictPoolHeaderData` (predicate_internals.h).
#[repr(C)]
pub struct RWConflictPoolHeaderData {
    pub availableList: dlist_head,
    pub element: RWConflict,
}

pub type RWConflictPoolHeader = *mut RWConflictPoolHeaderData;

/// `RWConflictPoolHeaderDataSize` == `MAXALIGN(sizeof(RWConflictPoolHeaderData))`.
#[inline]
pub fn RWConflictPoolHeaderDataSize() -> usize {
    maxalign(core::mem::size_of::<RWConflictPoolHeaderData>())
}

/// `SERIALIZABLEXIDTAG` (predicate_internals.h).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SERIALIZABLEXIDTAG {
    pub xid: TransactionId,
}

/// `SERIALIZABLEXID` (predicate_internals.h).
#[repr(C)]
pub struct SERIALIZABLEXID {
    pub tag: SERIALIZABLEXIDTAG,
    pub myXact: *mut SERIALIZABLEXACT,
}

/// `PREDICATELOCKTARGETTAG` (predicate_internals.h) — four 32-bit ID fields.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PREDICATELOCKTARGETTAG {
    pub locktag_field1: u32,
    pub locktag_field2: u32,
    pub locktag_field3: u32,
    pub locktag_field4: u32,
}

/// `PREDICATELOCKTARGET` (predicate_internals.h).
#[repr(C)]
pub struct PREDICATELOCKTARGET {
    pub tag: PREDICATELOCKTARGETTAG,
    pub predicateLocks: dlist_head,
}

/// `PREDICATELOCKTAG` (predicate_internals.h).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PREDICATELOCKTAG {
    pub myTarget: *mut PREDICATELOCKTARGET,
    pub myXact: *mut SERIALIZABLEXACT,
}

/// `PREDICATELOCK` (predicate_internals.h).
#[repr(C)]
pub struct PREDICATELOCK {
    pub tag: PREDICATELOCKTAG,
    pub targetLink: dlist_node,
    pub xactLink: dlist_node,
    pub commitSeqNo: SerCommitSeqNo,
}

/// `LOCALPREDICATELOCK` (predicate_internals.h).
#[repr(C)]
pub struct LOCALPREDICATELOCK {
    pub tag: PREDICATELOCKTARGETTAG,
    pub held: bool,
    pub childLocks: i32,
}

/// `PredicateLockTargetType` (predicate_internals.h).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredicateLockTargetType {
    PREDLOCKTAG_RELATION,
    PREDLOCKTAG_PAGE,
    PREDLOCKTAG_TUPLE,
}
pub use PredicateLockTargetType::*;

/// `TwoPhasePredicateRecordType` (predicate_internals.h).
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum TwoPhasePredicateRecordType {
    TWOPHASEPREDICATERECORD_XACT = 0,
    TWOPHASEPREDICATERECORD_LOCK = 1,
}
pub use TwoPhasePredicateRecordType::*;

/// `TwoPhasePredicateXactRecord` (predicate_internals.h).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TwoPhasePredicateXactRecord {
    pub xmin: TransactionId,
    pub flags: u32,
}

/// `TwoPhasePredicateLockRecord` (predicate_internals.h).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TwoPhasePredicateLockRecord {
    pub target: PREDICATELOCKTARGETTAG,
    pub filler: u32,
}

/// `union { xactRecord; lockRecord; }` (TwoPhasePredicateRecord.data).
#[repr(C)]
#[derive(Clone, Copy)]
pub union TwoPhasePredicateRecordData {
    pub xactRecord: TwoPhasePredicateXactRecord,
    pub lockRecord: TwoPhasePredicateLockRecord,
}

/// `TwoPhasePredicateRecord` (predicate_internals.h).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TwoPhasePredicateRecord {
    pub r#type: TwoPhasePredicateRecordType,
    pub data: TwoPhasePredicateRecordData,
}

/// `PredicateLockData` (predicate_internals.h) — pg_lock_status snapshot.
pub struct PredicateLockData {
    pub nelements: i32,
    pub locktags: Vec<PREDICATELOCKTARGETTAG>,
    pub xacts: Vec<SERIALIZABLEXACT_copy>,
}

/// A by-value copy of a `SERIALIZABLEXACT` (the C `*predlock->tag.myXact`
/// copied into `data->xacts[el]`). The C copy is a flat `SERIALIZABLEXACT`
/// (intrusive list links copied as raw bytes, harmless because callers only
/// read scalar fields). We keep the same flat layout.
pub type SERIALIZABLEXACT_copy = SerializableXactScalars;

/// The scalar fields callers of `GetPredicateLockStatusData` read; copied out
/// of the shared `SERIALIZABLEXACT` to avoid retaining the intrusive list
/// pointers in a reportable value.
#[derive(Clone, Copy)]
pub struct SerializableXactScalars {
    pub vxid: VirtualTransactionId,
    pub flags: u32,
    pub pid: i32,
    pub topXid: TransactionId,
    pub xmin: TransactionId,
}

// ---------------------------------------------------------------------------
// MAXALIGN.
// ---------------------------------------------------------------------------

/// `MAXIMUM_ALIGNOF` (typically 8 on the platforms we target).
pub const MAXIMUM_ALIGNOF: usize = 8;

/// `MAXALIGN(LEN)`.
#[inline]
pub const fn maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

// ---------------------------------------------------------------------------
// PREDICATELOCKTARGETTAG SET_*/GET_* macros (predicate_internals.h).
// ---------------------------------------------------------------------------

/// `InvalidBlockNumber`.
pub const InvalidBlockNumber: BlockNumber = types_core::primitive::InvalidBlockNumber;
/// `InvalidOffsetNumber` (storage/off.h) — 0.
pub const InvalidOffsetNumber: OffsetNumber = 0;

/// `SET_PREDICATELOCKTARGETTAG_RELATION(locktag, dboid, reloid)`.
#[inline]
pub fn SET_PREDICATELOCKTARGETTAG_RELATION(
    locktag: &mut PREDICATELOCKTARGETTAG,
    dboid: Oid,
    reloid: Oid,
) {
    locktag.locktag_field1 = dboid;
    locktag.locktag_field2 = reloid;
    locktag.locktag_field3 = InvalidBlockNumber;
    locktag.locktag_field4 = InvalidOffsetNumber as u32;
}

/// `SET_PREDICATELOCKTARGETTAG_PAGE(locktag, dboid, reloid, blocknum)`.
#[inline]
pub fn SET_PREDICATELOCKTARGETTAG_PAGE(
    locktag: &mut PREDICATELOCKTARGETTAG,
    dboid: Oid,
    reloid: Oid,
    blocknum: BlockNumber,
) {
    locktag.locktag_field1 = dboid;
    locktag.locktag_field2 = reloid;
    locktag.locktag_field3 = blocknum;
    locktag.locktag_field4 = InvalidOffsetNumber as u32;
}

/// `SET_PREDICATELOCKTARGETTAG_TUPLE(locktag, dboid, reloid, blocknum, offnum)`.
#[inline]
pub fn SET_PREDICATELOCKTARGETTAG_TUPLE(
    locktag: &mut PREDICATELOCKTARGETTAG,
    dboid: Oid,
    reloid: Oid,
    blocknum: BlockNumber,
    offnum: OffsetNumber,
) {
    locktag.locktag_field1 = dboid;
    locktag.locktag_field2 = reloid;
    locktag.locktag_field3 = blocknum;
    locktag.locktag_field4 = offnum as u32;
}

/// `GET_PREDICATELOCKTARGETTAG_DB(locktag)`.
#[inline]
pub fn GET_PREDICATELOCKTARGETTAG_DB(locktag: &PREDICATELOCKTARGETTAG) -> Oid {
    locktag.locktag_field1
}

/// `GET_PREDICATELOCKTARGETTAG_RELATION(locktag)`.
#[inline]
pub fn GET_PREDICATELOCKTARGETTAG_RELATION(locktag: &PREDICATELOCKTARGETTAG) -> Oid {
    locktag.locktag_field2
}

/// `GET_PREDICATELOCKTARGETTAG_PAGE(locktag)`.
#[inline]
pub fn GET_PREDICATELOCKTARGETTAG_PAGE(locktag: &PREDICATELOCKTARGETTAG) -> BlockNumber {
    locktag.locktag_field3
}

/// `GET_PREDICATELOCKTARGETTAG_OFFSET(locktag)`.
#[inline]
pub fn GET_PREDICATELOCKTARGETTAG_OFFSET(locktag: &PREDICATELOCKTARGETTAG) -> OffsetNumber {
    locktag.locktag_field4 as OffsetNumber
}

/// `GET_PREDICATELOCKTARGETTAG_TYPE(locktag)`.
#[inline]
pub fn GET_PREDICATELOCKTARGETTAG_TYPE(
    locktag: &PREDICATELOCKTARGETTAG,
) -> PredicateLockTargetType {
    if locktag.locktag_field4 != InvalidOffsetNumber as u32 {
        PREDLOCKTAG_TUPLE
    } else if locktag.locktag_field3 != InvalidBlockNumber {
        PREDLOCKTAG_PAGE
    } else {
        PREDLOCKTAG_RELATION
    }
}

/// `TargetTagIsCoveredBy(covered_target, covering_target)`.
#[inline]
pub fn TargetTagIsCoveredBy(
    covered_target: &PREDICATELOCKTARGETTAG,
    covering_target: &PREDICATELOCKTARGETTAG,
) -> bool {
    (GET_PREDICATELOCKTARGETTAG_RELATION(covered_target)
        == GET_PREDICATELOCKTARGETTAG_RELATION(covering_target))
        && (GET_PREDICATELOCKTARGETTAG_OFFSET(covering_target) == InvalidOffsetNumber)
        && (((GET_PREDICATELOCKTARGETTAG_OFFSET(covered_target) != InvalidOffsetNumber)
            && (GET_PREDICATELOCKTARGETTAG_PAGE(covering_target)
                == GET_PREDICATELOCKTARGETTAG_PAGE(covered_target)))
            || ((GET_PREDICATELOCKTARGETTAG_PAGE(covering_target) == InvalidBlockNumber)
                && (GET_PREDICATELOCKTARGETTAG_PAGE(covered_target) != InvalidBlockNumber)))
        && (GET_PREDICATELOCKTARGETTAG_DB(covered_target)
            == GET_PREDICATELOCKTARGETTAG_DB(covering_target))
}

// ---------------------------------------------------------------------------
// SxactIs* / SxactHas* flag accessors (predicate.c).
// They take a `*const SERIALIZABLEXACT`; we dereference the `flags` field.
// ---------------------------------------------------------------------------

macro_rules! sxact_flag_accessor {
    ($name:ident, $flag:ident) => {
        #[inline]
        pub unsafe fn $name(sxact: *const SERIALIZABLEXACT) -> bool {
            ((*sxact).flags & $flag) != 0
        }
    };
}

sxact_flag_accessor!(SxactIsCommitted, SXACT_FLAG_COMMITTED);
sxact_flag_accessor!(SxactIsPrepared, SXACT_FLAG_PREPARED);
sxact_flag_accessor!(SxactIsRolledBack, SXACT_FLAG_ROLLED_BACK);
sxact_flag_accessor!(SxactIsDoomed, SXACT_FLAG_DOOMED);
sxact_flag_accessor!(SxactIsReadOnly, SXACT_FLAG_READ_ONLY);
sxact_flag_accessor!(SxactHasSummaryConflictIn, SXACT_FLAG_SUMMARY_CONFLICT_IN);
sxact_flag_accessor!(SxactHasSummaryConflictOut, SXACT_FLAG_SUMMARY_CONFLICT_OUT);
sxact_flag_accessor!(SxactHasConflictOut, SXACT_FLAG_CONFLICT_OUT);
sxact_flag_accessor!(SxactIsDeferrableWaiting, SXACT_FLAG_DEFERRABLE_WAITING);
sxact_flag_accessor!(SxactIsROSafe, SXACT_FLAG_RO_SAFE);
sxact_flag_accessor!(SxactIsROUnsafe, SXACT_FLAG_RO_UNSAFE);
sxact_flag_accessor!(SxactIsPartiallyReleased, SXACT_FLAG_PARTIALLY_RELEASED);

/// `SxactIsOnFinishedList(sxact)` == `!dlist_node_is_detached(&sxact->finishedLink)`.
#[inline]
pub unsafe fn SxactIsOnFinishedList(sxact: *const SERIALIZABLEXACT) -> bool {
    !crate::ilist_inline::dlist_node_is_detached(&(*sxact).finishedLink)
}

/// `INVALID_PROC_NUMBER`.
pub const INVALID_PROC_NUMBER: ProcNumber = types_core::primitive::INVALID_PROC_NUMBER;
