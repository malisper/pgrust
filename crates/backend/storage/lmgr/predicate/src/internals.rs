// predicate_internals.h structs, #[repr(C)] so the intrusive-dlist pointer
// arithmetic in engine.rs matches C's layout assumptions.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use lwlock::LWLock;
use types_core::{
    BlockNumber, InvalidBlockNumber, OffsetNumber, Oid, TransactionId, VirtualTransactionId,
};

use crate::ilist::{dlist_head, dlist_node};

pub type SerCommitSeqNo = u64;

// 0 reserved (non-existent SLRU entry); Invalid sorts above all valid seqnos.
pub const InvalidSerCommitSeqNo: SerCommitSeqNo = u64::MAX;
pub const RecoverySerCommitSeqNo: SerCommitSeqNo = 1;
pub const FirstNormalSerCommitSeqNo: SerCommitSeqNo = 2;

pub const SXACT_FLAG_COMMITTED: u32 = 0x0000_0001;
pub const SXACT_FLAG_PREPARED: u32 = 0x0000_0002;
pub const SXACT_FLAG_ROLLED_BACK: u32 = 0x0000_0004;
pub const SXACT_FLAG_DOOMED: u32 = 0x0000_0008;
pub const SXACT_FLAG_CONFLICT_OUT: u32 = 0x0000_0010;
pub const SXACT_FLAG_READ_ONLY: u32 = 0x0000_0020;
pub const SXACT_FLAG_DEFERRABLE_WAITING: u32 = 0x0000_0040;
pub const SXACT_FLAG_RO_SAFE: u32 = 0x0000_0080;
pub const SXACT_FLAG_RO_UNSAFE: u32 = 0x0000_0100;
pub const SXACT_FLAG_SUMMARY_CONFLICT_IN: u32 = 0x0000_0200;
pub const SXACT_FLAG_SUMMARY_CONFLICT_OUT: u32 = 0x0000_0400;
pub const SXACT_FLAG_PARTIALLY_RELEASED: u32 = 0x0000_0800;

#[repr(C)]
#[derive(Clone, Copy)]
pub union SerializableXactSeqNo {
    pub earliestOutConflictCommit: SerCommitSeqNo,
    pub lastCommitBeforeSnapshot: SerCommitSeqNo,
}

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

pub const InvalidSerializableXact: *mut SERIALIZABLEXACT = core::ptr::null_mut();

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

#[inline]
pub fn PredXactListDataSize() -> usize {
    maxalign(size_of::<PredXactListData>())
}

#[repr(C)]
pub struct RWConflictData {
    pub outLink: dlist_node,
    pub inLink: dlist_node,
    pub sxactOut: *mut SERIALIZABLEXACT,
    pub sxactIn: *mut SERIALIZABLEXACT,
}

pub type RWConflict = *mut RWConflictData;

#[inline]
pub fn RWConflictDataSize() -> usize {
    maxalign(size_of::<RWConflictData>())
}

#[repr(C)]
pub struct RWConflictPoolHeaderData {
    pub availableList: dlist_head,
    pub element: RWConflict,
}

pub type RWConflictPoolHeader = *mut RWConflictPoolHeaderData;

#[inline]
pub fn RWConflictPoolHeaderDataSize() -> usize {
    maxalign(size_of::<RWConflictPoolHeaderData>())
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SERIALIZABLEXIDTAG {
    pub xid: TransactionId,
}

#[repr(C)]
pub struct SERIALIZABLEXID {
    pub tag: SERIALIZABLEXIDTAG,
    pub myXact: *mut SERIALIZABLEXACT,
}

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PREDICATELOCKTARGETTAG {
    pub locktag_field1: u32,
    pub locktag_field2: u32,
    pub locktag_field3: u32,
    pub locktag_field4: u32,
}

pub const ZERO_TARGET_TAG: PREDICATELOCKTARGETTAG = PREDICATELOCKTARGETTAG {
    locktag_field1: 0,
    locktag_field2: 0,
    locktag_field3: 0,
    locktag_field4: 0,
};

#[repr(C)]
pub struct PREDICATELOCKTARGET {
    pub tag: PREDICATELOCKTARGETTAG,
    pub predicateLocks: dlist_head,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PREDICATELOCKTAG {
    pub myTarget: *mut PREDICATELOCKTARGET,
    pub myXact: *mut SERIALIZABLEXACT,
}

#[repr(C)]
pub struct PREDICATELOCK {
    pub tag: PREDICATELOCKTAG,
    pub targetLink: dlist_node,
    pub xactLink: dlist_node,
    pub commitSeqNo: SerCommitSeqNo,
}

#[repr(C)]
pub struct LOCALPREDICATELOCK {
    pub tag: PREDICATELOCKTARGETTAG,
    pub held: bool,
    pub childLocks: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredicateLockTargetType {
    PREDLOCKTAG_RELATION,
    PREDLOCKTAG_PAGE,
    PREDLOCKTAG_TUPLE,
}
pub use PredicateLockTargetType::*;

pub const MAXIMUM_ALIGNOF: usize = 8;

#[inline]
pub const fn maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

pub const InvalidOffsetNumber: OffsetNumber = 0;

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

#[inline]
pub fn GET_PREDICATELOCKTARGETTAG_DB(locktag: &PREDICATELOCKTARGETTAG) -> Oid {
    locktag.locktag_field1
}

#[inline]
pub fn GET_PREDICATELOCKTARGETTAG_RELATION(locktag: &PREDICATELOCKTARGETTAG) -> Oid {
    locktag.locktag_field2
}

#[inline]
pub fn GET_PREDICATELOCKTARGETTAG_PAGE(locktag: &PREDICATELOCKTARGETTAG) -> BlockNumber {
    locktag.locktag_field3
}

#[inline]
pub fn GET_PREDICATELOCKTARGETTAG_OFFSET(locktag: &PREDICATELOCKTARGETTAG) -> OffsetNumber {
    locktag.locktag_field4 as OffsetNumber
}

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

#[inline]
pub unsafe fn SxactIsOnFinishedList(sxact: *const SERIALIZABLEXACT) -> bool {
    !crate::ilist::dlist_node_is_detached(&(*sxact).finishedLink)
}
