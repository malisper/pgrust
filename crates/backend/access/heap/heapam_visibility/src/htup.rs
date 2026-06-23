//! `htup_details.h` / `itemptr.h` accessor macros used by
//! `heapam_visibility.c`, over the owned [`HeapTupleHeaderData`] /
//! [`ItemPointerData`].
//!
//! These are pure bit/field manipulations with no external dependency, so they
//! live in this crate rather than behind a seam. The C `union` `t_choice`
//! (`t_heap` / `t_datum`) is the [`HeapTupleHeaderChoice`] enum; an on-page heap
//! tuple is always the `THeap` arm (the only variant these read-only visibility
//! accessors observe).

use types_core::primitive::{uint16, OffsetNumber, TransactionId};
use types_core::xact::{FrozenTransactionId, InvalidTransactionId};
use types_tuple::heaptuple::{
    HeapTupleField3, HeapTupleHeaderChoice, HeapTupleHeaderData, ItemPointerData, HEAP_MOVED,
    HEAP_XMAX_EXCL_LOCK, HEAP_XMAX_IS_MULTI, HEAP_XMAX_KEYSHR_LOCK, HEAP_XMAX_LOCK_ONLY,
    HEAP_XMIN_COMMITTED, HEAP_XMIN_FROZEN, HEAP_XMIN_INVALID, INVALID_OFFSET_NUMBER,
};

/* ------------------------------------------------------------------ */
/* itemptr.h                                                          */
/* ------------------------------------------------------------------ */

/// `ItemPointerGetOffsetNumberNoCheck(pointer)`.
#[inline]
pub fn ItemPointerGetOffsetNumberNoCheck(pointer: &ItemPointerData) -> OffsetNumber {
    pointer.ip_posid
}

/// `ItemPointerGetBlockNumber(pointer)`.
#[inline]
pub fn ItemPointerGetBlockNumber(pointer: &ItemPointerData) -> u32 {
    pointer.ip_blkid.block_number()
}

/// `ItemPointerIsValid(pointer)` — non-null pointer with a valid offset. (The
/// owned model has no null pointer; a default `ItemPointerData` has
/// `ip_posid == InvalidOffsetNumber`, matching the C "null or invalid" sense.)
#[inline]
pub fn ItemPointerIsValid(pointer: &ItemPointerData) -> bool {
    pointer.ip_posid != INVALID_OFFSET_NUMBER
}

/// `ItemPointerEquals(pointer1, pointer2)`.
#[inline]
pub fn ItemPointerEquals(pointer1: &ItemPointerData, pointer2: &ItemPointerData) -> bool {
    pointer1.ip_blkid.block_number() == pointer2.ip_blkid.block_number()
        && pointer1.ip_posid == pointer2.ip_posid
}

/* ------------------------------------------------------------------ */
/* htup_details.h — lock masks                                         */
/* ------------------------------------------------------------------ */

/// `HEAP_XMAX_SHR_LOCK` == `HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK`.
pub const HEAP_XMAX_SHR_LOCK: uint16 = HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK;
/// `HEAP_LOCK_MASK` == `HEAP_XMAX_SHR_LOCK | HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK`.
pub const HEAP_LOCK_MASK: uint16 =
    HEAP_XMAX_SHR_LOCK | HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK;

/* ------------------------------------------------------------------ */
/* htup_details.h — infomask predicate macros                          */
/* ------------------------------------------------------------------ */

/// `HEAP_XMAX_IS_LOCKED_ONLY(infomask)` (htup_details.h).
#[inline]
pub fn HEAP_XMAX_IS_LOCKED_ONLY(infomask: uint16) -> bool {
    (infomask & HEAP_XMAX_LOCK_ONLY) != 0
        || (infomask & (HEAP_XMAX_IS_MULTI | HEAP_LOCK_MASK)) == HEAP_XMAX_EXCL_LOCK
}

/// `HEAP_LOCKED_UPGRADED(infomask)` (htup_details.h) — a tuple with
/// `HEAP_XMAX_IS_MULTI` and `HEAP_XMAX_LOCK_ONLY` set but neither lock-strength
/// bit set: a share-locked-in-9.2-then-pg_upgrade'd multixact whose lockers are
/// all gone, so it may be considered not locked.
#[inline]
pub fn HEAP_LOCKED_UPGRADED(infomask: uint16) -> bool {
    (infomask & HEAP_XMAX_IS_MULTI) != 0
        && (infomask & HEAP_XMAX_LOCK_ONLY) != 0
        && (infomask & (HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK)) == 0
}

/* ------------------------------------------------------------------ */
/* htup_details.h — HeapTupleHeader field accessors                    */
/* ------------------------------------------------------------------ */

/// `HeapTupleHeaderGetRawXmax(tup)`.
#[inline]
pub fn HeapTupleHeaderGetRawXmax(tup: &HeapTupleHeaderData) -> TransactionId {
    match &tup.t_choice {
        HeapTupleHeaderChoice::THeap(f) => f.t_xmax,
        HeapTupleHeaderChoice::TDatum(_) => InvalidTransactionId,
    }
}

/// `HeapTupleHeaderXminInvalid(tup)`.
#[inline]
pub fn HeapTupleHeaderXminInvalid(tup: &HeapTupleHeaderData) -> bool {
    (tup.t_infomask & (HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID)) == HEAP_XMIN_INVALID
}

/// `HeapTupleHeaderXminFrozen(tup)`.
#[inline]
pub fn HeapTupleHeaderXminFrozen(tup: &HeapTupleHeaderData) -> bool {
    (tup.t_infomask & HEAP_XMIN_FROZEN) == HEAP_XMIN_FROZEN
}

/// `HeapTupleHeaderGetXmin(tup)` — resolves a frozen xmin to
/// `FrozenTransactionId`.
#[inline]
pub fn HeapTupleHeaderGetXmin(tup: &HeapTupleHeaderData) -> TransactionId {
    if HeapTupleHeaderXminFrozen(tup) {
        FrozenTransactionId
    } else {
        types_tuple::heaptuple::HeapTupleHeaderGetRawXmin(tup)
    }
}

/// `HeapTupleHeaderGetXvac(tup)` — the vacuum-move xid; only meaningful when
/// `HEAP_MOVED`. The `t_xvac` value lives in the `TXvac` arm of `t_field3`.
#[inline]
pub fn HeapTupleHeaderGetXvac(tup: &HeapTupleHeaderData) -> TransactionId {
    if (tup.t_infomask & HEAP_MOVED) != 0 {
        match &tup.t_choice {
            HeapTupleHeaderChoice::THeap(f) => match f.t_field3 {
                HeapTupleField3::TXvac(t_xvac) => t_xvac,
                HeapTupleField3::TCid(_) => InvalidTransactionId,
            },
            HeapTupleHeaderChoice::TDatum(_) => InvalidTransactionId,
        }
    } else {
        InvalidTransactionId
    }
}

/* ------------------------------------------------------------------ */
/* htup_details.h — speculative-insertion accessors                    */
/* ------------------------------------------------------------------ */

/// `SpecTokenOffsetNumber` (htup_details.h) — the special ctid offset that marks
/// a speculatively-inserted tuple.
pub const SpecTokenOffsetNumber: OffsetNumber = 0xfffe;

/// `HeapTupleHeaderIsSpeculative(tup)` — the ctid offset is the spec token.
#[inline]
pub fn HeapTupleHeaderIsSpeculative(tup: &HeapTupleHeaderData) -> bool {
    ItemPointerGetOffsetNumberNoCheck(&tup.t_ctid) == SpecTokenOffsetNumber
}

/// `HeapTupleHeaderGetSpeculativeToken(tup)` — the ctid block number doubles as
/// the token.
#[inline]
pub fn HeapTupleHeaderGetSpeculativeToken(tup: &HeapTupleHeaderData) -> u32 {
    ItemPointerGetBlockNumber(&tup.t_ctid)
}
