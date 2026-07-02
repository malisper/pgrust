use ::types_core::{
    BlockNumber, CommandId, FrozenTransactionId, InvalidTransactionId, Oid, TransactionId,
};

use crate::itemptr::{
    ItemPointerData, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumberNoCheck,
    ItemPointerIndicatesMovedPartitions, ItemPointerSet, ItemPointerSetMovedPartitions,
    SpecTokenOffsetNumber,
};
use crate::varatt::{set_varsize_4b_word, varsize_4b_word};

pub type bits8 = u8;

pub const MaxTupleAttributeNumber: i32 = 1664;
pub const MaxHeapAttributeNumber: i32 = 1600;

pub const HEAP_HASNULL: u16 = 0x0001;
pub const HEAP_HASVARWIDTH: u16 = 0x0002;
pub const HEAP_HASEXTERNAL: u16 = 0x0004;
pub const HEAP_HASOID_OLD: u16 = 0x0008;
pub const HEAP_XMAX_KEYSHR_LOCK: u16 = 0x0010;
pub const HEAP_COMBOCID: u16 = 0x0020;
pub const HEAP_XMAX_EXCL_LOCK: u16 = 0x0040;
pub const HEAP_XMAX_LOCK_ONLY: u16 = 0x0080;
pub const HEAP_XMAX_SHR_LOCK: u16 = HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK;
pub const HEAP_LOCK_MASK: u16 = HEAP_XMAX_SHR_LOCK | HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK;
pub const HEAP_XMIN_COMMITTED: u16 = 0x0100;
pub const HEAP_XMIN_INVALID: u16 = 0x0200;
pub const HEAP_XMIN_FROZEN: u16 = HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID;
pub const HEAP_XMAX_COMMITTED: u16 = 0x0400;
pub const HEAP_XMAX_INVALID: u16 = 0x0800;
pub const HEAP_XMAX_IS_MULTI: u16 = 0x1000;
pub const HEAP_UPDATED: u16 = 0x2000;
pub const HEAP_MOVED_OFF: u16 = 0x4000;
pub const HEAP_MOVED_IN: u16 = 0x8000;
pub const HEAP_MOVED: u16 = HEAP_MOVED_OFF | HEAP_MOVED_IN;
pub const HEAP_XACT_MASK: u16 = 0xFFF0;
pub const HEAP_XMAX_BITS: u16 =
    HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID | HEAP_XMAX_IS_MULTI | HEAP_LOCK_MASK | HEAP_XMAX_LOCK_ONLY;

pub const HEAP_NATTS_MASK: u16 = 0x07FF;
pub const HEAP_KEYS_UPDATED: u16 = 0x2000;
pub const HEAP_HOT_UPDATED: u16 = 0x4000;
pub const HEAP_ONLY_TUPLE: u16 = 0x8000;
pub const HEAP2_XACT_MASK: u16 = 0xE000;
pub const HEAP_TUPLE_HAS_MATCH: u16 = HEAP_ONLY_TUPLE;

pub const SelfItemPointerAttributeNumber: i32 = -1;
pub const MinTransactionIdAttributeNumber: i32 = -2;
pub const MinCommandIdAttributeNumber: i32 = -3;
pub const MaxTransactionIdAttributeNumber: i32 = -4;
pub const MaxCommandIdAttributeNumber: i32 = -5;
pub const TableOidAttributeNumber: i32 = -6;
pub const FirstLowInvalidHeapAttributeNumber: i32 = -7;

#[inline]
pub const fn HEAP_XMAX_IS_LOCKED_ONLY(infomask: u16) -> bool {
    (infomask & HEAP_XMAX_LOCK_ONLY) != 0
        || (infomask & (HEAP_XMAX_IS_MULTI | HEAP_LOCK_MASK)) == HEAP_XMAX_EXCL_LOCK
}

#[inline]
pub const fn HEAP_LOCKED_UPGRADED(infomask: u16) -> bool {
    (infomask & HEAP_XMAX_IS_MULTI) != 0
        && (infomask & HEAP_XMAX_LOCK_ONLY) != 0
        && (infomask & (HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK)) == 0
}

#[inline]
pub const fn HEAP_XMAX_IS_SHR_LOCKED(infomask: u16) -> bool {
    (infomask & HEAP_LOCK_MASK) == HEAP_XMAX_SHR_LOCK
}

#[inline]
pub const fn HEAP_XMAX_IS_EXCL_LOCKED(infomask: u16) -> bool {
    (infomask & HEAP_LOCK_MASK) == HEAP_XMAX_EXCL_LOCK
}

#[inline]
pub const fn HEAP_XMAX_IS_KEYSHR_LOCKED(infomask: u16) -> bool {
    (infomask & HEAP_LOCK_MASK) == HEAP_XMAX_KEYSHR_LOCK
}

#[inline]
pub const fn BITMAPLEN(natts: i32) -> i32 {
    (natts + 7) / 8
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct HeapTupleFields {
    pub t_xmin: TransactionId,
    pub t_xmax: TransactionId,
    // C union { CommandId t_cid; TransactionId t_xvac; } — both u32.
    pub t_field3: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct DatumTupleFields {
    pub datum_len_: i32,
    pub datum_typmod: i32,
    pub datum_typeid: Oid,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub union HeapTupleHeaderChoice {
    pub t_heap: HeapTupleFields,
    pub t_datum: DatumTupleFields,
}

#[repr(C)]
pub struct HeapTupleHeaderData {
    pub t_choice: HeapTupleHeaderChoice,
    pub t_ctid: ItemPointerData,
    pub t_infomask2: u16,
    pub t_infomask: u16,
    pub t_hoff: u8,
    pub t_bits: [bits8; 0],
}

pub const SizeofHeapTupleHeader: usize = 23;

const _: () = assert!(core::mem::offset_of!(HeapTupleHeaderData, t_ctid) == 12);
const _: () = assert!(core::mem::offset_of!(HeapTupleHeaderData, t_infomask2) == 18);
const _: () = assert!(core::mem::offset_of!(HeapTupleHeaderData, t_infomask) == 20);
const _: () = assert!(core::mem::offset_of!(HeapTupleHeaderData, t_hoff) == 22);
const _: () = assert!(core::mem::offset_of!(HeapTupleHeaderData, t_bits) == SizeofHeapTupleHeader);

impl HeapTupleHeaderData {
    // SAFETY (all union reads below): every arm is plain integers over the
    // same 12 always-initialized bytes; any bit pattern is valid (C's model).
    #[inline]
    pub fn xmin_raw(&self) -> TransactionId {
        unsafe { self.t_choice.t_heap.t_xmin }
    }

    #[inline]
    pub fn xmin(&self) -> TransactionId {
        if self.xmin_frozen() {
            FrozenTransactionId
        } else {
            self.xmin_raw()
        }
    }

    #[inline]
    pub fn set_xmin(&mut self, xid: TransactionId) {
        self.t_choice.t_heap.t_xmin = xid;
    }

    #[inline]
    pub fn xmin_committed(&self) -> bool {
        (self.t_infomask & HEAP_XMIN_COMMITTED) != 0
    }

    #[inline]
    pub fn xmin_invalid(&self) -> bool {
        (self.t_infomask & (HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID)) == HEAP_XMIN_INVALID
    }

    #[inline]
    pub fn xmin_frozen(&self) -> bool {
        (self.t_infomask & HEAP_XMIN_FROZEN) == HEAP_XMIN_FROZEN
    }

    #[inline]
    pub fn set_xmin_committed(&mut self) {
        debug_assert!(!self.xmin_invalid());
        self.t_infomask |= HEAP_XMIN_COMMITTED;
    }

    #[inline]
    pub fn set_xmin_invalid(&mut self) {
        debug_assert!(!self.xmin_committed());
        self.t_infomask |= HEAP_XMIN_INVALID;
    }

    #[inline]
    pub fn set_xmin_frozen(&mut self) {
        debug_assert!(!self.xmin_invalid());
        self.t_infomask |= HEAP_XMIN_FROZEN;
    }

    #[inline]
    pub fn xmax_raw(&self) -> TransactionId {
        unsafe { self.t_choice.t_heap.t_xmax }
    }

    #[inline]
    pub fn set_xmax(&mut self, xid: TransactionId) {
        self.t_choice.t_heap.t_xmax = xid;
    }

    #[inline]
    pub fn raw_command_id(&self) -> CommandId {
        unsafe { self.t_choice.t_heap.t_field3 }
    }

    #[inline]
    pub fn set_cmin(&mut self, cid: CommandId) {
        debug_assert!((self.t_infomask & HEAP_MOVED) == 0);
        self.t_choice.t_heap.t_field3 = cid;
        self.t_infomask &= !HEAP_COMBOCID;
    }

    #[inline]
    pub fn set_cmax(&mut self, cid: CommandId, iscombo: bool) {
        debug_assert!((self.t_infomask & HEAP_MOVED) == 0);
        self.t_choice.t_heap.t_field3 = cid;
        if iscombo {
            self.t_infomask |= HEAP_COMBOCID;
        } else {
            self.t_infomask &= !HEAP_COMBOCID;
        }
    }

    #[inline]
    pub fn xvac(&self) -> TransactionId {
        if (self.t_infomask & HEAP_MOVED) != 0 {
            unsafe { self.t_choice.t_heap.t_field3 }
        } else {
            InvalidTransactionId
        }
    }

    #[inline]
    pub fn set_xvac(&mut self, xid: TransactionId) {
        debug_assert!((self.t_infomask & HEAP_MOVED) != 0);
        self.t_choice.t_heap.t_field3 = xid;
    }

    #[inline]
    pub fn is_speculative(&self) -> bool {
        ItemPointerGetOffsetNumberNoCheck(&self.t_ctid) == SpecTokenOffsetNumber
    }

    #[inline]
    pub fn speculative_token(&self) -> BlockNumber {
        debug_assert!(self.is_speculative());
        ItemPointerGetBlockNumber(&self.t_ctid)
    }

    #[inline]
    pub fn set_speculative_token(&mut self, token: BlockNumber) {
        ItemPointerSet(&mut self.t_ctid, token, SpecTokenOffsetNumber);
    }

    #[inline]
    pub fn indicates_moved_partitions(&self) -> bool {
        ItemPointerIndicatesMovedPartitions(&self.t_ctid)
    }

    #[inline]
    pub fn set_moved_partitions(&mut self) {
        ItemPointerSetMovedPartitions(&mut self.t_ctid);
    }

    // datum_len_ is the composite Datum's varlena length word: get/set are C's
    // VARSIZE/SET_VARSIZE on the header pointer.
    #[inline]
    pub fn datum_length(&self) -> u32 {
        varsize_4b_word(unsafe { self.t_choice.t_datum.datum_len_ } as u32)
    }

    #[inline]
    pub fn set_datum_length(&mut self, len: u32) {
        self.t_choice.t_datum.datum_len_ = set_varsize_4b_word(len) as i32;
    }

    #[inline]
    pub fn type_id(&self) -> Oid {
        unsafe { self.t_choice.t_datum.datum_typeid }
    }

    #[inline]
    pub fn set_type_id(&mut self, datum_typeid: Oid) {
        self.t_choice.t_datum.datum_typeid = datum_typeid;
    }

    #[inline]
    pub fn typmod(&self) -> i32 {
        unsafe { self.t_choice.t_datum.datum_typmod }
    }

    #[inline]
    pub fn set_typmod(&mut self, typmod: i32) {
        self.t_choice.t_datum.datum_typmod = typmod;
    }

    #[inline]
    pub fn is_hot_updated(&self) -> bool {
        (self.t_infomask2 & HEAP_HOT_UPDATED) != 0
            && (self.t_infomask & HEAP_XMAX_INVALID) == 0
            && !self.xmin_invalid()
    }

    #[inline]
    pub fn set_hot_updated(&mut self) {
        self.t_infomask2 |= HEAP_HOT_UPDATED;
    }

    #[inline]
    pub fn clear_hot_updated(&mut self) {
        self.t_infomask2 &= !HEAP_HOT_UPDATED;
    }

    #[inline]
    pub fn is_heap_only(&self) -> bool {
        (self.t_infomask2 & HEAP_ONLY_TUPLE) != 0
    }

    #[inline]
    pub fn set_heap_only(&mut self) {
        self.t_infomask2 |= HEAP_ONLY_TUPLE;
    }

    #[inline]
    pub fn clear_heap_only(&mut self) {
        self.t_infomask2 &= !HEAP_ONLY_TUPLE;
    }

    #[inline]
    pub fn natts(&self) -> u16 {
        self.t_infomask2 & HEAP_NATTS_MASK
    }

    #[inline]
    pub fn set_natts(&mut self, natts: u16) {
        self.t_infomask2 = (self.t_infomask2 & !HEAP_NATTS_MASK) | natts;
    }

    #[inline]
    pub fn has_external(&self) -> bool {
        (self.t_infomask & HEAP_HASEXTERNAL) != 0
    }
}

pub const MINIMAL_TUPLE_OFFSET: usize = 8;
pub const MINIMAL_TUPLE_PADDING: usize = 6;
pub const MINIMAL_TUPLE_DATA_OFFSET: usize = 10;

#[repr(C)]
pub struct MinimalTupleData {
    pub t_len: u32,
    pub mt_padding: [i8; MINIMAL_TUPLE_PADDING],
    pub t_infomask2: u16,
    pub t_infomask: u16,
    pub t_hoff: u8,
    pub t_bits: [bits8; 0],
}

pub const SizeofMinimalTupleHeader: usize = 15;

const _: () =
    assert!(core::mem::offset_of!(MinimalTupleData, t_infomask2) == MINIMAL_TUPLE_DATA_OFFSET);
const _: () = assert!(core::mem::offset_of!(MinimalTupleData, t_infomask) == 12);
const _: () = assert!(core::mem::offset_of!(MinimalTupleData, t_hoff) == 14);
const _: () = assert!(core::mem::offset_of!(MinimalTupleData, t_bits) == SizeofMinimalTupleHeader);
// Fields from t_infomask2 down sit MINIMAL_TUPLE_OFFSET below their heap-header twins.
const _: () = assert!(
    core::mem::offset_of!(HeapTupleHeaderData, t_infomask2)
        == core::mem::offset_of!(MinimalTupleData, t_infomask2) + MINIMAL_TUPLE_OFFSET
);

impl MinimalTupleData {
    #[inline]
    pub fn has_match(&self) -> bool {
        (self.t_infomask2 & HEAP_TUPLE_HAS_MATCH) != 0
    }

    #[inline]
    pub fn set_match(&mut self) {
        self.t_infomask2 |= HEAP_TUPLE_HAS_MATCH;
    }

    #[inline]
    pub fn clear_match(&mut self) {
        self.t_infomask2 &= !HEAP_TUPLE_HAS_MATCH;
    }

    #[inline]
    pub fn natts(&self) -> u16 {
        self.t_infomask2 & HEAP_NATTS_MASK
    }

    #[inline]
    pub fn set_natts(&mut self, natts: u16) {
        self.t_infomask2 = (self.t_infomask2 & !HEAP_NATTS_MASK) | natts;
    }
}

// t_data points at a contiguous header+bitmap+data image; the 'tup borrow +
// from_raw_parts contract is the unsafe kernel, everything above it is safe.
pub struct HeapTupleData<'tup> {
    pub t_len: u32,
    pub t_self: ItemPointerData,
    pub t_tableOid: Oid,
    t_data: core::ptr::NonNull<HeapTupleHeaderData>,
    _image: core::marker::PhantomData<&'tup [u8]>,
}

impl<'tup> HeapTupleData<'tup> {
    /// # Safety
    /// `image` points to a live, MAXALIGN-aligned heap-tuple image (fixed
    /// header + optional null bitmap + user data) readable for `t_len` bytes
    /// and unwritten-by-others for `'tup`; `t_len >= SizeofHeapTupleHeader`.
    /// Calling `&mut self` methods additionally requires exclusive write
    /// access to the image for `'tup`.
    #[inline]
    pub unsafe fn from_raw_parts(
        image: *const u8,
        t_len: u32,
        t_self: ItemPointerData,
        t_tableOid: Oid,
    ) -> HeapTupleData<'tup> {
        HeapTupleData {
            t_len,
            t_self,
            t_tableOid,
            t_data: core::ptr::NonNull::new_unchecked(image.cast_mut().cast()),
            _image: core::marker::PhantomData,
        }
    }

    #[inline]
    pub fn t_data(&self) -> &HeapTupleHeaderData {
        // SAFETY: from_raw_parts contract (live, aligned, >= 23 bytes).
        unsafe { self.t_data.as_ref() }
    }

    #[inline]
    pub fn t_data_mut(&mut self) -> &mut HeapTupleHeaderData {
        // SAFETY: from_raw_parts contract grants exclusive write access.
        unsafe { self.t_data.as_mut() }
    }

    #[inline]
    pub fn header_ptr(&self) -> *const u8 {
        self.t_data.as_ptr().cast()
    }

    #[inline]
    pub(crate) fn bits_ptr(&self) -> *const bits8 {
        // SAFETY: in-bounds offset within the image (t_len >= 23).
        unsafe { self.header_ptr().add(SizeofHeapTupleHeader) }
    }

    /// GETSTRUCT: the user-data area at `t_hoff`.
    #[inline]
    pub fn getstruct(&self) -> *const u8 {
        // SAFETY: t_hoff <= t_len per the image invariant.
        unsafe { self.header_ptr().add(self.t_data().t_hoff as usize) }
    }

    #[inline]
    pub fn has_nulls(&self) -> bool {
        (self.t_data().t_infomask & HEAP_HASNULL) != 0
    }

    #[inline]
    pub fn no_nulls(&self) -> bool {
        !self.has_nulls()
    }

    #[inline]
    pub fn has_var_width(&self) -> bool {
        (self.t_data().t_infomask & HEAP_HASVARWIDTH) != 0
    }

    #[inline]
    pub fn all_fixed(&self) -> bool {
        !self.has_var_width()
    }

    #[inline]
    pub fn has_external(&self) -> bool {
        (self.t_data().t_infomask & HEAP_HASEXTERNAL) != 0
    }

    #[inline]
    pub fn is_hot_updated(&self) -> bool {
        self.t_data().is_hot_updated()
    }

    #[inline]
    pub fn is_heap_only(&self) -> bool {
        self.t_data().is_heap_only()
    }
}
