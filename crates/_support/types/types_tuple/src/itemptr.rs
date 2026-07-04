use ::types_core::{BlockNumber, InvalidBlockNumber, OffsetNumber, BLCKSZ};

pub const InvalidOffsetNumber: OffsetNumber = 0;
pub const FirstOffsetNumber: OffsetNumber = 1;
// sizeof(ItemIdData) == 4 (storage/itemid.h).
pub const MaxOffsetNumber: OffsetNumber = (BLCKSZ / 4) as OffsetNumber;

pub const SpecTokenOffsetNumber: OffsetNumber = 0xfffe;
pub const MovedPartitionsOffsetNumber: OffsetNumber = 0xfffd;
pub const MovedPartitionsBlockNumber: BlockNumber = InvalidBlockNumber;

const _: () = assert!(MaxOffsetNumber < SpecTokenOffsetNumber);

#[inline]
pub const fn OffsetNumberIsValid(offsetNumber: OffsetNumber) -> bool {
    offsetNumber != InvalidOffsetNumber && offsetNumber <= MaxOffsetNumber
}

#[inline]
pub const fn OffsetNumberNext(offsetNumber: OffsetNumber) -> OffsetNumber {
    offsetNumber + 1
}

#[inline]
pub const fn OffsetNumberPrev(offsetNumber: OffsetNumber) -> OffsetNumber {
    offsetNumber - 1
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct BlockIdData {
    pub bi_hi: u16,
    pub bi_lo: u16,
}

const _: () = assert!(core::mem::size_of::<BlockIdData>() == 4);

#[inline]
pub fn BlockIdSet(blockId: &mut BlockIdData, blockNumber: BlockNumber) {
    blockId.bi_hi = (blockNumber >> 16) as u16;
    blockId.bi_lo = (blockNumber & 0xffff) as u16;
}

#[inline]
pub const fn BlockIdGetBlockNumber(blockId: &BlockIdData) -> BlockNumber {
    ((blockId.bi_hi as BlockNumber) << 16) | blockId.bi_lo as BlockNumber
}

#[inline]
pub const fn BlockIdIsValid(blockId: &BlockIdData) -> bool {
    BlockIdGetBlockNumber(blockId) != InvalidBlockNumber
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct ItemPointerData {
    pub ip_blkid: BlockIdData,
    pub ip_posid: OffsetNumber,
}

const _: () = assert!(core::mem::size_of::<ItemPointerData>() == 6);
const _: () = assert!(core::mem::align_of::<ItemPointerData>() == 2);

impl ItemPointerData {
    pub const fn new(blockNumber: BlockNumber, offNum: OffsetNumber) -> Self {
        Self {
            ip_blkid: BlockIdData {
                bi_hi: (blockNumber >> 16) as u16,
                bi_lo: (blockNumber & 0xffff) as u16,
            },
            ip_posid: offNum,
        }
    }

    pub const fn invalid() -> Self {
        Self::new(InvalidBlockNumber, InvalidOffsetNumber)
    }
}

#[inline]
pub const fn ItemPointerIsValid(pointer: &ItemPointerData) -> bool {
    pointer.ip_posid != 0
}

#[inline]
pub const fn ItemPointerGetBlockNumberNoCheck(pointer: &ItemPointerData) -> BlockNumber {
    BlockIdGetBlockNumber(&pointer.ip_blkid)
}

#[inline]
pub fn ItemPointerGetBlockNumber(pointer: &ItemPointerData) -> BlockNumber {
    debug_assert!(ItemPointerIsValid(pointer));
    ItemPointerGetBlockNumberNoCheck(pointer)
}

#[inline]
pub const fn ItemPointerGetOffsetNumberNoCheck(pointer: &ItemPointerData) -> OffsetNumber {
    pointer.ip_posid
}

#[inline]
pub fn ItemPointerGetOffsetNumber(pointer: &ItemPointerData) -> OffsetNumber {
    debug_assert!(ItemPointerIsValid(pointer));
    ItemPointerGetOffsetNumberNoCheck(pointer)
}

#[inline]
pub fn ItemPointerSet(
    pointer: &mut ItemPointerData,
    blockNumber: BlockNumber,
    offNum: OffsetNumber,
) {
    BlockIdSet(&mut pointer.ip_blkid, blockNumber);
    pointer.ip_posid = offNum;
}

#[inline]
pub fn ItemPointerSetBlockNumber(pointer: &mut ItemPointerData, blockNumber: BlockNumber) {
    BlockIdSet(&mut pointer.ip_blkid, blockNumber);
}

#[inline]
pub fn ItemPointerSetOffsetNumber(pointer: &mut ItemPointerData, offsetNumber: OffsetNumber) {
    pointer.ip_posid = offsetNumber;
}

#[inline]
pub fn ItemPointerCopy(fromPointer: &ItemPointerData, toPointer: &mut ItemPointerData) {
    *toPointer = *fromPointer;
}

#[inline]
pub fn ItemPointerSetInvalid(pointer: &mut ItemPointerData) {
    BlockIdSet(&mut pointer.ip_blkid, InvalidBlockNumber);
    pointer.ip_posid = InvalidOffsetNumber;
}

#[inline]
pub fn ItemPointerIndicatesMovedPartitions(pointer: &ItemPointerData) -> bool {
    ItemPointerGetOffsetNumber(pointer) == MovedPartitionsOffsetNumber
        && ItemPointerGetBlockNumberNoCheck(pointer) == MovedPartitionsBlockNumber
}

#[inline]
pub fn ItemPointerSetMovedPartitions(pointer: &mut ItemPointerData) {
    ItemPointerSet(
        pointer,
        MovedPartitionsBlockNumber,
        MovedPartitionsOffsetNumber,
    );
}

#[inline]
pub fn ItemPointerEquals(pointer1: &ItemPointerData, pointer2: &ItemPointerData) -> bool {
    ItemPointerGetBlockNumber(pointer1) == ItemPointerGetBlockNumber(pointer2)
        && ItemPointerGetOffsetNumber(pointer1) == ItemPointerGetOffsetNumber(pointer2)
}

// itemptr_encode/decode (tableam.h): TIDs as sortable int8 for validate_index.
#[inline]
pub fn itemptr_encode(itemptr: &ItemPointerData) -> i64 {
    (((ItemPointerGetBlockNumber(itemptr) as u64) << 16)
        | ItemPointerGetOffsetNumber(itemptr) as u64) as i64
}

#[inline]
pub fn itemptr_decode(encoded: i64) -> ItemPointerData {
    ItemPointerData::new((encoded >> 16) as BlockNumber, (encoded & 0xffff) as OffsetNumber)
}

#[inline]
pub fn ItemPointerCompare(arg1: &ItemPointerData, arg2: &ItemPointerData) -> i32 {
    let b1 = ItemPointerGetBlockNumberNoCheck(arg1);
    let b2 = ItemPointerGetBlockNumberNoCheck(arg2);
    if b1 < b2 {
        -1
    } else if b1 > b2 {
        1
    } else if ItemPointerGetOffsetNumberNoCheck(arg1) < ItemPointerGetOffsetNumberNoCheck(arg2) {
        -1
    } else if ItemPointerGetOffsetNumberNoCheck(arg1) > ItemPointerGetOffsetNumberNoCheck(arg2) {
        1
    } else {
        0
    }
}

pub fn ItemPointerInc(pointer: &mut ItemPointerData) {
    let mut blk = ItemPointerGetBlockNumberNoCheck(pointer);
    let mut off = ItemPointerGetOffsetNumberNoCheck(pointer);
    if off == u16::MAX {
        if blk != InvalidBlockNumber {
            off = 0;
            blk += 1;
        }
    } else {
        off += 1;
    }
    ItemPointerSet(pointer, blk, off);
}

pub fn ItemPointerDec(pointer: &mut ItemPointerData) {
    let mut blk = ItemPointerGetBlockNumberNoCheck(pointer);
    let mut off = ItemPointerGetOffsetNumberNoCheck(pointer);
    if off == 0 {
        if blk != 0 {
            off = u16::MAX;
            blk -= 1;
        }
    } else {
        off -= 1;
    }
    ItemPointerSet(pointer, blk, off);
}
