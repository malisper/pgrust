//! SP-GiST rmgr WAL record bodies (`access/spgxlog.h`).
//!
//! The read side (`from_bytes`) is trimmed to the fields the redo ports consume
//! so far; the embedded `spgxlogState stateSrc` members are not read and are
//! represented only by the offsets of the fields around them.
//!
//! The write side (the `*_to_bytes` encoders + `SizeOfSpgxlog*` constants) is the
//! full record image `spgdoinsert.c` emits via `XLogRegisterData`: it serializes
//! the C struct byte-for-byte (native endian, honoring C struct padding and the
//! `offsetof(..., offsets)` flexible-array sizes), *including* the `stateSrc`
//! member where present. Trailing variable-length data (offset arrays, page
//! selectors, tuple images) is appended by the caller in `XLogRegisterData`
//! call order — these encoders produce only the fixed header.

extern crate alloc;

use alloc::vec::Vec;

use crate::bytes::{bool_at, i8_at, u16_at, u32_at};
use types_core::{OffsetNumber, TransactionId};

/// `spgxlogState {TransactionId redirectXid; bool isBuild;}` (spgxlog.h) — the
/// few `SpGistState` fields redo needs, carried inside several records via the
/// `STORE_STATE` macro. C layout: `redirectXid` (u32 @0), `isBuild` (bool @4),
/// struct size MAXALIGN'd to the u32 alignment = 8 bytes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct spgxlogState {
    pub redirectXid: TransactionId,
    pub isBuild: bool,
}

/// `sizeof(spgxlogState)` — 8 bytes (u32 + bool, padded to 4-byte alignment).
pub const SIZE_OF_SPGXLOG_STATE: usize = 8;

impl spgxlogState {
    /// Append the 8-byte C image (`redirectXid` u32, `isBuild` bool, 3 pad).
    fn write_into(&self, v: &mut Vec<u8>) {
        v.extend_from_slice(&self.redirectXid.to_ne_bytes());
        v.push(self.isBuild as u8);
        v.extend_from_slice(&[0u8; 3]); // pad to 8
    }
}

/// `spgxlogAddLeaf`: `{bool newPage; bool storesNulls;
/// OffsetNumber offnumLeaf; OffsetNumber offnumHeadLeaf;
/// OffsetNumber offnumParent; uint16 nodeI;}`.
#[derive(Clone, Copy, Debug)]
pub struct spgxlogAddLeaf {
    pub newPage: bool,
    pub storesNulls: bool,
    pub offnumLeaf: OffsetNumber,
    pub offnumHeadLeaf: OffsetNumber,
    pub offnumParent: OffsetNumber,
    pub nodeI: u16,
}

impl spgxlogAddLeaf {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            newPage: bool_at(rec, 0),
            storesNulls: bool_at(rec, 1),
            offnumLeaf: u16_at(rec, 2),
            offnumHeadLeaf: u16_at(rec, 4),
            offnumParent: u16_at(rec, 6),
            nodeI: u16_at(rec, 8),
        }
    }

    /// `XLogRegisterData(&xlrec, sizeof(xlrec))` image — `bool newPage` @0,
    /// `bool storesNulls` @1, then four uint16 @2/4/6/8; size 10.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(SIZE_OF_SPGXLOG_ADD_LEAF);
        v.push(self.newPage as u8);
        v.push(self.storesNulls as u8);
        v.extend_from_slice(&self.offnumLeaf.to_ne_bytes());
        v.extend_from_slice(&self.offnumHeadLeaf.to_ne_bytes());
        v.extend_from_slice(&self.offnumParent.to_ne_bytes());
        v.extend_from_slice(&self.nodeI.to_ne_bytes());
        v
    }
}

/// `sizeof(spgxlogAddLeaf)` — 10 bytes.
pub const SIZE_OF_SPGXLOG_ADD_LEAF: usize = 10;

/// `spgxlogMoveLeafs`: `{uint16 nMoves; bool newPage; bool replaceDead;
/// bool storesNulls; OffsetNumber offnumParent; uint16 nodeI;
/// spgxlogState stateSrc; OffsetNumber offsets[];}` — trimmed of `stateSrc`
/// and the trailing offsets.
#[derive(Clone, Copy, Debug)]
pub struct spgxlogMoveLeafs {
    pub nMoves: u16,
    pub newPage: bool,
    pub replaceDead: bool,
    pub storesNulls: bool,
    pub offnumParent: OffsetNumber,
    pub nodeI: u16,
}

impl spgxlogMoveLeafs {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            nMoves: u16_at(rec, 0),
            newPage: bool_at(rec, 2),
            replaceDead: bool_at(rec, 3),
            storesNulls: bool_at(rec, 4),
            offnumParent: u16_at(rec, 6),
            nodeI: u16_at(rec, 8),
        }
    }

    /// `XLogRegisterData(&xlrec, SizeOfSpgxlogMoveLeafs)` image: `nMoves` u16 @0,
    /// `newPage`/`replaceDead`/`storesNulls` bools @2/3/4, pad @5, `offnumParent`
    /// u16 @6, `nodeI` u16 @8, pad @10/11, `stateSrc` @12; total 20.
    pub fn to_bytes(&self, state_src: &spgxlogState) -> Vec<u8> {
        let mut v = Vec::with_capacity(SIZE_OF_SPGXLOG_MOVE_LEAFS);
        v.extend_from_slice(&self.nMoves.to_ne_bytes());
        v.push(self.newPage as u8);
        v.push(self.replaceDead as u8);
        v.push(self.storesNulls as u8);
        v.push(0u8); // pad to offset 6
        v.extend_from_slice(&self.offnumParent.to_ne_bytes());
        v.extend_from_slice(&self.nodeI.to_ne_bytes());
        v.extend_from_slice(&[0u8; 2]); // pad to offset 12 (stateSrc 4-aligned)
        state_src.write_into(&mut v);
        v
    }
}

/// `SizeOfSpgxlogMoveLeafs` = `offsetof(spgxlogMoveLeafs, offsets)` — 20 bytes.
pub const SIZE_OF_SPGXLOG_MOVE_LEAFS: usize = 20;

/// `spgxlogAddNode`: `{OffsetNumber offnum; OffsetNumber offnumNew;
/// bool newPage; int8 parentBlk; OffsetNumber offnumParent; uint16 nodeI;
/// spgxlogState stateSrc;}` — trimmed of `stateSrc`.
#[derive(Clone, Copy, Debug)]
pub struct spgxlogAddNode {
    pub offnum: OffsetNumber,
    pub offnumNew: OffsetNumber,
    pub newPage: bool,
    pub parentBlk: i8,
    pub offnumParent: OffsetNumber,
    pub nodeI: u16,
}

impl spgxlogAddNode {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            offnum: u16_at(rec, 0),
            offnumNew: u16_at(rec, 2),
            newPage: bool_at(rec, 4),
            parentBlk: i8_at(rec, 5),
            offnumParent: u16_at(rec, 6),
            nodeI: u16_at(rec, 8),
        }
    }

    /// `XLogRegisterData(&xlrec, sizeof(xlrec))` image: `offnum` u16 @0,
    /// `offnumNew` u16 @2, `newPage` bool @4, `parentBlk` int8 @5,
    /// `offnumParent` u16 @6, `nodeI` u16 @8, pad @10/11, `stateSrc` @12; size 20.
    pub fn to_bytes(&self, state_src: &spgxlogState) -> Vec<u8> {
        let mut v = Vec::with_capacity(SIZE_OF_SPGXLOG_ADD_NODE);
        v.extend_from_slice(&self.offnum.to_ne_bytes());
        v.extend_from_slice(&self.offnumNew.to_ne_bytes());
        v.push(self.newPage as u8);
        v.push(self.parentBlk as u8);
        v.extend_from_slice(&self.offnumParent.to_ne_bytes());
        v.extend_from_slice(&self.nodeI.to_ne_bytes());
        v.extend_from_slice(&[0u8; 2]); // pad to offset 12 (stateSrc 4-aligned)
        state_src.write_into(&mut v);
        v
    }
}

/// `sizeof(spgxlogAddNode)` — 20 bytes.
pub const SIZE_OF_SPGXLOG_ADD_NODE: usize = 20;

/// `spgxlogSplitTuple`: `{OffsetNumber offnumPrefix;
/// OffsetNumber offnumPostfix; bool newPage; bool postfixBlkSame;}`.
#[derive(Clone, Copy, Debug)]
pub struct spgxlogSplitTuple {
    pub offnumPrefix: OffsetNumber,
    pub offnumPostfix: OffsetNumber,
    pub newPage: bool,
    pub postfixBlkSame: bool,
}

impl spgxlogSplitTuple {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            offnumPrefix: u16_at(rec, 0),
            offnumPostfix: u16_at(rec, 2),
            newPage: bool_at(rec, 4),
            postfixBlkSame: bool_at(rec, 5),
        }
    }

    /// `XLogRegisterData(&xlrec, sizeof(xlrec))` image: `offnumPrefix` u16 @0,
    /// `offnumPostfix` u16 @2, `newPage` bool @4, `postfixBlkSame` bool @5;
    /// size 6. No `stateSrc`.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(SIZE_OF_SPGXLOG_SPLIT_TUPLE);
        v.extend_from_slice(&self.offnumPrefix.to_ne_bytes());
        v.extend_from_slice(&self.offnumPostfix.to_ne_bytes());
        v.push(self.newPage as u8);
        v.push(self.postfixBlkSame as u8);
        v
    }
}

/// `sizeof(spgxlogSplitTuple)` — 6 bytes.
pub const SIZE_OF_SPGXLOG_SPLIT_TUPLE: usize = 6;

/// `spgxlogPickSplit`: trimmed of `initSrc`/`initDest`/`stateSrc` and the
/// trailing offsets; layout `{bool isRootSplit /*0*/; uint16 nDelete /*2*/;
/// uint16 nInsert /*4*/; bool initSrc /*6*/; bool initDest /*7*/;
/// OffsetNumber offnumInner /*8*/; bool initInner /*10*/;
/// bool storesNulls /*11*/; bool innerIsParent /*12*/;
/// OffsetNumber offnumParent /*14*/; uint16 nodeI /*16*/;
/// spgxlogState stateSrc; OffsetNumber offsets[];}`.
#[derive(Clone, Copy, Debug)]
pub struct spgxlogPickSplit {
    pub isRootSplit: bool,
    pub nDelete: u16,
    pub nInsert: u16,
    pub offnumInner: OffsetNumber,
    pub initInner: bool,
    pub storesNulls: bool,
    pub innerIsParent: bool,
    pub offnumParent: OffsetNumber,
    pub nodeI: u16,
}

impl spgxlogPickSplit {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            isRootSplit: bool_at(rec, 0),
            nDelete: u16_at(rec, 2),
            nInsert: u16_at(rec, 4),
            offnumInner: u16_at(rec, 8),
            initInner: bool_at(rec, 10),
            storesNulls: bool_at(rec, 11),
            innerIsParent: bool_at(rec, 12),
            offnumParent: u16_at(rec, 14),
            nodeI: u16_at(rec, 16),
        }
    }

    /// `XLogRegisterData(&xlrec, SizeOfSpgxlogPickSplit)` image: `isRootSplit`
    /// bool @0, pad @1, `nDelete` u16 @2, `nInsert` u16 @4, `initSrc` bool @6,
    /// `initDest` bool @7, `offnumInner` u16 @8, `initInner` bool @10,
    /// `storesNulls` bool @11, `innerIsParent` bool @12, pad @13, `offnumParent`
    /// u16 @14, `nodeI` u16 @16, pad @18/19, `stateSrc` @20; total 28.
    ///
    /// `initSrc`/`initDest` are write-only fields not kept on the read struct;
    /// they are passed in.
    pub fn to_bytes(
        &self,
        init_src: bool,
        init_dest: bool,
        state_src: &spgxlogState,
    ) -> Vec<u8> {
        let mut v = Vec::with_capacity(SIZE_OF_SPGXLOG_PICK_SPLIT);
        v.push(self.isRootSplit as u8);
        v.push(0u8); // pad to offset 2
        v.extend_from_slice(&self.nDelete.to_ne_bytes());
        v.extend_from_slice(&self.nInsert.to_ne_bytes());
        v.push(init_src as u8);
        v.push(init_dest as u8);
        v.extend_from_slice(&self.offnumInner.to_ne_bytes());
        v.push(self.initInner as u8);
        v.push(self.storesNulls as u8);
        v.push(self.innerIsParent as u8);
        v.push(0u8); // pad to offset 14
        v.extend_from_slice(&self.offnumParent.to_ne_bytes());
        v.extend_from_slice(&self.nodeI.to_ne_bytes());
        v.extend_from_slice(&[0u8; 2]); // pad to offset 20 (stateSrc 4-aligned)
        state_src.write_into(&mut v);
        v
    }
}

/// `SizeOfSpgxlogPickSplit` = `offsetof(spgxlogPickSplit, offsets)` — 28 bytes.
pub const SIZE_OF_SPGXLOG_PICK_SPLIT: usize = 28;

/// `spgxlogVacuumLeaf`: `{uint16 nDead; uint16 nPlaceholder; uint16 nMove;
/// uint16 nChain; spgxlogState stateSrc; OffsetNumber offsets[];}` — trimmed
/// of `stateSrc` and the trailing offsets.
#[derive(Clone, Copy, Debug)]
pub struct spgxlogVacuumLeaf {
    pub nDead: u16,
    pub nPlaceholder: u16,
    pub nMove: u16,
    pub nChain: u16,
}

impl spgxlogVacuumLeaf {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            nDead: u16_at(rec, 0),
            nPlaceholder: u16_at(rec, 2),
            nMove: u16_at(rec, 4),
            nChain: u16_at(rec, 6),
        }
    }
}

/// `spgxlogVacuumRoot`: `{uint16 nDelete; spgxlogState stateSrc;
/// OffsetNumber offsets[];}` — trimmed of `stateSrc` and the offsets.
#[derive(Clone, Copy, Debug)]
pub struct spgxlogVacuumRoot {
    pub nDelete: u16,
}

impl spgxlogVacuumRoot {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { nDelete: u16_at(rec, 0) }
    }
}

/// `spgxlogVacuumRedirect`: `{uint16 nToPlaceholder;
/// OffsetNumber firstPlaceholder; TransactionId snapshotConflictHorizon;
/// bool isCatalogRel; OffsetNumber offsets[];}` — trimmed of the offsets.
#[derive(Clone, Copy, Debug)]
pub struct spgxlogVacuumRedirect {
    pub nToPlaceholder: u16,
    pub firstPlaceholder: OffsetNumber,
    pub snapshotConflictHorizon: TransactionId,
    pub isCatalogRel: bool,
}

impl spgxlogVacuumRedirect {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            nToPlaceholder: u16_at(rec, 0),
            firstPlaceholder: u16_at(rec, 2),
            snapshotConflictHorizon: u32_at(rec, 4),
            isCatalogRel: bool_at(rec, 8),
        }
    }
}
